package gosnowflake

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"net/http"
	"strings"
	"time"
)

const (
	awsWif   wifProviderType = "AWS"
	gcpWif   wifProviderType = "GCP"
	azureWif wifProviderType = "AZURE"
	oidcWif  wifProviderType = "OIDC"
)

type wifProviderType string

type wifAttestation struct {
	ProviderType string            `json:"providerType"`
	Credential   string            `json:"credential"`
	Metadata     map[string]string `json:"metadata"`
}

type wifAttestationCreator interface {
	createAttestation(ctx context.Context) (*wifAttestation, error)
}

type wifAttestationProvider struct {
	context      context.Context
	awsCreator   wifAttestationCreator
	gcpCreator   wifAttestationCreator
	azureCreator wifAttestationCreator
	oidcCreator  wifAttestationCreator
}

func createWifAttestationProvider(ctx context.Context) *wifAttestationProvider {
	return &wifAttestationProvider{
		context:      ctx,
		awsCreator:   &awsIdentityAttestationCreator{attestationService: createDefaultAwsAttestationService(ctx)},
		gcpCreator:   nil,
		azureCreator: nil,
		oidcCreator:  nil,
	}
}

func (p *wifAttestationProvider) getAttestation(identityProvider string) (*wifAttestation, error) {
	if strings.TrimSpace(identityProvider) == "" {
		logger.Info("Workload Identity Provider has not been specified. Using autodetect...")
		return p.createAutodetectAttestation()
	}
	creator, err := p.attestationCreator(identityProvider)
	if err != nil {
		logger.Errorf("error while creating specified Workload Identity provider %v", err)
		return nil, err
	}
	return creator.createAttestation(p.context)
}

func (p *wifAttestationProvider) attestationCreator(identityProvider string) (wifAttestationCreator, error) {
	switch strings.ToUpper(identityProvider) {
	case string(awsWif):
		return p.awsCreator, nil
	case string(gcpWif):
		return p.gcpCreator, nil
	case string(azureWif):
		return p.azureCreator, nil
	case string(oidcWif):
		return p.oidcCreator, nil
	default:
		return nil, errors.New("unknown Workload Identity provider specified: " + identityProvider)
	}
}

func (p *wifAttestationProvider) createAutodetectAttestation() (*wifAttestation, error) {
	if attestation := p.getAttestationForAutodetect(p.oidcCreator, oidcWif); attestation != nil {
		return attestation, nil
	}
	if attestation := p.getAttestationForAutodetect(p.awsCreator, awsWif); attestation != nil {
		return attestation, nil
	}
	if attestation := p.getAttestationForAutodetect(p.gcpCreator, gcpWif); attestation != nil {
		return attestation, nil
	}
	if attestation := p.getAttestationForAutodetect(p.azureCreator, azureWif); attestation != nil {
		return attestation, nil
	}
	return nil, errors.New("unable to autodetect Workload Identity. None of the supported Workload Identity environments has been identified")
}

func (p *wifAttestationProvider) getAttestationForAutodetect(
	creator wifAttestationCreator,
	providerType wifProviderType,
) *wifAttestation {
	attestation, err := creator.createAttestation(p.context)
	if err != nil {
		logger.Errorf("Unable to create identity attestation for %s, error: %v", providerType, err)
		return nil
	}
	return attestation
}

type awsIdentityAttestationCreator struct {
	attestationService awsAttestationService
}

type awsAttestationService interface {
	GetAWSCredentials() aws.Credentials
	GetAWSRegion() string
	GetArn() string
}

type defaultAwsAttestationService struct {
	ctx context.Context
	cfg aws.Config
}

func createDefaultAwsAttestationService(ctx context.Context) *defaultAwsAttestationService {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		logger.Debugf("Unable to load AWS config: %v", err)
		return nil
	}
	return &defaultAwsAttestationService{
		cfg: cfg,
		ctx: ctx,
	}
}

func (m *defaultAwsAttestationService) GetAWSCredentials() aws.Credentials {
	creds, err := m.cfg.Credentials.Retrieve(m.ctx)
	if err != nil {
		logger.Debugf("Unable to retrieve AWS credentials provider: %v", err)
		return aws.Credentials{}
	}
	return creds
}

func (m *defaultAwsAttestationService) GetAWSRegion() string {
	return m.cfg.Region
}

func (m *defaultAwsAttestationService) GetArn() string {
	client := sts.NewFromConfig(m.cfg)
	output, err := client.GetCallerIdentity(m.ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		logger.Debugf("Unable to get caller identity: %v", err)
		return ""
	}
	return aws.ToString(output.Arn)
}

func (creator *awsIdentityAttestationCreator) createAttestation(ctx context.Context) (*wifAttestation, error) {
	logger.Debug("Creating AWS identity attestation...")

	if creator.attestationService == nil {
		logger.Debug("AWS attestation service could not be created.")
		return nil, nil
	}

	creds := creator.attestationService.GetAWSCredentials()
	if creds.AccessKeyID == "" || creds.SecretAccessKey == "" {
		logger.Debug("No AWS credentials were found.")
		return nil, nil
	}

	region := creator.attestationService.GetAWSRegion()
	if region == "" {
		logger.Debug("No AWS region was found.")
		return nil, nil
	}

	arn := creator.attestationService.GetArn()
	if arn == "" {
		logger.Debug("No Caller Identity was found.")
		return nil, nil
	}

	stsHostname := fmt.Sprintf("sts.%s.amazonaws.com", region)
	req, err := creator.createStsRequest(stsHostname)
	if err != nil {
		return nil, err
	}

	err = creator.signRequestWithSigV4(ctx, req, creds, region)
	if err != nil {
		return nil, err
	}

	credential, err := creator.createBase64EncodedRequestCredential(req)
	if err != nil {
		return nil, err
	}

	return &wifAttestation{
		ProviderType: "AWS",
		Credential:   credential,
		Metadata:     map[string]string{"arn": arn},
	}, nil
}

func (creator *awsIdentityAttestationCreator) createStsRequest(hostname string) (*http.Request, error) {
	url := fmt.Sprintf("https://%s/?Action=GetCallerIdentity&Version=2011-06-15", hostname)
	req, err := http.NewRequest("POST", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Host", hostname)
	req.Header.Set("X-Snowflake-Audience", "snowflakecomputing.com")
	return req, nil
}

func (creator *awsIdentityAttestationCreator) signRequestWithSigV4(ctx context.Context, req *http.Request, creds aws.Credentials, region string) error {
	signer := v4.NewSigner()
	// as per docs of SignHTTP, the payload hash must be present even if the payload is empty
	payloadHash := hex.EncodeToString(sha256.New().Sum(nil))
	err := signer.SignHTTP(ctx, creds, req, payloadHash, "sts", region, time.Now())
	return err
}

func (creator *awsIdentityAttestationCreator) createBase64EncodedRequestCredential(req *http.Request) (string, error) {
	headers := make(map[string]string)
	for key, values := range req.Header {
		headers[key] = values[0]
	}

	assertion := map[string]interface{}{
		"url":     req.URL.String(),
		"method":  req.Method,
		"headers": headers,
	}

	assertionJSON, err := json.Marshal(assertion)
	if err != nil {
		return "", err
	}

	return base64.StdEncoding.EncodeToString(assertionJSON), nil
}
