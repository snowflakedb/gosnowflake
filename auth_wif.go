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
	AWS   wifProviderType = "AWS"
	GCP   wifProviderType = "GCP"
	AZURE wifProviderType = "AZURE"
	OIDC  wifProviderType = "OIDC"
)

type wifProviderType string

type WifAttestation struct {
	ProviderType string            `json:"providerType"`
	Credential   string            `json:"credential"`
	Metadata     map[string]string `json:"metadata"`
}

type wifAttestationCreator interface {
	CreateAttestation(ctx context.Context) (*WifAttestation, error)
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
		awsCreator:   &AwsIdentityAttestationCreator{attestationService: createDefaultAwsAttestationService(ctx)},
		gcpCreator:   nil,
		azureCreator: nil,
		oidcCreator:  nil,
	}
}

func (p *wifAttestationProvider) getAttestation(identityProvider string) (*WifAttestation, error) {
	if strings.TrimSpace(identityProvider) == "" {
		logger.Info("Workload Identity Provider has not been specified. Using autodetect...")
		return p.createAutodetectAttestation()
	}
	creator, err := p.attestationCreator(identityProvider)
	if err != nil {
		logger.Errorf("error while creating specified Workload Identity provider %v", err)
		return nil, err
	}
	return creator.CreateAttestation(p.context)
}

func (p *wifAttestationProvider) attestationCreator(identityProvider string) (wifAttestationCreator, error) {
	switch strings.ToUpper(identityProvider) {
	case string(AWS):
		return p.awsCreator, nil
	case string(GCP):
		return p.gcpCreator, nil
	case string(AZURE):
		return p.azureCreator, nil
	case string(OIDC):
		return p.oidcCreator, nil
	default:
		return nil, errors.New("unknown Workload Identity provider specified: " + identityProvider)
	}
}

func (p *wifAttestationProvider) createAutodetectAttestation() (*WifAttestation, error) {
	//if attestation := p.getAttestationForAutodetect(p.oidcCreator, OIDC); attestation != nil {
	//	return attestation, nil
	//}
	if attestation := p.getAttestationForAutodetect(p.awsCreator, AWS); attestation != nil {
		return attestation, nil
	}
	//if attestation := p.getAttestationForAutodetect(p.gcpCreator, GCP); attestation != nil {
	//	return attestation, nil
	//}
	//if attestation := p.getAttestationForAutodetect(p.azureCreator, AZURE); attestation != nil {
	//	return attestation, nil
	//}
	return nil, errors.New("unable to autodetect Workload Identity. None of the supported Workload Identity environments has been identified")
}

func (p *wifAttestationProvider) getAttestationForAutodetect(
	creator wifAttestationCreator,
	providerType wifProviderType,
) *WifAttestation {
	attestation, err := creator.CreateAttestation(p.context)
	if err != nil {
		logger.Errorf("Unable to create identity attestation for %s, error: %v", providerType, err)
		return nil
	}
	return attestation
}

type AwsIdentityAttestationCreator struct {
	attestationService AwsAttestationService
}

type AwsAttestationService interface {
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

func (creator *AwsIdentityAttestationCreator) CreateAttestation(ctx context.Context) (*WifAttestation, error) {
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

	return &WifAttestation{
		ProviderType: "AWS",
		Credential:   credential,
		Metadata:     map[string]string{"arn": arn},
	}, nil
}

func (creator *AwsIdentityAttestationCreator) createStsRequest(hostname string) (*http.Request, error) {
	url := fmt.Sprintf("https://%s/?Action=GetCallerIdentity&Version=2011-06-15", hostname)
	req, err := http.NewRequest("POST", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Host", hostname)
	req.Header.Set("X-Snowflake-Audience", "snowflakecomputing.com")
	return req, nil
}

func (creator *AwsIdentityAttestationCreator) signRequestWithSigV4(ctx context.Context, req *http.Request, creds aws.Credentials, region string) error {
	signer := v4.NewSigner()
	// as per docs of SignHTTP, the payload hash must be present even if the payload is empty
	payloadHash := hex.EncodeToString(sha256.New().Sum(nil))
	err := signer.SignHTTP(ctx, creds, req, payloadHash, "sts", region, time.Now())
	return err
}

func (creator *AwsIdentityAttestationCreator) createBase64EncodedRequestCredential(req *http.Request) (string, error) {
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
