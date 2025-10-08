package gosnowflake

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/golang-jwt/jwt/v5"
)

const (
	awsWif   wifProviderType = "AWS"
	gcpWif   wifProviderType = "GCP"
	azureWif wifProviderType = "AZURE"
	oidcWif  wifProviderType = "OIDC"

	gcpMetadataFlavorHeaderName = "Metadata-Flavor"
	gcpMetadataFlavor           = "Google"
	defaultMetadataServiceBase  = "http://169.254.169.254"
	snowflakeAudience           = "snowflakecomputing.com"
)

type wifProviderType string

type wifAttestation struct {
	ProviderType string            `json:"providerType"`
	Credential   string            `json:"credential"`
	Metadata     map[string]string `json:"metadata"`
}

type wifAttestationCreator interface {
	createAttestation() (*wifAttestation, error)
}

type wifAttestationProvider struct {
	context      context.Context
	cfg          *Config
	awsCreator   wifAttestationCreator
	gcpCreator   wifAttestationCreator
	azureCreator wifAttestationCreator
	oidcCreator  wifAttestationCreator
}

func createWifAttestationProvider(ctx context.Context, cfg *Config, telemetry *snowflakeTelemetry) *wifAttestationProvider {
	return &wifAttestationProvider{
		context: ctx,
		cfg:     cfg,
		awsCreator: &awsIdentityAttestationCreator{
			attestationServiceFactory: createDefaultAwsAttestationMetadataProvider,
			ctx:                       ctx,
		},
		gcpCreator: &gcpIdentityAttestationCreator{
			cfg:                    cfg,
			telemetry:              telemetry,
			metadataServiceBaseURL: defaultMetadataServiceBase,
		},
		azureCreator: &azureIdentityAttestationCreator{
			azureAttestationMetadataProvider: &defaultAzureAttestationMetadataProvider{},
			cfg:                              cfg,
			telemetry:                        telemetry,
			workloadIdentityEntraResource:    determineEntraResource(cfg),
			azureMetadataServiceBaseURL:      defaultMetadataServiceBase,
		},
		oidcCreator: &oidcIdentityAttestationCreator{token: cfg.Token},
	}
}

func (p *wifAttestationProvider) getAttestation(identityProvider string) (*wifAttestation, error) {
	switch strings.ToUpper(identityProvider) {
	case string(awsWif):
		return p.awsCreator.createAttestation()
	case string(gcpWif):
		return p.gcpCreator.createAttestation()
	case string(azureWif):
		return p.azureCreator.createAttestation()
	case string(oidcWif):
		return p.oidcCreator.createAttestation()
	default:
		return nil, fmt.Errorf("unknown WorkloadIdentityProvider specified: %s. Valid values are: %s, %s, %s, %s", identityProvider, awsWif, gcpWif, azureWif, oidcWif)
	}
}

type awsAttestastationMetadataProviderFactory func(ctx context.Context) awsAttestationMetadataProvider

type awsIdentityAttestationCreator struct {
	attestationServiceFactory awsAttestastationMetadataProviderFactory
	ctx                       context.Context
}

type gcpIdentityAttestationCreator struct {
	cfg                    *Config
	telemetry              *snowflakeTelemetry
	metadataServiceBaseURL string
}

type oidcIdentityAttestationCreator struct {
	token string
}

type awsAttestationMetadataProvider interface {
	awsCredentials() aws.Credentials
	awsRegion() string
}

type defaultAwsAttestationMetadataProvider struct {
	ctx    context.Context
	awsCfg aws.Config
}

func createDefaultAwsAttestationMetadataProvider(ctx context.Context) awsAttestationMetadataProvider {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithEC2IMDSRegion())
	if err != nil {
		logger.Debugf("Unable to load AWS config: %v", err)
		return nil
	}
	return &defaultAwsAttestationMetadataProvider{
		awsCfg: cfg,
		ctx:    ctx,
	}
}

func (s *defaultAwsAttestationMetadataProvider) awsCredentials() aws.Credentials {
	creds, err := s.awsCfg.Credentials.Retrieve(s.ctx)
	if err != nil {
		logger.Debugf("Unable to retrieve AWS credentials provider: %v", err)
		return aws.Credentials{}
	}
	return creds
}

func (s *defaultAwsAttestationMetadataProvider) awsRegion() string {
	return s.awsCfg.Region
}

func (c *awsIdentityAttestationCreator) createAttestation() (*wifAttestation, error) {
	logger.Debug("Creating AWS identity attestation...")

	attestationService := c.attestationServiceFactory(c.ctx)
	if attestationService == nil {
		return nil, fmt.Errorf("AWS attestation service could not be created")
	}

	creds := attestationService.awsCredentials()
	if creds.AccessKeyID == "" || creds.SecretAccessKey == "" {
		return nil, fmt.Errorf("no AWS credentials were found")
	}

	region := attestationService.awsRegion()
	if region == "" {
		return nil, fmt.Errorf("no AWS region was found")
	}

	stsHostname := stsHostname(region)
	req, err := c.createStsRequest(stsHostname)
	if err != nil {
		return nil, err
	}

	err = c.signRequestWithSigV4(c.ctx, req, creds, region)
	if err != nil {
		return nil, err
	}

	credential, err := c.createBase64EncodedRequestCredential(req)
	if err != nil {
		return nil, err
	}

	return &wifAttestation{
		ProviderType: string(awsWif),
		Credential:   credential,
		Metadata:     map[string]string{},
	}, nil
}

func stsHostname(region string) string {
	var domain string
	if strings.HasPrefix(region, "cn-") {
		domain = "amazonaws.com.cn"
	} else {
		domain = "amazonaws.com"
	}
	return fmt.Sprintf("sts.%s.%s", region, domain)
}

func (c *awsIdentityAttestationCreator) createStsRequest(hostname string) (*http.Request, error) {
	url := fmt.Sprintf("https://%s?Action=GetCallerIdentity&Version=2011-06-15", hostname)
	req, err := http.NewRequest("POST", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Host", hostname)
	req.Header.Set("X-Snowflake-Audience", "snowflakecomputing.com")
	return req, nil
}

func (c *awsIdentityAttestationCreator) signRequestWithSigV4(ctx context.Context, req *http.Request, creds aws.Credentials, region string) error {
	signer := v4.NewSigner()
	// as per docs of SignHTTP, the payload hash must be present even if the payload is empty
	payloadHash := hex.EncodeToString(sha256.New().Sum(nil))
	return signer.SignHTTP(ctx, creds, req, payloadHash, "sts", region, time.Now())
}

func (c *awsIdentityAttestationCreator) createBase64EncodedRequestCredential(req *http.Request) (string, error) {
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

func (c *gcpIdentityAttestationCreator) createAttestation() (*wifAttestation, error) {
	logger.Debugf("Creating GCP identity attestation...")
	req, err := c.createTokenRequest()
	if err != nil {
		return nil, fmt.Errorf("failed to create GCP token request: %v", err)
	}
	token := fetchTokenFromMetadataService(req, c.cfg, c.telemetry)
	if token == "" {
		return nil, fmt.Errorf("no GCP token was found")
	}
	sub, _, err := extractSubIssWithoutVerifyingSignature(token)
	if err != nil {
		return nil, fmt.Errorf("could not extract claims from token: %v", err)
	}
	return &wifAttestation{
		ProviderType: string(gcpWif),
		Credential:   token,
		Metadata:     map[string]string{"sub": sub},
	}, nil
}

func (c *gcpIdentityAttestationCreator) createTokenRequest() (*http.Request, error) {
	uri := fmt.Sprintf("%s/computeMetadata/v1/instance/service-accounts/default/identity?audience=%s",
		c.metadataServiceBaseURL, snowflakeAudience)
	req, err := http.NewRequest("GET", uri, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %v", err)
	}
	req.Header.Set(gcpMetadataFlavorHeaderName, gcpMetadataFlavor)
	return req, nil
}

func fetchTokenFromMetadataService(req *http.Request, cfg *Config, telemetry *snowflakeTelemetry) string {
	transport, err := newTransportFactory(cfg, telemetry).createTransport()
	if err != nil {
		logger.Debugf("Failed to create HTTP transport: %v", err)
		return ""
	}
	client := &http.Client{Transport: transport}
	resp, err := client.Do(req)
	if err != nil {
		logger.Debugf("Metadata server request was not successful: %v", err)
		return ""
	}
	defer func() {
		if err = resp.Body.Close(); err != nil {
			logger.Debugf("Failed to close response body: %v", err)
		}
	}()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		logger.Debugf("Failed to read response body: %v", err)
		return ""
	}
	return string(body)
}

func extractSubIssWithoutVerifyingSignature(token string) (subject string, issuer string, err error) {
	claims, err := extractClaimsMap(token)
	if err != nil {
		return "", "", err
	}
	issuerClaim, ok := claims["iss"]
	if !ok {
		return "", "", errors.New("missing issuer claim in JWT token")
	}
	subjectClaim, ok := claims["sub"]
	if !ok {
		return "", "", errors.New("missing sub claim in JWT token")
	}
	subject, ok = subjectClaim.(string)
	if !ok {
		return "", "", errors.New("sub claim is not a string in JWT token")
	}
	issuer, ok = issuerClaim.(string)
	if !ok {
		return "", "", errors.New("iss claim is not a string in JWT token")
	}
	return
}

// extractClaimsMap parses a JWT token and returns its claims as a map.
// It does not verify the token signature.
func extractClaimsMap(token string) (map[string]interface{}, error) {
	parser := jwt.NewParser(jwt.WithoutClaimsValidation())
	claims := jwt.MapClaims{}
	_, _, err := parser.ParseUnverified(token, claims)
	if err != nil {
		return nil, fmt.Errorf("unable to extract JWT claims from token: %w", err)
	}
	return claims, nil
}

func (c *oidcIdentityAttestationCreator) createAttestation() (*wifAttestation, error) {
	logger.Debugf("Creating OIDC identity attestation...")
	if c.token == "" {
		return nil, fmt.Errorf("no OIDC token was specified")
	}
	sub, iss, err := extractSubIssWithoutVerifyingSignature(c.token)
	if err != nil {
		return nil, err
	}
	if sub == "" || iss == "" {
		return nil, errors.New("missing sub or iss claim in JWT token")
	}
	return &wifAttestation{
		ProviderType: string(oidcWif),
		Credential:   c.token,
		Metadata:     map[string]string{"sub": sub},
	}, nil
}

// azureAttestationMetadataProvider defines the interface for Azure attestation services
type azureAttestationMetadataProvider interface {
	identityEndpoint() string
	identityHeader() string
	clientID() string
}

type defaultAzureAttestationMetadataProvider struct{}

func (p *defaultAzureAttestationMetadataProvider) identityEndpoint() string {
	return os.Getenv("IDENTITY_ENDPOINT")
}

func (p *defaultAzureAttestationMetadataProvider) identityHeader() string {
	return os.Getenv("IDENTITY_HEADER")
}

func (p *defaultAzureAttestationMetadataProvider) clientID() string {
	return os.Getenv("MANAGED_IDENTITY_CLIENT_ID")
}

type azureIdentityAttestationCreator struct {
	azureAttestationMetadataProvider azureAttestationMetadataProvider
	cfg                              *Config
	telemetry                        *snowflakeTelemetry
	workloadIdentityEntraResource    string
	azureMetadataServiceBaseURL      string
}

// createAttestation creates an attestation using Azure identity
func (a *azureIdentityAttestationCreator) createAttestation() (*wifAttestation, error) {
	logger.Debug("Creating Azure identity attestation...")

	identityEndpoint := a.azureAttestationMetadataProvider.identityEndpoint()
	var request *http.Request
	var err error

	if identityEndpoint == "" {
		request, err = a.azureVMIdentityRequest()
		if err != nil {
			return nil, fmt.Errorf("failed to create Azure VM identity request: %v", err)
		}
	} else {
		identityHeader := a.azureAttestationMetadataProvider.identityHeader()
		if identityHeader == "" {
			return nil, fmt.Errorf("managed identity is not enabled on this Azure function")
		}
		request, err = a.azureFunctionsIdentityRequest(
			identityEndpoint,
			identityHeader,
			a.azureAttestationMetadataProvider.clientID(),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create Azure Functions identity request: %v", err)
		}
	}

	tokenJSON := fetchTokenFromMetadataService(request, a.cfg, a.telemetry)
	if tokenJSON == "" {
		return nil, fmt.Errorf("could not fetch Azure token")
	}

	token, err := extractTokenFromJSON(tokenJSON)
	if err != nil {
		return nil, fmt.Errorf("failed to extract token from JSON: %v", err)
	}
	if token == "" {
		return nil, fmt.Errorf("no access token found in Azure response")
	}

	sub, iss, err := extractSubIssWithoutVerifyingSignature(token)
	if err != nil {
		return nil, fmt.Errorf("failed to extract sub and iss claims from token: %v", err)
	}
	if sub == "" || iss == "" {
		return nil, fmt.Errorf("missing sub or iss claim in JWT token")
	}

	return &wifAttestation{
		ProviderType: string(azureWif),
		Credential:   token,
		Metadata:     map[string]string{"sub": sub, "iss": iss},
	}, nil
}

func determineEntraResource(config *Config) string {
	if config != nil && config.WorkloadIdentityEntraResource != "" {
		return config.WorkloadIdentityEntraResource
	}
	// default resource if none specified
	return "api://fd3f753b-eed3-462c-b6a7-a4b5bb650aad"
}

func extractTokenFromJSON(tokenJSON string) (string, error) {
	var response struct {
		AccessToken string `json:"access_token"`
	}

	err := json.Unmarshal([]byte(tokenJSON), &response)
	if err != nil {
		return "", err
	}

	return response.AccessToken, nil
}

func (a *azureIdentityAttestationCreator) azureFunctionsIdentityRequest(identityEndpoint, identityHeader, managedIdentityClientID string) (*http.Request, error) {
	queryParams := fmt.Sprintf("api-version=2019-08-01&resource=%s", a.workloadIdentityEntraResource)
	if managedIdentityClientID != "" {
		queryParams += fmt.Sprintf("&client_id=%s", managedIdentityClientID)
	}

	url := fmt.Sprintf("%s?%s", identityEndpoint, queryParams)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("X-IDENTITY-HEADER", identityHeader)

	return req, nil
}

func (a *azureIdentityAttestationCreator) azureVMIdentityRequest() (*http.Request, error) {
	urlWithoutQuery := a.azureMetadataServiceBaseURL + "/metadata/identity/oauth2/token?"
	queryParams := fmt.Sprintf("api-version=2018-02-01&resource=%s", a.workloadIdentityEntraResource)

	url := urlWithoutQuery + queryParams
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Metadata", "True")

	return req, nil
}
