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
	"github.com/golang-jwt/jwt/v5"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

const (
	awsWif   wifProviderType = "AWS"
	gcpWif   wifProviderType = "GCP"
	azureWif wifProviderType = "AZURE"
	oidcWif  wifProviderType = "OIDC"

	gcpMetadataFlavorHeaderName = "Metadata-Flavor"
	gcpMetadataFlavor           = "Google"
	expectedGcpTokenIssuer      = "https://accounts.google.com"
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

func createWifAttestationProvider(ctx context.Context, cfg *Config) *wifAttestationProvider {
	return &wifAttestationProvider{
		context: ctx,
		cfg:     cfg,
		awsCreator: &awsIdentityAttestationCreator{
			attestationService: createDefaultAwsAttestationMetadataProvider(ctx),
			ctx:                ctx},
		gcpCreator: &gcpIdentityAttestationCreator{
			cfg:                    cfg,
			metadataServiceBaseURL: defaultMetadataServiceBase},
		azureCreator: &azureIdentityAttestationCreator{
			azureAttestationMetadataProvider: &defaultAzureAttestationMetadataProvider{},
			cfg:                              cfg,
			workloadIdentityEntraResource:    determineEntraResource(cfg),
			azureMetadataServiceBaseURL:      defaultMetadataServiceBase,
		},
		oidcCreator: &oidcIdentityAttestationCreator{token: cfg.Token},
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
	return creator.createAttestation()
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
	attestation, err := creator.createAttestation()
	if err != nil {
		logger.Errorf("Unable to create identity attestation for %s, error: %v", providerType, err)
		return nil
	}
	return attestation
}

type awsIdentityAttestationCreator struct {
	attestationService awsAttestationMetadataProvider
	ctx                context.Context
}

type gcpIdentityAttestationCreator struct {
	cfg                    *Config
	metadataServiceBaseURL string
}

type oidcIdentityAttestationCreator struct {
	token string
}

type awsAttestationMetadataProvider interface {
	awsCredentials() aws.Credentials
	awsRegion() string
	awsArn() string
}

type defaultAwsAttestationMetadataProvider struct {
	ctx    context.Context
	awsCfg aws.Config
}

func createDefaultAwsAttestationMetadataProvider(ctx context.Context) *defaultAwsAttestationMetadataProvider {
	cfg, err := config.LoadDefaultConfig(ctx)
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

func (s *defaultAwsAttestationMetadataProvider) awsArn() string {
	client := sts.NewFromConfig(s.awsCfg)
	output, err := client.GetCallerIdentity(s.ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		logger.Debugf("Unable to get caller identity: %v", err)
		return ""
	}
	return aws.ToString(output.Arn)
}

func (c *awsIdentityAttestationCreator) createAttestation() (*wifAttestation, error) {
	logger.Debug("Creating AWS identity attestation...")

	if c.attestationService == nil {
		logger.Debug("AWS attestation service could not be created.")
		return nil, nil
	}

	creds := c.attestationService.awsCredentials()
	if creds.AccessKeyID == "" || creds.SecretAccessKey == "" {
		logger.Debug("No AWS credentials were found.")
		return nil, nil
	}

	region := c.attestationService.awsRegion()
	if region == "" {
		logger.Debug("No AWS region was found.")
		return nil, nil
	}

	arn := c.attestationService.awsArn()
	if arn == "" {
		logger.Debug("No Caller Identity was found.")
		return nil, nil
	}

	stsHostname := fmt.Sprintf("sts.%s.amazonaws.com", region)
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
		Metadata:     map[string]string{"arn": arn},
	}, nil
}

func (c *awsIdentityAttestationCreator) createStsRequest(hostname string) (*http.Request, error) {
	url := fmt.Sprintf("https://%s/?Action=GetCallerIdentity&Version=2011-06-15", hostname)
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
	token := fetchTokenFromMetadataService(req, c.cfg)
	if token == "" {
		logger.Debugf("no GCP token was found.")
		return nil, nil
	}
	sub, iss, err := extractSubIssWithoutVerifyingSignature(token)
	if err != nil {
		logger.Errorf("could not extract claims from token: %v", err.Error())
		return nil, nil
	}
	if iss != expectedGcpTokenIssuer {
		logger.Errorf("unexpected token issuer: %s, should be %s", iss, expectedGcpTokenIssuer)
		return nil, nil
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

func fetchTokenFromMetadataService(req *http.Request, cfg *Config) string {
	client := &http.Client{Transport: getTransport(cfg)}
	resp, err := client.Do(req)
	if err != nil {
		logger.Debugf("Metadata server request was not successful: %v", err)
		return ""
	}
	defer resp.Body.Close()
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
		logger.Debugf("No OIDC token was specified")
		return nil, nil
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
	workloadIdentityEntraResource    string
	azureMetadataServiceBaseURL      string
}

var allowedAzureTokenIssuerPrefixes = []string{
	"https://sts.windows.net/",
	"https://login.microsoftonline.com/",
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
			logger.Warn("Managed identity is not enabled on this Azure function.")
			return nil, nil
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

	tokenJSON := fetchTokenFromMetadataService(request, a.cfg)
	if tokenJSON == "" {
		logger.Debug("Could not fetch Azure token.")
		return nil, nil
	}

	token, err := extractTokenFromJSON(tokenJSON)
	if err != nil {
		logger.Errorf("Failed to extract token from JSON: %v", err)
		return nil, nil
	}
	if token == "" {
		logger.Error("No access token found in Azure response.")
		return nil, nil
	}

	sub, iss, err := extractSubIssWithoutVerifyingSignature(token)
	if err != nil {
		logger.Errorf("Failed to extract sub and iss claims from token: %v", err)
		return nil, nil
	}
	if sub == "" || iss == "" {
		logger.Error("Missing sub or iss claim in JWT token")
		return nil, nil
	}

	hasAllowedPrefix := false
	for _, prefix := range allowedAzureTokenIssuerPrefixes {
		if strings.HasPrefix(iss, prefix) {
			hasAllowedPrefix = true
			break
		}
	}

	if !hasAllowedPrefix {
		logger.Errorf("Unexpected Azure token issuer: %s", iss)
		return nil, nil
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
