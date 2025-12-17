package gosnowflake

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
)

type mockWifAttestationCreator struct {
	providerType wifProviderType
	returnError  error
}

func (m *mockWifAttestationCreator) createAttestation() (*wifAttestation, error) {
	if m.returnError != nil {
		return nil, m.returnError
	}
	return &wifAttestation{
		ProviderType: string(m.providerType),
	}, nil
}

func TestGetAttestation(t *testing.T) {
	awsError := errors.New("aws attestation error")
	gcpError := errors.New("gcp attestation error")
	azureError := errors.New("azure attestation error")
	oidcError := errors.New("oidc attestation error")

	provider := &wifAttestationProvider{
		context:      context.Background(),
		awsCreator:   &mockWifAttestationCreator{providerType: awsWif},
		gcpCreator:   &mockWifAttestationCreator{providerType: gcpWif},
		azureCreator: &mockWifAttestationCreator{providerType: azureWif},
		oidcCreator:  &mockWifAttestationCreator{providerType: oidcWif},
	}

	providerWithErrors := &wifAttestationProvider{
		context:      context.Background(),
		awsCreator:   &mockWifAttestationCreator{providerType: awsWif, returnError: awsError},
		gcpCreator:   &mockWifAttestationCreator{providerType: gcpWif, returnError: gcpError},
		azureCreator: &mockWifAttestationCreator{providerType: azureWif, returnError: azureError},
		oidcCreator:  &mockWifAttestationCreator{providerType: oidcWif, returnError: oidcError},
	}

	tests := []struct {
		name             string
		provider         *wifAttestationProvider
		identityProvider string
		expectedResult   *wifAttestation
		expectedError    error
	}{
		{
			name:             "AWS success",
			provider:         provider,
			identityProvider: "AWS",
			expectedResult:   &wifAttestation{ProviderType: string(awsWif)},
			expectedError:    nil,
		},
		{
			name:             "AWS error",
			provider:         providerWithErrors,
			identityProvider: "AWS",
			expectedResult:   nil,
			expectedError:    awsError,
		},
		{
			name:             "GCP success",
			provider:         provider,
			identityProvider: "GCP",
			expectedResult:   &wifAttestation{ProviderType: string(gcpWif)},
			expectedError:    nil,
		},
		{
			name:             "GCP error",
			provider:         providerWithErrors,
			identityProvider: "GCP",
			expectedResult:   nil,
			expectedError:    gcpError,
		},
		{
			name:             "AZURE success",
			provider:         provider,
			identityProvider: "AZURE",
			expectedResult:   &wifAttestation{ProviderType: string(azureWif)},
			expectedError:    nil,
		},
		{
			name:             "AZURE error",
			provider:         providerWithErrors,
			identityProvider: "AZURE",
			expectedResult:   nil,
			expectedError:    azureError,
		},
		{
			name:             "OIDC success",
			provider:         provider,
			identityProvider: "OIDC",
			expectedResult:   &wifAttestation{ProviderType: string(oidcWif)},
			expectedError:    nil,
		},
		{
			name:             "OIDC error",
			provider:         providerWithErrors,
			identityProvider: "OIDC",
			expectedResult:   nil,
			expectedError:    oidcError,
		},
		{
			name:             "Unknown provider",
			provider:         provider,
			identityProvider: "UNKNOWN",
			expectedResult:   nil,
			expectedError:    errors.New("unknown WorkloadIdentityProvider specified: UNKNOWN. Valid values are: AWS, GCP, AZURE, OIDC"),
		},
		{
			name:             "Empty provider",
			provider:         provider,
			identityProvider: "",
			expectedResult:   nil,
			expectedError:    errors.New("unknown WorkloadIdentityProvider specified: . Valid values are: AWS, GCP, AZURE, OIDC"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			attestation, err := test.provider.getAttestation(test.identityProvider)
			if test.expectedError != nil {
				assertNilE(t, attestation)
				assertNotNilF(t, err)
				assertEqualE(t, test.expectedError.Error(), err.Error())
			} else if test.expectedResult != nil {
				assertNilE(t, err)
				assertNotNilF(t, attestation)
				assertEqualE(t, test.expectedResult.ProviderType, attestation.ProviderType)
			} else {
				t.Fatal("test case must specify either expectedError or expectedResult")
			}
		})
	}
}

func TestAwsIdentityAttestationCreator(t *testing.T) {
	tests := []struct {
		name             string
		config           Config
		attestationSvc   awsAttestationMetadataProvider
		expectedError    error
		expectedProvider string
		expectedStsHost  string
	}{
		{
			name:           "No attestation service",
			attestationSvc: nil,
			expectedError:  fmt.Errorf("AWS attestation service could not be created"),
		},
		{
			name: "No AWS credentials",
			attestationSvc: &mockAwsAttestationMetadataProvider{
				creds:  aws.Credentials{},
				region: "us-west-2",
			},
			expectedError: fmt.Errorf("no AWS credentials were found"),
		},
		{
			name: "No AWS region",
			attestationSvc: &mockAwsAttestationMetadataProvider{
				creds:  mockCreds,
				region: "",
			},
			expectedError: fmt.Errorf("no AWS region was found"),
		},
		{
			name: "Successful attestation",
			attestationSvc: &mockAwsAttestationMetadataProvider{
				creds:  mockCreds,
				region: "us-west-2",
			},
			expectedProvider: "AWS",
			expectedStsHost:  "sts.us-west-2.amazonaws.com",
		},
		{
			name: "Successful attestation for CN region",
			attestationSvc: &mockAwsAttestationMetadataProvider{
				creds:  mockCreds,
				region: "cn-northwest-1",
			},
			expectedProvider: "AWS",
			expectedStsHost:  "sts.cn-northwest-1.amazonaws.com.cn",
		},
		{
			name: "Successful attestation with single role chaining",
			config: Config{
				WorkloadIdentityImpersonationPath: []string{"arn:aws:iam::123456789012:role/test-role"},
			},
			attestationSvc: &mockAwsAttestationMetadataProvider{
				creds: mockCreds,
				chainingCreds: aws.Credentials{
					AccessKeyID:     "chainedAccessKey",
					SecretAccessKey: "chainedSecretKey",
					SessionToken:    "chainedSessionToken",
				},
				region:          "us-east-1",
				useRoleChaining: true,
			},
			expectedProvider: "AWS",
			expectedStsHost:  "sts.us-east-1.amazonaws.com",
		},
		{
			name: "Successful attestation with multiple role chaining",
			config: Config{
				WorkloadIdentityImpersonationPath: []string{
					"arn:aws:iam::123456789012:role/role1",
					"arn:aws:iam::123456789012:role/role2",
					"arn:aws:iam::123456789012:role/role3",
				},
			},
			attestationSvc: &mockAwsAttestationMetadataProvider{
				creds: mockCreds,
				chainingCreds: aws.Credentials{
					AccessKeyID:     "finalRoleAccessKey",
					SecretAccessKey: "finalRoleSecretKey",
					SessionToken:    "finalRoleSessionToken",
				},
				region:          "us-west-2",
				useRoleChaining: true,
			},
			expectedProvider: "AWS",
			expectedStsHost:  "sts.us-west-2.amazonaws.com",
		},
		{
			name: "Role chaining with no credentials",
			config: Config{
				WorkloadIdentityImpersonationPath: []string{"arn:aws:iam::123456789012:role/test-role"},
			},
			attestationSvc: &mockAwsAttestationMetadataProvider{
				creds:           aws.Credentials{},
				region:          "us-west-2",
				useRoleChaining: true,
			},
			expectedError: fmt.Errorf("no AWS credentials were found"),
		},
		{
			name: "Role chaining with no region",
			config: Config{
				WorkloadIdentityImpersonationPath: []string{"arn:aws:iam::123456789012:role/test-role"},
			},
			attestationSvc: &mockAwsAttestationMetadataProvider{
				creds: aws.Credentials{
					AccessKeyID:     "chainedAccessKey",
					SecretAccessKey: "chainedSecretKey",
					SessionToken:    "chainedSessionToken",
				},
				region:          "",
				useRoleChaining: true,
			},
			expectedError: fmt.Errorf("no AWS region was found"),
		},
		{
			name: "Role chaining failure",
			config: Config{
				WorkloadIdentityImpersonationPath: []string{"arn:aws:iam::123456789012:role/test-role"},
			},
			attestationSvc: &mockAwsAttestationMetadataProvider{
				creds:           mockCreds,
				region:          "us-west-2",
				chainingError:   fmt.Errorf("failed to assume role: AccessDenied"),
				useRoleChaining: true,
			},
			expectedError: fmt.Errorf("failed to assume role: AccessDenied"),
		},
		{
			name: "Cross-account role chaining",
			config: Config{
				WorkloadIdentityImpersonationPath: []string{
					"arn:aws:iam::111111111111:role/cross-account-role",
					"arn:aws:iam::222222222222:role/target-role",
				},
			},
			attestationSvc: &mockAwsAttestationMetadataProvider{
				creds: mockCreds,
				chainingCreds: aws.Credentials{
					AccessKeyID:     "crossAccountAccessKey",
					SecretAccessKey: "crossAccountSecretKey",
					SessionToken:    "crossAccountSessionToken",
				},
				region:          "us-east-1",
				useRoleChaining: true,
			},
			expectedProvider: "AWS",
			expectedStsHost:  "sts.us-east-1.amazonaws.com",
		},
		{
			name: "Role chaining in CN region",
			config: Config{
				WorkloadIdentityImpersonationPath: []string{"arn:aws-cn:iam::123456789012:role/cn-role"},
			},
			attestationSvc: &mockAwsAttestationMetadataProvider{
				creds: mockCreds,
				chainingCreds: aws.Credentials{
					AccessKeyID:     "cnRoleAccessKey",
					SecretAccessKey: "cnRoleSecretKey",
					SessionToken:    "cnRoleSessionToken",
				},
				region:          "cn-north-1",
				useRoleChaining: true,
			},
			expectedProvider: "AWS",
			expectedStsHost:  "sts.cn-north-1.amazonaws.com.cn",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			creator := &awsIdentityAttestationCreator{
				attestationServiceFactory: func(ctx context.Context, cfg *Config) awsAttestationMetadataProvider {
					return test.attestationSvc
				},
				ctx: context.Background(),
				cfg: &test.config,
			}
			attestation, err := creator.createAttestation()
			if test.expectedError != nil {
				assertNilF(t, attestation)
				assertNotNilE(t, err)
				assertEqualE(t, test.expectedError.Error(), err.Error())
			} else {
				assertNilE(t, err)
				assertNotNilE(t, attestation)
				assertNotNilE(t, attestation.Credential)
				assertEqualE(t, test.expectedProvider, attestation.ProviderType)
				decoded, err := base64.StdEncoding.DecodeString(attestation.Credential)
				if err != nil {
					t.Fatalf("Failed to decode credential: %v", err)
				}
				var credentialMap map[string]interface{}
				if err := json.Unmarshal(decoded, &credentialMap); err != nil {
					t.Fatalf("Failed to unmarshal credential JSON: %v", err)
				}
				assertEqualE(t, fmt.Sprintf("https://%s?Action=GetCallerIdentity&Version=2011-06-15", test.expectedStsHost), credentialMap["url"])
			}
		})
	}
}

type mockAwsAttestationMetadataProvider struct {
	creds           aws.Credentials
	region          string
	chainingCreds   aws.Credentials
	chainingError   error
	useRoleChaining bool
}

var mockCreds = aws.Credentials{
	AccessKeyID:     "mockAccessKey",
	SecretAccessKey: "mockSecretKey",
	SessionToken:    "mockSessionToken",
}

func (m *mockAwsAttestationMetadataProvider) awsCredentials() (aws.Credentials, error) {
	return m.creds, nil
}

func (m *mockAwsAttestationMetadataProvider) awsCredentialsViaRoleChaining() (aws.Credentials, error) {
	if m.chainingError != nil {
		return aws.Credentials{}, m.chainingError
	}
	if m.chainingCreds.AccessKeyID != "" {
		return m.chainingCreds, nil
	}
	return m.creds, nil
}

func (m *mockAwsAttestationMetadataProvider) awsRegion() string {
	return m.region
}

func TestGcpIdentityAttestationCreator(t *testing.T) {
	tests := []struct {
		name                string
		wiremockMappingPath string
		config              Config
		expectedError       error
		expectedSub         string
	}{
		{
			name:                "Successful flow",
			wiremockMappingPath: "auth/wif/gcp/successful_flow.json",
			expectedError:       nil,
			expectedSub:         "some-subject",
		},
		{
			name:                "Successful impersonation flow",
			wiremockMappingPath: "auth/wif/gcp/successful_impersionation_flow.json",
			config: Config{
				WorkloadIdentityImpersonationPath: []string{
					"delegate1",
					"delegate2",
					"targetServiceAccount",
				},
			},
			expectedError: nil,
			expectedSub:   "some-impersonated-subject",
		},
		{
			name:                "No GCP credential - http error",
			wiremockMappingPath: "auth/wif/gcp/http_error.json",
			expectedError:       fmt.Errorf("no GCP token was found"),
			expectedSub:         "",
		},
		{
			name:                "missing issuer claim",
			wiremockMappingPath: "auth/wif/gcp/missing_issuer_claim.json",
			expectedError:       fmt.Errorf("could not extract claims from token: missing issuer claim in JWT token"),
			expectedSub:         "",
		},
		{
			name:                "missing sub claim",
			wiremockMappingPath: "auth/wif/gcp/missing_sub_claim.json",
			expectedError:       fmt.Errorf("could not extract claims from token: missing sub claim in JWT token"),
			expectedSub:         "",
		},
		{
			name:                "unparsable token",
			wiremockMappingPath: "auth/wif/gcp/unparsable_token.json",
			expectedError:       fmt.Errorf("could not extract claims from token: unable to extract JWT claims from token: token is malformed: token contains an invalid number of segments"),
			expectedSub:         "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			creator := &gcpIdentityAttestationCreator{
				cfg:                    &test.config,
				metadataServiceBaseURL: wiremock.baseURL(),
				iamCredentialsURL:      wiremock.baseURL(),
			}
			wiremock.registerMappings(t, wiremockMapping{filePath: test.wiremockMappingPath})
			attestation, err := creator.createAttestation()

			if test.expectedError != nil {
				assertNilF(t, attestation)
				assertNotNilF(t, err)
				assertEqualE(t, test.expectedError.Error(), err.Error())
			} else {
				assertNilF(t, err)
				assertNotNilF(t, attestation)
				assertEqualE(t, string(gcpWif), attestation.ProviderType)
				assertEqualE(t, test.expectedSub, attestation.Metadata["sub"])
			}
		})
	}
}

func TestOidcIdentityAttestationCreator(t *testing.T) {
	const (
		/*
		 * {
		 *   "sub": "some-subject",
		 *   "iat": 1743761213,
		 *   "exp": 1743764813,
		 *   "aud": "www.example.com"
		 * }
		 */
		missingIssuerClaimToken = "eyJ0eXAiOiJhdCtqd3QiLCJhbGciOiJFUzI1NiIsImtpZCI6ImU2M2I5NzA1OTRiY2NmZTAxMDlkOTg4OWM2MDk3OWEwIn0.eyJzdWIiOiJzb21lLXN1YmplY3QiLCJpYXQiOjE3NDM3NjEyMTMsImV4cCI6MTc0Mzc2NDgxMywiYXVkIjoid3d3LmV4YW1wbGUuY29tIn0.H6sN6kjA82EuijFcv-yCJTqau5qvVTCsk0ZQ4gvFQMkB7c71XPs4lkwTa7ZlNNlx9e6TpN1CVGnpCIRDDAZaDw" // pragma: allowlist secret
		/*
		 * {
		 *   "iss": "https://accounts.google.com",
		 *   "iat": 1743761213,
		 *   "exp": 1743764813,
		 *   "aud": "www.example.com"
		 * }
		 */
		missingSubClaimToken = "eyJ0eXAiOiJhdCtqd3QiLCJhbGciOiJFUzI1NiIsImtpZCI6ImU2M2I5NzA1OTRiY2NmZTAxMDlkOTg4OWM2MDk3OWEwIn0.eyJpc3MiOiJodHRwczovL2FjY291bnRzLmdvb2dsZS5jb20iLCJpYXQiOjE3NDM3NjEyMTMsImV4cCI6MTc0Mzc2NDgxMywiYXVkIjoid3d3LmV4YW1wbGUuY29tIn0.w0njdpfWFETVK8Ktq9GdvuKRQJjvhOplcSyvQ_zHHwBUSMapqO1bjEWBx5VhGkdECZIGS1VY7db_IOqT45yOMA" // pragma: allowlist secret
		/*
		 * {
		 *     "iss": "https://oidc.eks.us-east-2.amazonaws.com/id/3B869BC5D12CEB5515358621D8085D58",
		 *     "iat": 1743692017,
		 *     "exp": 1775228014,
		 *     "aud": "www.example.com",
		 *     "sub": "system:serviceaccount:poc-namespace:oidc-sa"
		 * }
		 */
		validToken      = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL29pZGMuZWtzLnVzLWVhc3QtMi5hbWF6b25hd3MuY29tL2lkLzNCODY5QkM1RDEyQ0VCNTUxNTM1ODYyMUQ4MDg1RDU4IiwiaWF0IjoxNzQ0Mjg3ODc4LCJleHAiOjE3NzU4MjM4NzgsImF1ZCI6Ind3dy5leGFtcGxlLmNvbSIsInN1YiI6InN5c3RlbTpzZXJ2aWNlYWNjb3VudDpwb2MtbmFtZXNwYWNlOm9pZGMtc2EifQ.a8H6KRIF1XmM8lkqL6kR8ccInr7wAzQrbKd3ZHFgiEg" // pragma: allowlist secret
		unparsableToken = "unparsable_token"
		emptyToken      = ""
	)

	type testCase struct {
		name          string
		token         string
		expectedError error
		expectedSub   string
	}

	tests := []testCase{
		{
			name:          "no token input",
			token:         emptyToken,
			expectedError: fmt.Errorf("no OIDC token was specified"),
		},
		{
			name:          "valid token returns proper attestation",
			token:         validToken,
			expectedError: nil,
			expectedSub:   "system:serviceaccount:poc-namespace:oidc-sa",
		},
		{
			name:          "missing issuer returns error",
			token:         missingIssuerClaimToken,
			expectedError: errors.New("missing issuer claim in JWT token"),
		},
		{
			name:          "missing sub returns error",
			token:         missingSubClaimToken,
			expectedError: errors.New("missing sub claim in JWT token"),
		},
		{
			name:          "unparsable token returns error",
			token:         unparsableToken,
			expectedError: errors.New("unable to extract JWT claims from token: token is malformed: token contains an invalid number of segments"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			creator := &oidcIdentityAttestationCreator{token: test.token}
			attestation, err := creator.createAttestation()

			if test.expectedError != nil {
				assertNotNilE(t, err)
				assertNilF(t, attestation)
				assertEqualE(t, test.expectedError.Error(), err.Error())
			} else {
				assertNilE(t, err)
				assertNotNilE(t, attestation)
				assertEqualE(t, string(oidcWif), attestation.ProviderType)
				assertEqualE(t, test.expectedSub, attestation.Metadata["sub"])
			}
		})
	}
}

func TestAzureIdentityAttestationCreator(t *testing.T) {
	tests := []struct {
		name                string
		wiremockMappingPath string
		metadataProvider    *mockAzureAttestationMetadataProvider
		cfg                 *Config
		expectedIss         string
		expectedError       error
	}{
		/*
		 * {
		 *     "iss": "https://sts.windows.net/fa15d692-e9c7-4460-a743-29f29522229/",
		 *     "sub": "77213E30-E8CB-4595-B1B6-5F050E8308FD"
		 * }
		 */
		{
			name:                "Successful flow",
			wiremockMappingPath: "auth/wif/azure/successful_flow_basic.json",
			metadataProvider:    azureVMMetadataProvider(),
			expectedIss:         "https://sts.windows.net/fa15d692-e9c7-4460-a743-29f29522229/",
			expectedError:       nil,
		},
		/*
		 * {
		 *     "iss": "https://login.microsoftonline.com/fa15d692-e9c7-4460-a743-29f29522229/",
		 *     "sub": "77213E30-E8CB-4595-B1B6-5F050E8308FD"
		 * }
		 */
		{
			name:                "Successful flow v2 issuer",
			wiremockMappingPath: "auth/wif/azure/successful_flow_v2_issuer.json",
			metadataProvider:    azureVMMetadataProvider(),
			expectedIss:         "https://login.microsoftonline.com/fa15d692-e9c7-4460-a743-29f29522229/",
			expectedError:       nil,
		},
		/*
		 * {
		 *     "iss": "https://sts.windows.net/fa15d692-e9c7-4460-a743-29f29522229/",
		 *     "sub": "77213E30-E8CB-4595-B1B6-5F050E8308FD"
		 * }
		 */
		{
			name:                "Successful flow azure functions",
			wiremockMappingPath: "auth/wif/azure/successful_flow_azure_functions.json",
			metadataProvider:    azureFunctionsMetadataProvider(),
			expectedIss:         "https://sts.windows.net/fa15d692-e9c7-4460-a743-29f29522229/",
			expectedError:       nil,
		},
		/*
		 * {
		 *     "iss": "https://login.microsoftonline.com/fa15d692-e9c7-4460-a743-29f29522229/",
		 *     "sub": "77213E30-E8CB-4595-B1B6-5F050E8308FD"
		 * }
		 */
		{
			name:                "Successful flow azure functions v2 issuer",
			wiremockMappingPath: "auth/wif/azure/successful_flow_azure_functions_v2_issuer.json",
			metadataProvider:    azureFunctionsMetadataProvider(),
			expectedIss:         "https://login.microsoftonline.com/fa15d692-e9c7-4460-a743-29f29522229/",
			expectedError:       nil,
		},
		/*
		 * {
		 *     "iss": "https://sts.windows.net/fa15d692-e9c7-4460-a743-29f29522229/",
		 *     "sub": "77213E30-E8CB-4595-B1B6-5F050E8308FD"
		 * }
		 */
		{
			name:                "Successful flow azure functions no client ID",
			wiremockMappingPath: "auth/wif/azure/successful_flow_azure_functions_no_client_id.json",
			metadataProvider: &mockAzureAttestationMetadataProvider{
				identityEndpointValue: wiremock.baseURL() + "/metadata/identity/endpoint/from/env",
				identityHeaderValue:   "some-identity-header-from-env",
				clientIDValue:         "",
			},
			expectedIss:   "https://sts.windows.net/fa15d692-e9c7-4460-a743-29f29522229/",
			expectedError: nil,
		},
		/*
		 * {
		 *     "iss": "https://sts.windows.net/fa15d692-e9c7-4460-a743-29f29522229/",
		 *     "sub": "77213E30-E8CB-4595-B1B6-5F050E8308FD"
		 * }
		 */
		{
			name:                "Successful flow azure functions custom Entra resource",
			wiremockMappingPath: "auth/wif/azure/successful_flow_azure_functions_custom_entra_resource.json",
			metadataProvider:    azureFunctionsMetadataProvider(),
			cfg:                 &Config{WorkloadIdentityEntraResource: "api://1111111-2222-3333-44444-55555555"},
			expectedIss:         "https://sts.windows.net/fa15d692-e9c7-4460-a743-29f29522229/",
			expectedError:       nil,
		},
		{
			name:                "Non-json response",
			wiremockMappingPath: "auth/wif/azure/non_json_response.json",
			metadataProvider:    azureVMMetadataProvider(),
			expectedError:       fmt.Errorf("failed to extract token from JSON: invalid character 'o' in literal null (expecting 'u')"),
		},
		{
			name: "Identity endpoint but no identity header",
			metadataProvider: &mockAzureAttestationMetadataProvider{
				identityEndpointValue: wiremock.baseURL() + "/metadata/identity/endpoint/from/env",
				identityHeaderValue:   "",
				clientIDValue:         "managed-client-id-from-env",
			},
			expectedError: fmt.Errorf("managed identity is not enabled on this Azure function"),
		},
		{
			name:                "Unparsable token",
			wiremockMappingPath: "auth/wif/azure/unparsable_token.json",
			metadataProvider:    azureVMMetadataProvider(),
			expectedError:       fmt.Errorf("failed to extract sub and iss claims from token: unable to extract JWT claims from token: token is malformed: token contains an invalid number of segments"),
		},
		{
			name:                "HTTP error",
			metadataProvider:    azureVMMetadataProvider(),
			wiremockMappingPath: "auth/wif/azure/http_error.json",
			expectedError:       fmt.Errorf("could not fetch Azure token"),
		},
		{
			name:                "Missing sub or iss claim",
			wiremockMappingPath: "auth/wif/azure/missing_issuer_claim.json",
			metadataProvider:    azureVMMetadataProvider(),
			expectedError:       fmt.Errorf("failed to extract sub and iss claims from token: missing issuer claim in JWT token"),
		},
		{
			name:                "Missing sub claim",
			wiremockMappingPath: "auth/wif/azure/missing_sub_claim.json",
			metadataProvider:    azureVMMetadataProvider(),
			expectedError:       fmt.Errorf("failed to extract sub and iss claims from token: missing sub claim in JWT token"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.wiremockMappingPath != "" {
				wiremock.registerMappings(t, wiremockMapping{filePath: test.wiremockMappingPath})
			}
			creator := &azureIdentityAttestationCreator{
				cfg:                              test.cfg,
				azureMetadataServiceBaseURL:      wiremock.baseURL(),
				azureAttestationMetadataProvider: test.metadataProvider,
				workloadIdentityEntraResource:    determineEntraResource(test.cfg),
			}
			attestation, err := creator.createAttestation()

			if test.expectedError != nil {
				assertNilF(t, attestation)
				assertNotNilE(t, err)
				assertEqualE(t, test.expectedError.Error(), err.Error())
			} else {
				assertNilE(t, err)
				assertNotNilE(t, attestation)
				assertEqualE(t, string(azureWif), attestation.ProviderType)
				assertEqualE(t, test.expectedIss, attestation.Metadata["iss"])
				assertEqualE(t, "77213E30-E8CB-4595-B1B6-5F050E8308FD", attestation.Metadata["sub"])
			}
		})
	}
}

type mockAzureAttestationMetadataProvider struct {
	identityEndpointValue string
	identityHeaderValue   string
	clientIDValue         string
}

func (m *mockAzureAttestationMetadataProvider) identityEndpoint() string {
	return m.identityEndpointValue
}

func (m *mockAzureAttestationMetadataProvider) identityHeader() string {
	return m.identityHeaderValue
}

func (m *mockAzureAttestationMetadataProvider) clientID() string {
	return m.clientIDValue
}

func azureFunctionsMetadataProvider() *mockAzureAttestationMetadataProvider {
	return &mockAzureAttestationMetadataProvider{
		identityEndpointValue: wiremock.baseURL() + "/metadata/identity/endpoint/from/env",
		identityHeaderValue:   "some-identity-header-from-env",
		clientIDValue:         "managed-client-id-from-env",
	}
}

func azureVMMetadataProvider() *mockAzureAttestationMetadataProvider {
	return &mockAzureAttestationMetadataProvider{
		identityEndpointValue: "",
		identityHeaderValue:   "",
		clientIDValue:         "",
	}
}

// Running this test locally:
// * Push branch to repository
// * Set PARAMETERS_SECRET
// * Run ci/test_wif.sh
func TestWorkloadIdentityAuthOnCloudVM(t *testing.T) {
	account := os.Getenv("SNOWFLAKE_TEST_WIF_ACCOUNT")
	host := os.Getenv("SNOWFLAKE_TEST_WIF_HOST")
	provider := os.Getenv("SNOWFLAKE_TEST_WIF_PROVIDER")
	println("provider = " + provider)
	if account == "" || host == "" || provider == "" {
		t.Skip("Test can run only on cloud VM with env variables set")
	}
	testCases := []struct {
		name             string
		skip             func() (bool, string)
		setupCfg         func(*testing.T, *Config)
		expectedUsername string
	}{
		{
			name: "provider=" + provider,
			setupCfg: func(_ *testing.T, config *Config) {
				if provider != "GCP+OIDC" {
					config.WorkloadIdentityProvider = provider
				} else {
					config.WorkloadIdentityProvider = "OIDC"
					config.Token = func() string {
						cmd := exec.Command("wget", "-O", "-", "--header=Metadata-Flavor: Google", "http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/identity?audience=snowflakecomputing.com")
						output, err := cmd.Output()
						if err != nil {
							t.Fatalf("error executing GCP metadata request: %v", err)
						}
						token := strings.TrimSpace(string(output))
						if token == "" {
							t.Fatal("failed to retrieve GCP access token: empty response")
						}
						return token
					}()
				}
			},
			expectedUsername: os.Getenv("SNOWFLAKE_TEST_WIF_USERNAME"),
		},
		{
			name: "provider=" + provider + ",impersonation",
			skip: func() (bool, string) {
				if provider != "AWS" && provider != "GCP" {
					return true, "Impersonation is supported only on AWS and GCP"
				}
				return false, ""
			},
			setupCfg: func(t *testing.T, config *Config) {
				config.WorkloadIdentityProvider = provider
				impersonationPath := os.Getenv("SNOWFLAKE_TEST_WIF_IMPERSONATION_PATH")
				assertNotEqualF(t, impersonationPath, "", "SNOWFLAKE_TEST_WIF_IMPERSONATION_PATH is not set")
				config.WorkloadIdentityImpersonationPath = strings.Split(impersonationPath, ",")
				assertNotEqualF(t, os.Getenv("SNOWFLAKE_TEST_WIF_USERNAME_IMPERSONATION"), "", "SNOWFLAKE_TEST_WIF_USERNAME_IMPERSONATION is not set")
			},
			expectedUsername: os.Getenv("SNOWFLAKE_TEST_WIF_USERNAME_IMPERSONATION"),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skip != nil {
				if skip, msg := tc.skip(); skip {
					t.Skip(msg)
				}
			}
			config := &Config{
				Account:       account,
				Host:          host,
				Authenticator: AuthTypeWorkloadIdentityFederation,
			}
			tc.setupCfg(t, config)
			connector := NewConnector(SnowflakeDriver{}, *config)
			db := sql.OpenDB(connector)
			defer db.Close()
			currentUser := runSelectCurrentUser(t, db)
			assertEqualE(t, currentUser, tc.expectedUsername)
		})
	}
}
