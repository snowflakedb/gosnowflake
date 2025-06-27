package gosnowflake

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"testing"
)

var awsErrorCreator = &mockWifAttestationCreator{attestation: nil, err: errors.New("AWS error")}
var gcpErrorCreator = &mockWifAttestationCreator{attestation: nil, err: errors.New("GCP error")}
var azureErrorCreator = &mockWifAttestationCreator{attestation: nil, err: errors.New("AZURE error")}
var oidcErrorCreator = &mockWifAttestationCreator{attestation: nil, err: errors.New("OIDC error")}

type mockWifAttestationCreator struct {
	attestation *wifAttestation
	err         error
}

func (m *mockWifAttestationCreator) createAttestation() (*wifAttestation, error) {
	return m.attestation, m.err
}

func TestAttestationCreator(t *testing.T) {
	provider := &wifAttestationProvider{
		context:      context.Background(),
		awsCreator:   &mockWifAttestationCreator{},
		gcpCreator:   &mockWifAttestationCreator{},
		azureCreator: &mockWifAttestationCreator{},
		oidcCreator:  &mockWifAttestationCreator{},
	}

	tests := []struct {
		identityProvider string
		expectedCreator  wifAttestationCreator
		expectedError    error
	}{
		{"AWS", provider.awsCreator, nil},
		{"GCP", provider.gcpCreator, nil},
		{"AZURE", provider.azureCreator, nil},
		{"OIDC", provider.oidcCreator, nil},
		{"UNKNOWN", nil, errors.New("unknown Workload Identity provider specified: UNKNOWN")},
		{"", nil, errors.New("unknown Workload Identity provider specified: ")},
	}

	for _, test := range tests {
		t.Run(test.identityProvider, func(t *testing.T) {
			creator, err := provider.attestationCreator(test.identityProvider)
			if creator != test.expectedCreator {
				t.Errorf("expected creator %v, got %v", test.expectedCreator, creator)
			}
			if (err != nil && test.expectedError == nil) || (err == nil && test.expectedError != nil) || (err != nil && test.expectedError != nil && err.Error() != test.expectedError.Error()) {
				t.Errorf("expected error %v, got %v", test.expectedError, err)
			}
		})
	}
}

func TestCreateAutodetectAttestation(t *testing.T) {
	tests := []struct {
		name             string
		oidcCreator      *mockWifAttestationCreator
		awsCreator       *mockWifAttestationCreator
		gcpCreator       *mockWifAttestationCreator
		azureCreator     *mockWifAttestationCreator
		expectedProvider string
		expectedError    string
	}{
		{
			name: "OIDC",
			oidcCreator: &mockWifAttestationCreator{
				attestation: &wifAttestation{ProviderType: "OIDC", Credential: "oidc-credential"},
				err:         nil,
			},
			awsCreator:       awsErrorCreator,
			gcpCreator:       gcpErrorCreator,
			azureCreator:     azureErrorCreator,
			expectedProvider: "OIDC",
			expectedError:    "",
		},
		{
			name:        "AWS",
			oidcCreator: oidcErrorCreator,
			awsCreator: &mockWifAttestationCreator{
				attestation: &wifAttestation{ProviderType: "AWS", Credential: "aws-credential"},
				err:         nil,
			},
			gcpCreator:       gcpErrorCreator,
			azureCreator:     azureErrorCreator,
			expectedProvider: "AWS",
			expectedError:    "",
		},
		{
			name:        "GCP",
			oidcCreator: oidcErrorCreator,
			awsCreator:  awsErrorCreator,
			gcpCreator: &mockWifAttestationCreator{
				attestation: &wifAttestation{ProviderType: "GCP", Credential: "gcp-credential"},
				err:         nil,
			},
			azureCreator:     azureErrorCreator,
			expectedProvider: "GCP",
			expectedError:    "",
		},
		{
			name:        "Azure",
			oidcCreator: oidcErrorCreator,
			awsCreator:  awsErrorCreator,
			gcpCreator:  gcpErrorCreator,
			azureCreator: &mockWifAttestationCreator{
				attestation: &wifAttestation{ProviderType: "AZURE", Credential: "azure-credential"},
				err:         nil,
			},
			expectedProvider: "AZURE",
			expectedError:    "",
		},
		{
			name:             "None",
			oidcCreator:      oidcErrorCreator,
			awsCreator:       awsErrorCreator,
			gcpCreator:       gcpErrorCreator,
			azureCreator:     azureErrorCreator,
			expectedProvider: "",
			expectedError:    "unable to autodetect Workload Identity. None of the supported Workload Identity environments has been identified",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			provider := &wifAttestationProvider{
				oidcCreator:  test.oidcCreator,
				awsCreator:   test.awsCreator,
				gcpCreator:   test.gcpCreator,
				azureCreator: test.azureCreator,
			}

			attestation, err := provider.createAutodetectAttestation()

			if test.expectedError == "" {
				assertNilE(t, err)
				assertNotNilE(t, attestation)
				assertEqualE(t, test.expectedProvider, attestation.ProviderType)
			} else {
				assertNotNilE(t, err)
				assertNilE(t, attestation)
				assertEqualE(t, test.expectedError, err.Error())
			}
		})
	}
}

func TestAwsIdentityAttestationCreator(t *testing.T) {
	tests := []struct {
		name                string
		attestationSvc      awsAttestationMetadataProvider
		attestationReturned bool
		expectedProvider    string
		expectedStsHost     string
	}{
		{
			name: "No AWS credentials",
			attestationSvc: &mockAwsAttestationMetadataProvider{
				creds:  aws.Credentials{},
				region: "us-west-2",
			},
			attestationReturned: false,
		},
		{
			name: "No AWS region",
			attestationSvc: &mockAwsAttestationMetadataProvider{
				creds:  mockCreds,
				region: "",
			},
			attestationReturned: false,
		},
		{
			name: "Successful attestation",
			attestationSvc: &mockAwsAttestationMetadataProvider{
				creds:  mockCreds,
				region: "us-west-2",
			},
			attestationReturned: true,
			expectedProvider:    "AWS",
			expectedStsHost:     "sts.us-west-2.amazonaws.com",
		},
		{
			name: "Successful attestation for CN region",
			attestationSvc: &mockAwsAttestationMetadataProvider{
				creds:  mockCreds,
				region: "cn-northwest-1",
			},
			attestationReturned: true,
			expectedProvider:    "AWS",
			expectedStsHost:     "sts.cn-northwest-1.amazonaws.com.cn",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			creator := &awsIdentityAttestationCreator{
				attestationService: test.attestationSvc,
			}
			attestation, _ := creator.createAttestation()

			if test.attestationReturned {
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
			} else {
				assertNilF(t, attestation)
			}
		})
	}
}

type mockAwsAttestationMetadataProvider struct {
	creds  aws.Credentials
	region string
}

var mockCreds = aws.Credentials{
	AccessKeyID:     "mockAccessKey",
	SecretAccessKey: "mockSecretKey",
	SessionToken:    "mockSessionToken",
}

func (m *mockAwsAttestationMetadataProvider) awsCredentials() aws.Credentials {
	return m.creds
}

func (m *mockAwsAttestationMetadataProvider) awsRegion() string {
	return m.region
}

func TestGcpIdentityAttestationCreator(t *testing.T) {
	tests := []struct {
		name                string
		wiremockMappingPath string
		expectedSub         string
	}{
		{
			name:                "Successful flow",
			wiremockMappingPath: "wif/gcp/successful_flow.json",
			expectedSub:         "some-subject",
		},
		{
			name:                "No GCP credential - http error",
			wiremockMappingPath: "wif/gcp/http_error.json",
		},
		{
			name:                "missing issuer claim",
			wiremockMappingPath: "wif/gcp/missing_issuer_claim.json",
		},
		{
			name:                "unparsable token",
			wiremockMappingPath: "wif/gcp/unparsable_token.json",
		},
	}

	creator := &gcpIdentityAttestationCreator{
		cfg:                    &Config{},
		metadataServiceBaseURL: wiremock.baseURL(),
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			wiremock.registerMappings(t, wiremockMapping{filePath: test.wiremockMappingPath})
			attestation, err := creator.createAttestation()

			if test.expectedSub != "" {
				assertNilE(t, err)
				assertNotNilE(t, attestation)
				assertEqualE(t, string(gcpWif), attestation.ProviderType)
				assertEqualE(t, test.expectedSub, attestation.Metadata["sub"])
			} else {
				assertNilE(t, err)
				assertNilF(t, attestation)
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
		missingIssuerClaimToken = "eyJ0eXAiOiJhdCtqd3QiLCJhbGciOiJFUzI1NiIsImtpZCI6ImU2M2I5NzA1OTRiY2NmZTAxMDlkOTg4OWM2MDk3OWEwIn0.eyJzdWIiOiJzb21lLXN1YmplY3QiLCJpYXQiOjE3NDM3NjEyMTMsImV4cCI6MTc0Mzc2NDgxMywiYXVkIjoid3d3LmV4YW1wbGUuY29tIn0.H6sN6kjA82EuijFcv-yCJTqau5qvVTCsk0ZQ4gvFQMkB7c71XPs4lkwTa7ZlNNlx9e6TpN1CVGnpCIRDDAZaDw"
		/*
		 * {
		 *   "iss": "https://accounts.google.com",
		 *   "iat": 1743761213,
		 *   "exp": 1743764813,
		 *   "aud": "www.example.com"
		 * }
		 */
		missingSubClaimToken = "eyJ0eXAiOiJhdCtqd3QiLCJhbGciOiJFUzI1NiIsImtpZCI6ImU2M2I5NzA1OTRiY2NmZTAxMDlkOTg4OWM2MDk3OWEwIn0.eyJpc3MiOiJodHRwczovL2FjY291bnRzLmdvb2dsZS5jb20iLCJpYXQiOjE3NDM3NjEyMTMsImV4cCI6MTc0Mzc2NDgxMywiYXVkIjoid3d3LmV4YW1wbGUuY29tIn0.w0njdpfWFETVK8Ktq9GdvuKRQJjvhOplcSyvQ_zHHwBUSMapqO1bjEWBx5VhGkdECZIGS1VY7db_IOqT45yOMA"
		/*
		 * {
		 *     "iss": "https://oidc.eks.us-east-2.amazonaws.com/id/3B869BC5D12CEB5515358621D8085D58",
		 *     "iat": 1743692017,
		 *     "exp": 1775228014,
		 *     "aud": "www.example.com",
		 *     "sub": "system:serviceaccount:poc-namespace:oidc-sa"
		 * }
		 */
		validToken      = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL29pZGMuZWtzLnVzLWVhc3QtMi5hbWF6b25hd3MuY29tL2lkLzNCODY5QkM1RDEyQ0VCNTUxNTM1ODYyMUQ4MDg1RDU4IiwiaWF0IjoxNzQ0Mjg3ODc4LCJleHAiOjE3NzU4MjM4NzgsImF1ZCI6Ind3dy5leGFtcGxlLmNvbSIsInN1YiI6InN5c3RlbTpzZXJ2aWNlYWNjb3VudDpwb2MtbmFtZXNwYWNlOm9pZGMtc2EifQ.a8H6KRIF1XmM8lkqL6kR8ccInr7wAzQrbKd3ZHFgiEg"
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
			expectedError: nil,
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
			} else if test.expectedSub == "" && test.expectedError == nil {
				assertNilE(t, err)
				assertNilF(t, attestation)
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
	}{
		/*
		 * {
		 *     "iss": "https://sts.windows.net/fa15d692-e9c7-4460-a743-29f29522229/",
		 *     "sub": "77213E30-E8CB-4595-B1B6-5F050E8308FD"
		 * }
		 */
		{
			name:                "Successful flow",
			wiremockMappingPath: "wif/azure/successful_flow_basic.json",
			metadataProvider:    azureVMMetadataProvider(),
			expectedIss:         "https://sts.windows.net/fa15d692-e9c7-4460-a743-29f29522229/",
		},
		/*
		 * {
		 *     "iss": "https://login.microsoftonline.com/fa15d692-e9c7-4460-a743-29f29522229/",
		 *     "sub": "77213E30-E8CB-4595-B1B6-5F050E8308FD"
		 * }
		 */
		{
			name:                "Successful flow v2 issuer",
			wiremockMappingPath: "wif/azure/successful_flow_v2_issuer.json",
			metadataProvider:    azureVMMetadataProvider(),
			expectedIss:         "https://login.microsoftonline.com/fa15d692-e9c7-4460-a743-29f29522229/",
		},
		/*
		 * {
		 *     "iss": "https://sts.windows.net/fa15d692-e9c7-4460-a743-29f29522229/",
		 *     "sub": "77213E30-E8CB-4595-B1B6-5F050E8308FD"
		 * }
		 */
		{
			name:                "Successful flow azure functions",
			wiremockMappingPath: "wif/azure/successful_flow_azure_functions.json",
			metadataProvider:    azureFunctionsMetadataProvider(),
			expectedIss:         "https://sts.windows.net/fa15d692-e9c7-4460-a743-29f29522229/",
		},
		/*
		 * {
		 *     "iss": "https://login.microsoftonline.com/fa15d692-e9c7-4460-a743-29f29522229/",
		 *     "sub": "77213E30-E8CB-4595-B1B6-5F050E8308FD"
		 * }
		 */
		{
			name:                "Successful flow azure functions v2 issuer",
			wiremockMappingPath: "wif/azure/successful_flow_azure_functions_v2_issuer.json",
			metadataProvider:    azureFunctionsMetadataProvider(),
			expectedIss:         "https://login.microsoftonline.com/fa15d692-e9c7-4460-a743-29f29522229/",
		},
		/*
		 * {
		 *     "iss": "https://sts.windows.net/fa15d692-e9c7-4460-a743-29f29522229/",
		 *     "sub": "77213E30-E8CB-4595-B1B6-5F050E8308FD"
		 * }
		 */
		{
			name:                "Successful flow azure functions no client ID",
			wiremockMappingPath: "wif/azure/successful_flow_azure_functions_no_client_id.json",
			metadataProvider: &mockAzureAttestationMetadataProvider{
				identityEndpointValue: wiremock.baseURL() + "/metadata/identity/endpoint/from/env",
				identityHeaderValue:   "some-identity-header-from-env",
				clientIDValue:         "",
			},
			expectedIss: "https://sts.windows.net/fa15d692-e9c7-4460-a743-29f29522229/",
		},
		/*
		 * {
		 *     "iss": "https://sts.windows.net/fa15d692-e9c7-4460-a743-29f29522229/",
		 *     "sub": "77213E30-E8CB-4595-B1B6-5F050E8308FD"
		 * }
		 */
		{
			name:                "Successful flow azure functions custom Entra resource",
			wiremockMappingPath: "wif/azure/successful_flow_azure_functions_custom_entra_resource.json",
			metadataProvider:    azureFunctionsMetadataProvider(),
			cfg:                 &Config{WorkloadIdentityEntraResource: "api://1111111-2222-3333-44444-55555555"},
			expectedIss:         "https://sts.windows.net/fa15d692-e9c7-4460-a743-29f29522229/",
		},
		{
			name:                "Non-json response",
			wiremockMappingPath: "wif/azure/non_json_response.json",
			metadataProvider:    azureVMMetadataProvider(),
		},
		{
			name: "Identity endpoint but no identity header",
			metadataProvider: &mockAzureAttestationMetadataProvider{
				identityEndpointValue: wiremock.baseURL() + "/metadata/identity/endpoint/from/env",
				identityHeaderValue:   "",
				clientIDValue:         "managed-client-id-from-env",
			},
		},
		{
			name:                "unparsable token",
			wiremockMappingPath: "wif/azure/unparsable_token.json",
			metadataProvider:    azureVMMetadataProvider(),
		},
		{
			name:                "HTTP error",
			metadataProvider:    azureVMMetadataProvider(),
			wiremockMappingPath: "wif/azure/http_error.json",
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

			if test.expectedIss != "" {
				assertNilE(t, err)
				assertNotNilE(t, attestation)
				assertEqualE(t, string(azureWif), attestation.ProviderType)
				assertEqualE(t, test.expectedIss, attestation.Metadata["iss"])
				assertEqualE(t, "77213E30-E8CB-4595-B1B6-5F050E8308FD", attestation.Metadata["sub"])
			} else {
				assertNilE(t, err)
				assertNilF(t, attestation)
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
