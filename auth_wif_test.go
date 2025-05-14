package gosnowflake

import (
	"context"
	"errors"
	"github.com/aws/aws-sdk-go-v2/aws"
	"testing"
)

var awsErrorCreator = &mockWifAttestationCreator{attestation: nil, err: errors.New("AWS error")}
var gcpErrorCreator = &mockWifAttestationCreator{attestation: nil, err: errors.New("GCP error")}
var azureErrorCreator = &mockWifAttestationCreator{attestation: nil, err: errors.New("AZURE error")}
var oidcErrorCreator = &mockWifAttestationCreator{attestation: nil, err: errors.New("OIDC error")}

type mockWifAttestationCreator struct {
	attestation *WifAttestation
	err         error
}

func (m *mockWifAttestationCreator) CreateAttestation(ctx context.Context) (*WifAttestation, error) {
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
				attestation: &WifAttestation{ProviderType: "OIDC", Credential: "oidc-credential"},
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
				attestation: &WifAttestation{ProviderType: "AWS", Credential: "aws-credential"},
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
				attestation: &WifAttestation{ProviderType: "GCP", Credential: "gcp-credential"},
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
				attestation: &WifAttestation{ProviderType: "AZURE", Credential: "azure-credential"},
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

func TestAwsIdentityAttestationCreator_CreateAttestation(t *testing.T) {
	tests := []struct {
		name                string
		attestationSvc      AwsAttestationService
		attestationReturned bool
		expectedProvider    string
		expectedMetadata    map[string]string
	}{
		{
			name: "No AWS credentials",
			attestationSvc: &mockAwsAttestationService{
				creds:  aws.Credentials{},
				region: "us-west-2",
				arn:    "arn:aws:iam::123456789012:role/test-role",
			},
			attestationReturned: false,
		},
		{
			name: "No AWS region",
			attestationSvc: &mockAwsAttestationService{
				creds:  mockCreds,
				region: "",
				arn:    "arn:aws:iam::123456789012:role/test-role",
			},
			attestationReturned: false,
		},
		{
			name: "No AWS ARN",
			attestationSvc: &mockAwsAttestationService{
				creds:  mockCreds,
				region: "us-west-2",
				arn:    "",
			},
			attestationReturned: false,
		},
		{
			name: "Successful attestation",
			attestationSvc: &mockAwsAttestationService{
				creds:  mockCreds,
				region: "us-west-2",
				arn:    "arn:aws:iam::123456789012:role/test-role",
			},
			attestationReturned: true,
			expectedProvider:    "AWS",
			expectedMetadata:    map[string]string{"arn": "arn:aws:iam::123456789012:role/test-role"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			creator := &AwsIdentityAttestationCreator{
				attestationService: test.attestationSvc,
			}

			ctx := context.Background()
			attestation, _ := creator.CreateAttestation(ctx)

			if test.attestationReturned {
				assertNotNilE(t, attestation)
				assertNotNilE(t, attestation.Credential)
				assertEqualE(t, test.expectedProvider, attestation.ProviderType)
				assertDeepEqualE(t, test.expectedMetadata, attestation.Metadata)
			} else {
				assertNilF(t, attestation)
			}
		})
	}
}

type mockAwsAttestationService struct {
	creds  aws.Credentials
	region string
	arn    string
}

var mockCreds = aws.Credentials{
	AccessKeyID:     "mockAccessKey",
	SecretAccessKey: "mockSecretKey",
	SessionToken:    "mockSessionToken",
}

func (m *mockAwsAttestationService) GetAWSCredentials() aws.Credentials {
	return m.creds
}

func (m *mockAwsAttestationService) GetAWSRegion() string {
	return m.region
}

func (m *mockAwsAttestationService) GetArn() string {
	return m.arn
}
