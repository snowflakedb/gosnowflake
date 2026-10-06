package config

import "testing"

func TestParseTomlWorkloadIdentityAzureClientID(t *testing.T) {
	for _, key := range []string{"workload_identity_azure_client_id", "workloadIdentityAzureClientId"} {
		t.Run(key, func(t *testing.T) {
			cfg := &Config{}
			assertNilF(t, ParseToml(cfg, map[string]any{key: "11111111-2222-3333-4444-555555555555"}))
			assertEqualE(t, cfg.WorkloadIdentityAzureClientID, "11111111-2222-3333-4444-555555555555")
		})
	}
}

func TestValidateWorkloadIdentityAzureClientID(t *testing.T) {
	t.Run("accepted for Azure", func(t *testing.T) {
		cfg := &Config{WorkloadIdentityProvider: "AZURE", WorkloadIdentityAzureClientID: "11111111-2222-3333-4444-555555555555"}
		assertNilE(t, cfg.Validate())
	})

	t.Run("rejected for an explicitly non-Azure provider", func(t *testing.T) {
		for _, provider := range []string{"AWS", "GCP", "OIDC"} {
			cfg := &Config{WorkloadIdentityProvider: provider, WorkloadIdentityAzureClientID: "11111111-2222-3333-4444-555555555555"}
			err := cfg.Validate()
			assertNotNilF(t, err, "provider "+provider)
			assertEqualE(t, err.Error(), "WorkloadIdentityAzureClientID is supported only for Azure")
		}
	})

	t.Run("unset is fine for every provider", func(t *testing.T) {
		for _, provider := range []string{"AWS", "GCP", "AZURE", "OIDC", ""} {
			cfg := &Config{WorkloadIdentityProvider: provider}
			assertNilE(t, cfg.Validate(), "provider "+provider)
		}
	})

	t.Run("set with an empty provider is unused", func(t *testing.T) {
		cfg := &Config{WorkloadIdentityAzureClientID: "11111111-2222-3333-4444-555555555555"}
		assertNilE(t, cfg.Validate())
	})
}
