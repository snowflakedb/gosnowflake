package config

import "testing"

func TestParseTomlWorkloadIdentityHost(t *testing.T) {
	// Keys are normalized by lowercasing and stripping underscores, so both the
	// snake_case and the camelCase spelling have to land in the same field.
	for _, key := range []string{"workload_identity_host", "workloadIdentityHost"} {
		t.Run(key, func(t *testing.T) {
			cfg := &Config{}
			assertNilF(t, ParseToml(cfg, map[string]any{key: "sts.custom.com"}))
			assertEqualE(t, cfg.WorkloadIdentityHost, "sts.custom.com")
		})
	}
}

func TestValidateWorkloadIdentityHost(t *testing.T) {
	t.Run("accepted for AWS", func(t *testing.T) {
		cfg := &Config{WorkloadIdentityProvider: "AWS", WorkloadIdentityHost: "sts.custom.com"}
		assertNilE(t, cfg.Validate())
	})

	t.Run("rejected for an explicitly non-AWS provider", func(t *testing.T) {
		for _, provider := range []string{"GCP", "AZURE", "OIDC"} {
			cfg := &Config{WorkloadIdentityProvider: provider, WorkloadIdentityHost: "sts.custom.com"}
			err := cfg.Validate()
			assertNotNilF(t, err, "provider "+provider)
			assertEqualE(t, err.Error(), "WorkloadIdentityHost is supported only for AWS")
		}
	})

	t.Run("unset is fine for every provider", func(t *testing.T) {
		for _, provider := range []string{"AWS", "GCP", "AZURE", "OIDC", ""} {
			cfg := &Config{WorkloadIdentityProvider: provider}
			assertNilE(t, cfg.Validate(), "provider "+provider)
		}
	})
}
