package config

import (
	"testing"
)

// TestConnectionIdentifierShapeCapture exercises the user-vs-inferred
// provenance recorded on Config's unexported inputShape field across the
// three entry points (DSN, programmatic Config, TOML). The actual telemetry
// emission lives in the gosnowflake package and is exercised separately.
func TestConnectionIdentifierShapeCapture(t *testing.T) {
	t.Run("ParseDSN", testShapeFromParseDSN)
	t.Run("ProgrammaticConfig", testShapeFromProgrammaticConfig)
	t.Run("ParseToml", testShapeFromToml)
	t.Run("Idempotent", testInferIsIdempotent)
}

func testShapeFromParseDSN(t *testing.T) {
	cases := []struct {
		name string
		dsn  string
		want ConnectionIdentifierShape
	}{
		{
			name: "account_only_locator",
			// Authority is a bare account locator. Region/host are inferred.
			dsn: "u:p@myacct/db",
			want: ConnectionIdentifierShape{
				AccountProvided: true,
			},
		},
		{
			name: "account_with_region_via_authority",
			// "myacct.us-east-1" is account-form; dot signals region embedded.
			dsn: "u:p@myacct.us-east-1/db",
			want: ConnectionIdentifierShape{
				AccountProvided:   true,
				AccountWithRegion: true,
			},
		},
		{
			name: "org_account_via_authority_global",
			// Dash signals org-prefixed account; ".global" host triggers
			// the org-id strip in FillMissingConfigParameters.
			dsn: "u:p@myorg-myacct.global/db",
			want: ConnectionIdentifierShape{
				AccountProvided:    true,
				AccountWithRegion:  true, // also has '.' before normalization
				AccountOrgProvided: true,
			},
		},
		{
			name: "host_explicit_in_authority",
			// Full hostname; account inferred from first DNS label later.
			dsn: "u:p@myacct.us-east-1.aws.snowflakecomputing.com:443/db",
			want: ConnectionIdentifierShape{
				HostProvided: true,
			},
		},
		{
			name: "account_query_param",
			// Bare host less form; account supplied via ?account=.
			dsn: "u:p@/db?account=myacct",
			want: ConnectionIdentifierShape{
				AccountProvided: true,
			},
		},
		{
			name: "account_and_region_query_params",
			dsn:  "u:p@/db?account=myacct&region=us-east-1",
			want: ConnectionIdentifierShape{
				AccountProvided: true,
				RegionProvided:  true,
			},
		},
		{
			name: "host_authority_account_query_param",
			dsn:  "u:p@myacct.us-east-1.aws.snowflakecomputing.com:443/db?account=myacct",
			want: ConnectionIdentifierShape{
				AccountProvided: true,
				HostProvided:    true,
			},
		},
		{
			name: "non_snowflake_host_with_port",
			// Localhost / proxy / private-DNS targets do not carry the
			// Snowflake TLD, but an explicit port disables the account-form
			// rewrite in transformAccountToHost — so the user did supply a
			// host and must be recorded as such.
			dsn: "u:p@localhost:8080/db?account=myacct",
			want: ConnectionIdentifierShape{
				AccountProvided: true,
				HostProvided:    true,
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := ParseDSN(tc.dsn)
			assertNilF(t, err, "ParseDSN("+tc.dsn+")")
			assertNotNilF(t, cfg.inputShape, "ParseDSN("+tc.dsn+") left inputShape nil")
			assertEqualE(t, *cfg.inputShape, tc.want)
		})
	}
}

func testShapeFromProgrammaticConfig(t *testing.T) {
	cases := []struct {
		name string
		cfg  Config
		want ConnectionIdentifierShape
	}{
		{
			name: "bare_account",
			cfg:  Config{Account: "myacct"},
			want: ConnectionIdentifierShape{AccountProvided: true},
		},
		{
			name: "account_with_dot_and_dash",
			cfg:  Config{Account: "myorg-myacct.us-east-1"},
			want: ConnectionIdentifierShape{
				AccountProvided:    true,
				AccountWithRegion:  true,
				AccountOrgProvided: true,
			},
		},
		{
			name: "host_only",
			cfg:  Config{Host: "myacct.snowflakecomputing.com"},
			want: ConnectionIdentifierShape{HostProvided: true},
		},
		{
			name: "all_three",
			cfg: Config{
				Account: "myacct",
				Region:  "us-east-1",
				Host:    "myacct.us-east-1.aws.snowflakecomputing.com",
			},
			want: ConnectionIdentifierShape{
				AccountProvided: true,
				RegionProvided:  true,
				HostProvided:    true,
			},
		},
		{
			name: "empty",
			cfg:  Config{},
			want: ConnectionIdentifierShape{},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := tc.cfg
			inferInputShapeIfMissing(&cfg)
			assertNotNilF(t, cfg.inputShape, "inferInputShapeIfMissing left inputShape nil")
			assertEqualE(t, *cfg.inputShape, tc.want)
		})
	}
}

func testShapeFromToml(t *testing.T) {
	cases := []struct {
		name string
		toml map[string]any
		want ConnectionIdentifierShape
	}{
		{
			name: "account_and_region",
			toml: map[string]any{
				"account": "myacct",
				"region":  "us-east-1",
				"user":    "u",
			},
			want: ConnectionIdentifierShape{
				AccountProvided: true,
				RegionProvided:  true,
			},
		},
		{
			name: "host_only",
			toml: map[string]any{
				"host": "myacct.snowflakecomputing.com",
				"user": "u",
			},
			want: ConnectionIdentifierShape{HostProvided: true},
		},
		{
			name: "org_account_only",
			toml: map[string]any{
				"account": "myorg-myacct",
				"user":    "u",
			},
			want: ConnectionIdentifierShape{
				AccountProvided:    true,
				AccountOrgProvided: true,
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &Config{
				Params:     make(map[string]*string),
				inputShape: &ConnectionIdentifierShape{},
			}
			assertNilF(t, ParseToml(cfg, tc.toml), "ParseToml")
			assertNotNilF(t, cfg.inputShape, "ParseToml left inputShape nil")
			assertEqualE(t, *cfg.inputShape, tc.want)
		})
	}
}

func testInferIsIdempotent(t *testing.T) {
	preset := &ConnectionIdentifierShape{AccountProvided: true}
	cfg := Config{
		Account:    "myacct",
		Host:       "myacct.snowflakecomputing.com",
		inputShape: preset,
	}
	inferInputShapeIfMissing(&cfg)
	assertEqualF(t, cfg.inputShape, preset,
		"inferInputShapeIfMissing overwrote existing inputShape pointer")
	// HostProvided was *not* set on the preset and must remain false because
	// shape capture must not run twice on the same Config.
	assertFalseE(t, cfg.inputShape.HostProvided,
		"inferInputShapeIfMissing mutated an already-populated inputShape")
}
