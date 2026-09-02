package config

import (
	"strings"
	"testing"

	sferrors "github.com/snowflakedb/gosnowflake/v2/internal/errors"
)

// validatableConfig returns a Config that satisfies every *other* requirement of
// FillMissingConfigParameters, so that the only thing a test can trip over is the
// account/region validation added by this change.
func validatableConfig(account, region, host string) *Config {
	return &Config{
		Account:  account,
		Region:   region,
		Host:     host,
		User:     "testuser",
		Password: "testpassword",
		Params:   make(map[string]*string),
	}
}

func asSnowflakeError(t *testing.T, err error) *sferrors.SnowflakeError {
	t.Helper()
	if err == nil {
		t.Fatalf("expected an error, got nil")
	}
	sfErr, ok := err.(*sferrors.SnowflakeError)
	if !ok {
		t.Fatalf("expected *errors.SnowflakeError, got %T: %v", err, err)
	}
	return sfErr
}

// TestAccountIdentifierAcceptsRealForms pins the compatibility contract: every
// account identifier form Snowflake actually issues must keep deriving a host.
func TestAccountIdentifierAcceptsRealForms(t *testing.T) {
	for _, tc := range []struct {
		name     string
		account  string
		region   string
		host     string
		wantHost string
	}{
		{name: "bare_locator", account: "xy12345", wantHost: "xy12345.snowflakecomputing.com"},
		{name: "org_account_hyphen", account: "myorg-myaccount", wantHost: "myorg-myaccount.snowflakecomputing.com"},
		{name: "underscores", account: "my_acct", wantHost: "my_acct.snowflakecomputing.com"},
		{name: "uppercase", account: "MyAcct", wantHost: "MyAcct.snowflakecomputing.com"},
		{name: "consecutive_hyphens", account: "a--b", wantHost: "a--b.snowflakecomputing.com"},
		{name: "dotted_region", account: "acct.us-east-1", wantHost: "acct.us-east-1.snowflakecomputing.com"},
		{name: "three_labels", account: "a.b.c", wantHost: "a.b.c.snowflakecomputing.com"},
		{name: "dotted_global", account: "myorg-myacct.global", wantHost: "myorg-myacct.global.snowflakecomputing.com"},
		{name: "cn_dotted_region", account: "acct.cn-north-1", wantHost: "acct.cn-north-1.snowflakecomputing.cn"},
		{name: "explicit_region", account: "acct", region: "us-east-1", wantHost: "acct.us-east-1.snowflakecomputing.com"},
		{name: "explicit_dotted_region", account: "acct", region: "us-east-1.aws", wantHost: "acct.us-east-1.aws.snowflakecomputing.com"},
		{name: "explicit_cn_region", account: "acct", region: "cn-north-1", wantHost: "acct.cn-north-1.snowflakecomputing.cn"},
		{
			// The ".global." org-id strip in FillMissingConfigParameters rewrites
			// Account *after* validation runs; validation sees the pre-strip
			// "myorg-myacct", which is a legal identifier, and the strip still
			// happens.
			name:     "global_host_strips_org_id",
			account:  "myorg-myacct",
			host:     "myorg-myacct.global.snowflakecomputing.com",
			wantHost: "myorg-myacct.global.snowflakecomputing.com",
		},
		{
			name:     "account_given_as_full_hostname",
			account:  "myacct.snowflakecomputing.com",
			wantHost: "myacct.snowflakecomputing.com.snowflakecomputing.com",
		},
		{
			// Surrounding spaces were tolerated before this change (the empty
			// check trims them) and must stay tolerated.
			name:     "spaces_around_account",
			account:  " acct ",
			wantHost: " acct .snowflakecomputing.com",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validatableConfig(tc.account, tc.region, tc.host)
			assertNilF(t, FillMissingConfigParameters(cfg), "FillMissingConfigParameters(account="+tc.account+", region="+tc.region+")")
			assertEqualE(t, cfg.Host, tc.wantHost)
		})
	}
}

// TestAccountIdentifierRejectsInvalidCharacters covers characters and label
// shapes the identifier rule does not allow. cfg.Host staying empty shows the
// check ran before FillMissingConfigParameters derived a host.
func TestAccountIdentifierRejectsInvalidCharacters(t *testing.T) {
	for _, tc := range []struct {
		name      string
		account   string
		region    string
		wantField string
	}{
		{name: "slash", account: "acct/", wantField: "account"},
		{name: "slash_with_tail", account: "acct/other.example.com", wantField: "account"},
		{name: "question_mark", account: "acct?x", wantField: "account"},
		{name: "hash", account: "acct#x", wantField: "account"},
		{name: "at_sign", account: "acct@x", wantField: "account"},
		{name: "colon_port", account: "acct:8080", wantField: "account"},
		{name: "empty_label", account: "acct..b", wantField: "account"},
		{name: "leading_dot", account: ".acct", wantField: "account"},
		{name: "trailing_dot", account: "acct.", wantField: "account"},
		{name: "bare_percent", account: "%", wantField: "account"},
		{name: "percent_encoded_question", account: "acct%3fx", wantField: "account"},
		{name: "newline", account: "acct\n", wantField: "account"},
		{name: "space_inside", account: "ac ct", wantField: "account"},
		{name: "non_ascii_cyrillic_a", account: "аcct", wantField: "account"},
		// extractRegionFromAccount moves everything after the first dot into
		// Region, so both the full account string and Region must be checked.
		{name: "dot_tail_path", account: "acct.other.example.com/x", wantField: "account"},
		{name: "dot_tail_percent_question", account: "acct.host%3fx", wantField: "account"},
		// DSN() performs that split itself and then validates Region.
		{name: "region_path", account: "acct", region: "other.example.com/x", wantField: "region"},
		{name: "region_question", account: "acct", region: "other.example.com?x", wantField: "region"},
		{name: "region_at_sign", account: "acct", region: "other.example.com@x", wantField: "region"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validatableConfig(tc.account, tc.region, "")
			err := FillMissingConfigParameters(cfg)
			sfErr := asSnowflakeError(t, err)
			assertEqualE(t, sfErr.Number, sferrors.ErrCodeInvalidAccountIdentifier)
			assertTrueE(t, strings.Contains(sfErr.Error(), tc.wantField), "error should name the offending field: "+sfErr.Error())
			// Negative control: no host may have been derived before the error.
			assertEqualE(t, cfg.Host, "", "a host was derived from an invalid identifier")
		})
	}
}

// TestAccountValidationCoversEveryEntryPoint checks the public entry points that
// assign cfg.Host from account-derived values, including DSN(), which performs
// the first-dot split itself before deriving the host and so can only be covered
// by validating cfg.Region.
func TestAccountValidationCoversEveryEntryPoint(t *testing.T) {
	t.Run("DSN_dotted_account_invalid_tail", func(t *testing.T) {
		cfg := validatableConfig("acct.other.example.com/x", "", "")
		dsn, err := DSN(cfg)
		sfErr := asSnowflakeError(t, err)
		assertEqualE(t, sfErr.Number, sferrors.ErrCodeInvalidAccountIdentifier)
		assertEqualE(t, dsn, "")
		// DSN() splits at the first dot before FillMissingConfigParameters, so
		// the rejected value is reported as region.
		assertTrueE(t, strings.Contains(sfErr.Error(), "region"), sfErr.Error())
	})

	t.Run("DSN_invalid_account", func(t *testing.T) {
		cfg := validatableConfig("acct?x", "", "")
		dsn, err := DSN(cfg)
		assertEqualE(t, asSnowflakeError(t, err).Number, sferrors.ErrCodeInvalidAccountIdentifier)
		assertEqualE(t, dsn, "")
	})

	t.Run("ParseDSN_account_parameter", func(t *testing.T) {
		// parseDSNParams URL-decodes parameter values, so %3f arrives as '?'.
		cfg, err := ParseDSN("u:p@/db?account=acct%3Fx")
		assertEqualE(t, asSnowflakeError(t, err).Number, sferrors.ErrCodeInvalidAccountIdentifier)
		assertTrueE(t, cfg == nil, "ParseDSN must not return a Config alongside the error")
	})

	t.Run("ParseDSN_region_parameter", func(t *testing.T) {
		cfg, err := ParseDSN("u:p@/db?account=acct&region=other.example.com%2Fx")
		assertEqualE(t, asSnowflakeError(t, err).Number, sferrors.ErrCodeInvalidAccountIdentifier)
		assertTrueE(t, cfg == nil, "ParseDSN must not return a Config alongside the error")
	})

	t.Run("ParseDSN_account_host_form", func(t *testing.T) {
		// No Snowflake TLD and no port, so transformAccountToHost treats the
		// host as Account+Region and the tail after the first dot is Region.
		cfg, err := ParseDSN("u:p@acct.other%3Fx/db")
		assertEqualE(t, asSnowflakeError(t, err).Number, sferrors.ErrCodeInvalidAccountIdentifier)
		assertTrueE(t, cfg == nil, "ParseDSN must not return a Config alongside the error")
	})
}

// TestHostDerivedAccountIsNotValidated pins the scoping rule: only user-supplied
// accounts are validated. applyAccountFromHostIfMissing synthesizes Account from
// the first DNS label of an operator-supplied Host, and that synthesized value
// must never be rejected — the operator already chose the endpoint, and failing
// here would break connections that work today.
func TestHostDerivedAccountIsNotValidated(t *testing.T) {
	cfg := validatableConfig("", "", "acct~test.snowflakecomputing.com")
	assertNilF(t, FillMissingConfigParameters(cfg))
	assertEqualE(t, cfg.Account, "acct~test")
	assertEqualE(t, cfg.Host, "acct~test.snowflakecomputing.com")
}

// TestEmptyAccountKeepsItsOwnErrorCode guards requirement that this change does
// not introduce a second, earlier empty-account error: callers switching on
// error numbers must still see 260000.
func TestEmptyAccountKeepsItsOwnErrorCode(t *testing.T) {
	for _, tc := range []struct {
		name    string
		account string
	}{
		{name: "empty", account: ""},
		{name: "spaces_only", account: "   "},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validatableConfig(tc.account, "", "")
			err := FillMissingConfigParameters(cfg)
			assertEqualE(t, asSnowflakeError(t, err).Number, sferrors.ErrCodeEmptyAccountCode)
		})
	}

	// The entirely empty Config pinned by connection_identifier_shape_test.go.
	err := FillMissingConfigParameters(&Config{})
	assertEqualE(t, asSnowflakeError(t, err).Number, sferrors.ErrCodeEmptyAccountCode)
}

// TestPinnedDSNFormsStillParse re-runs the DSN forms that
// connection_identifier_shape_test.go depends on, so a regression in this
// validation surfaces here as well as there.
func TestPinnedDSNFormsStillParse(t *testing.T) {
	for _, dsn := range []string{
		"u:p@/db?account=myacct",
		"u:p@localhost:8080/db?account=myacct",
		"u:p@myacct.us-east-1.aws.snowflakecomputing.com:443/db",
		"u:p@/db?account=myacct&region=us-east-1",
	} {
		t.Run(dsn, func(t *testing.T) {
			cfg, err := ParseDSN(dsn)
			assertNilF(t, err, "ParseDSN("+dsn+")")
			assertTrueE(t, cfg != nil, "ParseDSN returned a nil Config")
		})
	}
}

// TestIsValidAccountIdentifier exercises the rule directly, including the label
// cases that only matter as inputs to the loop.
func TestIsValidAccountIdentifier(t *testing.T) {
	for _, valid := range []string{"xy12345", "myorg-myaccount", "my_acct", "acct.us-east-1", "a.b.c", "a--b", "A", "0", "_", "-"} {
		assertTrueE(t, isValidAccountIdentifier(valid), "expected valid: "+valid)
	}
	for _, invalid := range []string{"", ".", "..", "a.", ".a", "a..b", "a/b", "a?b", "a#b", "a@b", "a:1", "a b", "a%2fb", "a\tb", "a\nb", "аb"} {
		assertFalseE(t, isValidAccountIdentifier(invalid), "expected invalid: "+invalid)
	}
}
