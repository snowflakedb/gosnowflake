package gosnowflake

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

// withoutOCSPCacheServerEnv makes a test independent of SF_OCSP_RESPONSE_CACHE_SERVER_URL.
//
// It is needed because other tests in this package set that variable without
// restoring it — TestConnectionToMultipleConfigurations (driver_ocsp_test.go) calls
// the bare setenv helper, which has no cleanup — and `go test` runs a package's test
// files in filename order. "ocsp_url_test.go" sorts after "driver_ocsp_test.go", so
// without this the leaked value makes newOcspValidator take the env-override branch
// (ocsp.go) and every host-derived assertion below fails in a full-package run while
// passing under a -run filter. t.Setenv is not usable here: it can only set a value,
// and os.LookupEnv would still report the variable as present.
func withoutOCSPCacheServerEnv(t *testing.T) {
	t.Helper()
	prev, had := os.LookupEnv(cacheServerURLEnv)
	if err := os.Unsetenv(cacheServerURLEnv); err != nil {
		t.Fatalf("cannot unset %v: %v", cacheServerURLEnv, err)
	}
	t.Cleanup(func() {
		if had {
			if err := os.Setenv(cacheServerURLEnv, prev); err != nil {
				t.Errorf("cannot restore %v: %v", cacheServerURLEnv, err)
			}
			return
		}
		if err := os.Unsetenv(cacheServerURLEnv); err != nil {
			t.Errorf("cannot unset %v: %v", cacheServerURLEnv, err)
		}
	})
}

// TestOCSPCacheServerURLPerBranch asserts the OCSP address setup for each branch
// of newOcspValidator separately. A happy-path-only test would still pass if the
// PrivateLink or the non-global (.cn) branch were dropped or collapsed into the
// default one.
func TestOCSPCacheServerURLPerBranch(t *testing.T) {
	withoutOCSPCacheServerEnv(t)
	for _, tc := range []struct {
		name         string
		host         string
		wantCacheURL string
		wantRetryURL string
	}{
		{
			name:         "privatelink_branch",
			host:         "testaccount.us-east-1.privatelink.snowflakecomputing.com",
			wantCacheURL: "http://ocsp.testaccount.us-east-1.privatelink.snowflakecomputing.com/ocsp_response_cache.json",
			wantRetryURL: "http://ocsp.testaccount.us-east-1.privatelink.snowflakecomputing.com/retry/%v/%v",
		},
		{
			name:         "privatelink_cn_branch",
			host:         "testaccount.cn-region.privatelink.snowflakecomputing.cn",
			wantCacheURL: "http://ocsp.testaccount.cn-region.privatelink.snowflakecomputing.cn/ocsp_response_cache.json",
			wantRetryURL: "http://ocsp.testaccount.cn-region.privatelink.snowflakecomputing.cn/retry/%v/%v",
		},
		{
			name: "non_default_domain_branch",
			host: "testaccount.cn-region.snowflakecomputing.cn",
			// Not a PrivateLink environment: cache server is host-derived, but
			// no retry proxy is set up.
			wantCacheURL: "http://ocsp.testaccount.cn-region.snowflakecomputing.cn/ocsp_response_cache.json",
			wantRetryURL: "",
		},
		{
			name:         "default_domain_branch",
			host:         "testaccount.us-east-1.snowflakecomputing.com",
			wantCacheURL: fmt.Sprintf("%v/%v", defaultCacheServerHost, cacheFileBaseName),
			wantRetryURL: "",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ov := newOcspValidator(&Config{Host: tc.host})
			assertEqualE(t, ov.cacheServerURL, tc.wantCacheURL)
			assertEqualE(t, ov.retryURL, tc.wantRetryURL)
		})
	}
}

// TestOCSPCacheServerURLEnvOverrideWins pins the SF_OCSP_RESPONSE_CACHE_SERVER_URL
// override: it takes precedence over every host-derived branch and, as before,
// leaves the retry proxy unset even on a PrivateLink host.
func TestOCSPCacheServerURLEnvOverrideWins(t *testing.T) {
	t.Setenv(cacheServerURLEnv, "http://ocsp-cache.example.com/cache.json")
	ov := newOcspValidator(&Config{
		Host: "testaccount.us-east-1.privatelink.snowflakecomputing.com",
	})
	assertEqualE(t, ov.cacheServerURL, "http://ocsp-cache.example.com/cache.json")
	assertEqualE(t, ov.retryURL, "")
	assertEqualE(t, ov.isPrivateLink, true)
}

// TestOCSPRetryURLKeepsItsPlaceholders is the guard for the retry *template*: the
// value stored in ocspValidator.retryURL is later fed to fmt.Sprintf with two
// arguments (see retryOCSP), so it must still carry exactly two "%v" verbs. URL
// escaping the template — for example by putting the placeholders in url.URL.Path —
// would rewrite '%' as "%25" and break OCSP retry silently.
func TestOCSPRetryURLKeepsItsPlaceholders(t *testing.T) {
	withoutOCSPCacheServerEnv(t)
	ov := newOcspValidator(&Config{
		Host: "testaccount.us-east-1.privatelink.snowflakecomputing.com",
	})
	assertEqualE(t, strings.Count(ov.retryURL, "%v"), 2)
	// A host made of ordinary DNS characters needs no escaping, so the template
	// carries no percent signs other than its two verbs.
	assertEqualE(t, strings.Count(ov.retryURL, "%"), 2)

	filled := fmt.Sprintf(ov.retryURL, "ocsp.responder.example.com/path", "QUJDRA==")
	assertTrueE(t, !strings.Contains(filled, "%v"), "unfilled placeholder left in "+filled)
	assertTrueE(t, !strings.Contains(filled, "%!"), "fmt error verb in "+filled)
	assertStringContainsE(t, filled, "ocsp.responder.example.com/path")
	assertStringContainsE(t, filled, "QUJDRA==")
	assertTrueE(t, strings.HasPrefix(filled, "http://ocsp.testaccount.us-east-1.privatelink.snowflakecomputing.com/retry/"),
		"retry URL lost its proxy prefix: "+filled)
}

// TestOCSPURLsNonLdhHostFallsToDefault verifies that a host whose account label
// contains a URL-significant character ('#', '?', '/', '%') is no longer classified
// as a PrivateLink host and therefore does not derive an OCSP cache URL from the
// raw host string. The LDH character allowlist in checkIsPrivateLink rejects these
// hosts, so they fall through to the global default cache server.
//
// The non-global (.cn) branch is not PrivateLink but still derives a host-specific
// URL: it bypasses the PrivateLink gate and therefore reaches ocspCacheServerURLForHost
// directly. That helper uses url.URL to percent-encode any non-LDH chars so they
// cannot redirect the request — the safety property of the url.URL approach still
// applies for that branch.
func TestOCSPURLsNonLdhHostFallsToDefault(t *testing.T) {
	withoutOCSPCacheServerEnv(t)
	defaultCacheURL := fmt.Sprintf("%v/%v", defaultCacheServerHost, cacheFileBaseName)
	for _, tc := range []struct {
		name         string
		host         string
		wantCacheURL string
		wantRetryURL string
	}{
		{
			// Non-LDH char '/' in the account label → LDH gate rejects →
			// ends with DefaultDomain (.com) → global default cache server.
			name:         "slash_privatelink_rejected_by_ldh_gate",
			host:         "acct/other.example.com.us-east-1.privatelink.snowflakecomputing.com",
			wantCacheURL: defaultCacheURL,
			wantRetryURL: "",
		},
		{
			name:         "question_mark_privatelink_rejected_by_ldh_gate",
			host:         "acct?x.us-east-1.privatelink.snowflakecomputing.com",
			wantCacheURL: defaultCacheURL,
			wantRetryURL: "",
		},
		{
			name:         "hash_privatelink_rejected_by_ldh_gate",
			host:         "acct#x.us-east-1.privatelink.snowflakecomputing.com",
			wantCacheURL: defaultCacheURL,
			wantRetryURL: "",
		},
		{
			name:         "literal_percent_privatelink_rejected_by_ldh_gate",
			host:         "acct%2fx.us-east-1.privatelink.snowflakecomputing.com",
			wantCacheURL: defaultCacheURL,
			wantRetryURL: "",
		},
		{
			// Non-global (.cn) branch: not PrivateLink, does not end with
			// DefaultDomain, so reaches ocspCacheServerURLForHost directly.
			// url.URL percent-encodes the '/' so the authority is not altered.
			name:         "slash_non_default_domain",
			host:         "acct/other.example.com.cn-region.snowflakecomputing.cn",
			wantCacheURL: "http://ocsp.acct%2fother.example.com.cn-region.snowflakecomputing.cn/ocsp_response_cache.json",
			wantRetryURL: "",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ov := newOcspValidator(&Config{Host: tc.host})
			assertEqualE(t, ov.cacheServerURL, tc.wantCacheURL)
			assertEqualE(t, ov.retryURL, tc.wantRetryURL)
			assertEqualE(t, strings.Count(ov.cacheServerURL, "/"), 3)
			assertTrueE(t, strings.HasSuffix(ov.cacheServerURL, "/"+cacheFileBaseName), ov.cacheServerURL)
		})
	}
}
