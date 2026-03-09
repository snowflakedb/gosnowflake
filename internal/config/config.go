// Package config provides the Config struct which contains all configuration parameters for the driver and a Validate method to check if the configuration is correct.
package config

import (
	"crypto/rsa"
	"errors"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// Config is a set of configuration parameters
type Config struct {
	Account   string // Account name
	User      string // Username
	Password  string // Password (requires User)
	Database  string // Database name
	Schema    string // Schema
	Warehouse string // Warehouse
	Role      string // Role
	Region    string // Region

	OauthClientID                string // Client id for OAuth2 external IdP
	OauthClientSecret            string // Client secret for OAuth2 external IdP
	OauthAuthorizationURL        string // Authorization URL of Auth2 external IdP
	OauthTokenRequestURL         string // Token request URL of Auth2 external IdP
	OauthRedirectURI             string // Redirect URI registered in IdP. The default is http://127.0.0.1:<random port>
	OauthScope                   string // Comma separated list of scopes. If empty it is derived from role.
	EnableSingleUseRefreshTokens bool   // Enables single use refresh tokens for Snowflake IdP

	// ValidateDefaultParameters disable the validation checks for Database, Schema, Warehouse and Role
	// at the time a connection is established
	ValidateDefaultParameters Bool

	Params map[string]*string // other connection parameters

	Protocol string // http or https (optional)
	Host     string // hostname (optional)
	Port     int    // port (optional)

	Authenticator              AuthType // The authenticator type
	SingleAuthenticationPrompt Bool     // If enabled prompting for authentication will only occur for the first authentication challenge

	Passcode           string
	PasscodeInPassword bool

	OktaURL *url.URL

	// Deprecated: timeouts may be reorganized in a future release.
	LoginTimeout time.Duration // Login retry timeout EXCLUDING network roundtrip and read out http response
	// Deprecated: timeouts may be reorganized in a future release.
	RequestTimeout time.Duration // request retry timeout EXCLUDING network roundtrip and read out http response
	// Deprecated: timeouts may be reorganized in a future release.
	JWTExpireTimeout time.Duration // JWT expire after timeout
	// Deprecated: timeouts may be reorganized in a future release.
	ClientTimeout time.Duration // Timeout for network round trip + read out http response
	// Deprecated: timeouts may be reorganized in a future release.
	JWTClientTimeout time.Duration // Timeout for network round trip + read out http response used when JWT token auth is taking place
	// Deprecated: timeouts may be reorganized in a future release.
	ExternalBrowserTimeout time.Duration // Timeout for external browser login
	// Deprecated: timeouts may be reorganized in a future release.
	CloudStorageTimeout time.Duration // Timeout for a single call to a cloud storage provider
	MaxRetryCount       int           // Specifies how many times non-periodic HTTP request can be retried

	Application       string           // application name.
	DisableOCSPChecks bool             // driver doesn't check certificate revocation status
	OCSPFailOpen      OCSPFailOpenMode // OCSP Fail Open

	Token                  string        // Token to use for OAuth other forms of token based auth
	TokenFilePath          string        // TokenFilePath defines a file where to read token from
	TokenAccessor          TokenAccessor // TokenAccessor Optional token accessor to use
	ServerSessionKeepAlive bool          // ServerSessionKeepAlive enables the session to persist even after the driver connection is closed

	PrivateKey *rsa.PrivateKey // Private key used to sign JWT

	Transporter http.RoundTripper // RoundTripper to intercept HTTP requests and responses

	TLSConfigName string // Name of the TLS config to use

	// Deprecated: may be removed in a future release with logging reorganization.
	Tracing            string // sets logging level
	LogQueryText       bool   // indicates whether query text should be logged.
	LogQueryParameters bool   // indicates whether query parameters should be logged.

	TmpDirPath string // sets temporary directory used by a driver for operations like encrypting, compressing etc

	ClientRequestMfaToken          Bool // When true the MFA token is cached in the credential manager. True by default in Windows/OSX. False for Linux.
	ClientStoreTemporaryCredential Bool // When true the ID token is cached in the credential manager. True by default in Windows/OSX. False for Linux.

	DisableQueryContextCache bool // Should HTAP query context cache be disabled

	IncludeRetryReason Bool // Should retried request contain retry reason

	ClientConfigFile string // File path to the client configuration json file

	DisableConsoleLogin Bool // Indicates whether console login should be disabled

	DisableSamlURLCheck Bool // Indicates whether the SAML URL check should be disabled

	WorkloadIdentityProvider          string   // The workload identity provider to use for WIF authentication
	WorkloadIdentityEntraResource     string   // The resource to use for WIF authentication on Azure environment
	WorkloadIdentityImpersonationPath []string // The components to use for WIF impersonation.

	CertRevocationCheckMode           CertRevocationCheckMode // revocation check mode for CRLs
	CrlAllowCertificatesWithoutCrlURL Bool                    // Allow certificates (not short-lived) without CRL DP included to be treated as correct ones
	CrlInMemoryCacheDisabled          bool                    // Should the in-memory cache be disabled
	CrlOnDiskCacheDisabled            bool                    // Should the on-disk cache be disabled
	CrlDownloadMaxSize                int                     // Max size in bytes of CRL to download. 0 means use default (20MB).
	CrlHTTPClientTimeout              time.Duration           // Timeout for HTTP client used to download CRL

	ConnectionDiagnosticsEnabled       bool   // Indicates whether connection diagnostics should be enabled
	ConnectionDiagnosticsAllowlistFile string // File path to the allowlist file for connection diagnostics. If not specified, the allowlist.json file in the current directory will be used.

	ProxyHost     string // Proxy host
	ProxyPort     int    // Proxy port
	ProxyUser     string // Proxy user
	ProxyPassword string // Proxy password
	ProxyProtocol string // Proxy protocol (http or https)
	NoProxy       string // No proxy for this host list
}

var errTokenConfigConflict = errors.New("token and tokenFilePath cannot be specified at the same time")

// Validate enables testing if config is correct.
// A driver client may call it manually, but it is also called during opening first connection.
func (c *Config) Validate() error {
	if c.TmpDirPath != "" {
		if _, err := os.Stat(c.TmpDirPath); err != nil {
			return err
		}
	}
	if strings.EqualFold(c.WorkloadIdentityProvider, "azure") && len(c.WorkloadIdentityImpersonationPath) > 0 {
		return errors.New("WorkloadIdentityImpersonationPath is not supported for Azure")
	}
	if c.Token != "" && c.TokenFilePath != "" {
		return errTokenConfigConflict
	}
	return nil
}

// Param binds Config field names to environment variable names.
type Param struct {
	Name          string
	EnvName       string
	FailOnMissing bool
}
