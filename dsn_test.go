package gosnowflake

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	cr "crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/pem"
	"errors"
	"fmt"
	"net/url"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/smithy-go/rand"
)

type tcParseDSN struct {
	dsn      string
	config   *Config
	ocspMode string
	err      error
}

func TestParseDSN(t *testing.T) {
	testPrivKey, _ := rsa.GenerateKey(cr.Reader, 2048)
	privKeyPKCS8 := generatePKCS8StringSupress(testPrivKey)
	privKeyPKCS1 := generatePKCS1String(testPrivKey)
	testcases := []tcParseDSN{
		{
			dsn: "user:pass@ac-1-laksdnflaf.global/db/schema",
			config: &Config{
				Account: "ac-1", User: "user", Password: "pass", Region: "global",
				Protocol: "https", Host: "ac-1-laksdnflaf.global.snowflakecomputing.com", Port: 443,
				Database: "db", Schema: "schema",
				OCSPFailOpen:              OCSPFailOpenTrue,
				ValidateDefaultParameters: ConfigBoolTrue,
				ClientTimeout:             defaultClientTimeout,
				JWTClientTimeout:          defaultJWTClientTimeout,
				ExternalBrowserTimeout:    defaultExternalBrowserTimeout,
				CloudStorageTimeout:       defaultCloudStorageTimeout,
				IncludeRetryReason:        ConfigBoolTrue,
			},
			ocspMode: ocspModeFailOpen,
			err:      nil,
		},
		{
			dsn: "user:pass@ac-1-laksdnflaf.global/db/schema?disableTelemetry=true",
			config: &Config{
				Account: "ac-1", User: "user", Password: "pass", Region: "global",
				Protocol: "https", Host: "ac-1-laksdnflaf.global.snowflakecomputing.com", Port: 443,
				Database: "db", Schema: "schema",
				OCSPFailOpen:              OCSPFailOpenTrue,
				DisableTelemetry:          true,
				ValidateDefaultParameters: ConfigBoolTrue,
				ClientTimeout:             defaultClientTimeout,
				JWTClientTimeout:          defaultJWTClientTimeout,
				ExternalBrowserTimeout:    defaultExternalBrowserTimeout,
				CloudStorageTimeout:       defaultCloudStorageTimeout,
				IncludeRetryReason:        ConfigBoolTrue,
			},
			ocspMode: ocspModeFailOpen,
			err:      nil,
		},
		{
			dsn: "user:pass@ac-laksdnflaf.global/db/schema",
			config: &Config{
				Account: "ac", User: "user", Password: "pass", Region: "global",
				Protocol: "https", Host: "ac-laksdnflaf.global.snowflakecomputing.com", Port: 443,
				Database: "db", Schema: "schema",
				OCSPFailOpen:              OCSPFailOpenTrue,
				ValidateDefaultParameters: ConfigBoolTrue,
				ClientTimeout:             defaultClientTimeout,
				JWTClientTimeout:          defaultJWTClientTimeout,
				ExternalBrowserTimeout:    defaultExternalBrowserTimeout,
				CloudStorageTimeout:       defaultCloudStorageTimeout,
				IncludeRetryReason:        ConfigBoolTrue,
			},
			ocspMode: ocspModeFailOpen,
			err:      nil,
		},
		{
			dsn: "u:p@asnowflakecomputing.com/db/pa?account=a&protocol=https&role=r&timezone=UTC&aehouse=w",
			config: &Config{Account: "a", User: "u", Password: "p", Database: "db", Schema: "pa",
				Protocol: "https", Role: "r", Host: "asnowflakecomputing.com.snowflakecomputing.com", Port: 443, Region: "com",
				OCSPFailOpen:              OCSPFailOpenTrue,
				ValidateDefaultParameters: ConfigBoolTrue,
				ClientTimeout:             defaultClientTimeout,
				JWTClientTimeout:          defaultJWTClientTimeout,
				ExternalBrowserTimeout:    defaultExternalBrowserTimeout,
				CloudStorageTimeout:       defaultCloudStorageTimeout,
				IncludeRetryReason:        ConfigBoolTrue,
			},
			ocspMode: ocspModeFailOpen,
			err:      nil,
		},
		{
			dsn: "u:p@/db?account=ac",
			config: &Config{
				Account: "ac", User: "u", Password: "p", Database: "db",
				Protocol: "https", Host: "ac.snowflakecomputing.com", Port: 443,
				OCSPFailOpen:              OCSPFailOpenTrue,
				ValidateDefaultParameters: ConfigBoolTrue,
				ClientTimeout:             defaultClientTimeout,
				JWTClientTimeout:          defaultJWTClientTimeout,
				ExternalBrowserTimeout:    defaultExternalBrowserTimeout,
				CloudStorageTimeout:       defaultCloudStorageTimeout,
				IncludeRetryReason:        ConfigBoolTrue,
			},
			ocspMode: ocspModeFailOpen,
			err:      nil,
		},
		{
			dsn: "u:p@/db?account=ac&workloadIdentityEntraResource=https%3A%2F%2Fexample.com%2F.default&workloadIdentityProvider=azure",
			config: &Config{
				Account: "ac", User: "u", Password: "p", Database: "db",
				Protocol: "https", Host: "ac.snowflakecomputing.com", Port: 443,
				WorkloadIdentityProvider: "azure", WorkloadIdentityEntraResource: "https://example.com/.default",
				OCSPFailOpen:              OCSPFailOpenTrue,
				ValidateDefaultParameters: ConfigBoolTrue,
				ClientTimeout:             defaultClientTimeout,
				JWTClientTimeout:          defaultJWTClientTimeout,
				ExternalBrowserTimeout:    defaultExternalBrowserTimeout,
				CloudStorageTimeout:       defaultCloudStorageTimeout,
				IncludeRetryReason:        ConfigBoolTrue,
			},
			ocspMode: ocspModeFailOpen,
			err:      nil,
		},
		{
			dsn: "u:p@/db?account=ac&region=cn-region",
			config: &Config{
				Account: "ac", User: "u", Password: "p", Database: "db", Region: "cn-region",
				Protocol: "https", Host: "ac.cn-region.snowflakecomputing.cn", Port: 443,
				OCSPFailOpen:              OCSPFailOpenTrue,
				ValidateDefaultParameters: ConfigBoolTrue,
				ClientTimeout:             defaultClientTimeout,
				JWTClientTimeout:          defaultJWTClientTimeout,
				ExternalBrowserTimeout:    defaultExternalBrowserTimeout,
				CloudStorageTimeout:       defaultCloudStorageTimeout,
				IncludeRetryReason:        ConfigBoolTrue,
			},
			ocspMode: ocspModeFailOpen,
			err:      nil,
		},
		{
			dsn: "user:pass@account-hfdw89q748ew9gqf48w9qgf.global/db/s",
			config: &Config{
				Account: "account", User: "user", Password: "pass", Region: "global",
				Protocol: "https", Host: "account-hfdw89q748ew9gqf48w9qgf.global.snowflakecomputing.com", Port: 443,
				Database: "db", Schema: "s",
				ValidateDefaultParameters: ConfigBoolTrue,
				OCSPFailOpen:              OCSPFailOpenTrue,
				ClientTimeout:             defaultClientTimeout,
				JWTClientTimeout:          defaultJWTClientTimeout,
				ExternalBrowserTimeout:    defaultExternalBrowserTimeout,
				CloudStorageTimeout:       defaultCloudStorageTimeout,
				IncludeRetryReason:        ConfigBoolTrue,
			},
			ocspMode: ocspModeFailOpen,
			err:      nil,
		},
		{
			dsn: "user:pass@account-hfdw89q748ew9gqf48w9qgf/db/s",
			config: &Config{
				Account: "account-hfdw89q748ew9gqf48w9qgf", User: "user", Password: "pass", Region: "",
				Protocol: "https", Host: "account-hfdw89q748ew9gqf48w9qgf.snowflakecomputing.com", Port: 443,
				Database: "db", Schema: "s",
				ValidateDefaultParameters: ConfigBoolTrue,
				OCSPFailOpen:              OCSPFailOpenTrue,
				ClientTimeout:             defaultClientTimeout,
				JWTClientTimeout:          defaultJWTClientTimeout,
				ExternalBrowserTimeout:    defaultExternalBrowserTimeout,
				CloudStorageTimeout:       defaultCloudStorageTimeout,
				IncludeRetryReason:        ConfigBoolTrue,
			},
			ocspMode: ocspModeFailOpen,
			err:      nil,
		},
		{
			dsn: "user:pass@account",
			config: &Config{
				Account: "account", User: "user", Password: "pass", Region: "",
				Protocol: "https", Host: "account.snowflakecomputing.com", Port: 443,
				OCSPFailOpen:              OCSPFailOpenTrue,
				ValidateDefaultParameters: ConfigBoolTrue,
				ClientTimeout:             defaultClientTimeout,
				JWTClientTimeout:          defaultJWTClientTimeout,
				ExternalBrowserTimeout:    defaultExternalBrowserTimeout,
				CloudStorageTimeout:       defaultCloudStorageTimeout,
				IncludeRetryReason:        ConfigBoolTrue,
			},
			ocspMode: ocspModeFailOpen,
			err:      nil,
		},
		{
			dsn: "user:pass@account.cn-region",
			config: &Config{
				Account: "account", User: "user", Password: "pass", Region: "cn-region",
				Protocol: "https", Host: "account.cn-region.snowflakecomputing.cn", Port: 443,
				OCSPFailOpen:              OCSPFailOpenTrue,
				ValidateDefaultParameters: ConfigBoolTrue,
				ClientTimeout:             defaultClientTimeout,
				JWTClientTimeout:          defaultJWTClientTimeout,
				ExternalBrowserTimeout:    defaultExternalBrowserTimeout,
				CloudStorageTimeout:       defaultCloudStorageTimeout,
				IncludeRetryReason:        ConfigBoolTrue,
			},
			ocspMode: ocspModeFailOpen,
			err:      nil,
		},
		{
			dsn: "user:pass@account.eu-faraway",
			config: &Config{
				Account: "account", User: "user", Password: "pass", Region: "eu-faraway",
				Protocol: "https", Host: "account.eu-faraway.snowflakecomputing.com", Port: 443,
				OCSPFailOpen:              OCSPFailOpenTrue,
				ValidateDefaultParameters: ConfigBoolTrue,
				ClientTimeout:             defaultClientTimeout,
				JWTClientTimeout:          defaultJWTClientTimeout,
				ExternalBrowserTimeout:    defaultExternalBrowserTimeout,
				CloudStorageTimeout:       defaultCloudStorageTimeout,
				IncludeRetryReason:        ConfigBoolTrue,
			},
			ocspMode: ocspModeFailOpen,
			err:      nil,
		},
		{
			dsn: "user:pass@account?region=eu-faraway",
			config: &Config{
				Account: "account", User: "user", Password: "pass", Region: "eu-faraway",
				Protocol: "https", Host: "account.eu-faraway.snowflakecomputing.com", Port: 443,
				OCSPFailOpen:              OCSPFailOpenTrue,
				ValidateDefaultParameters: ConfigBoolTrue,
				ClientTimeout:             defaultClientTimeout,
				JWTClientTimeout:          defaultJWTClientTimeout,
				ExternalBrowserTimeout:    defaultExternalBrowserTimeout,
				CloudStorageTimeout:       defaultCloudStorageTimeout,
				IncludeRetryReason:        ConfigBoolTrue,
			},
			ocspMode: ocspModeFailOpen,
			err:      nil,
		},
		{
			dsn: "user:pass@account/db",
			config: &Config{
				Account: "account", User: "user", Password: "pass",
				Protocol: "https", Host: "account.snowflakecomputing.com", Port: 443,
				Database:                  "db",
				OCSPFailOpen:              OCSPFailOpenTrue,
				ValidateDefaultParameters: ConfigBoolTrue,
				ClientTimeout:             defaultClientTimeout,
				JWTClientTimeout:          defaultJWTClientTimeout,
				ExternalBrowserTimeout:    defaultExternalBrowserTimeout,
				CloudStorageTimeout:       defaultCloudStorageTimeout,
				IncludeRetryReason:        ConfigBoolTrue,
			},
			ocspMode: ocspModeFailOpen,
			err:      nil,
		},
		{
			dsn: "user:pass@account?oauthRedirectUri=http:%2F%2Flocalhost:8001%2Fsome-path&oauthClientId=testClientId&oauthClientSecret=testClientSecret&oauthAuthorizationUrl=http:%2F%2Fsomehost.com&oauthTokenRequestUrl=https:%2F%2Fsomehost2.com%2Fsomepath&oauthScope=test+scope",
			config: &Config{
				Account: "account", User: "user", Password: "pass",
				Protocol: "https", Host: "account.snowflakecomputing.com", Port: 443,
				OauthClientID: "testClientId", OauthClientSecret: "testClientSecret", OauthAuthorizationURL: "http://somehost.com", OauthTokenRequestURL: "https://somehost2.com/somepath", OauthRedirectURI: "http://localhost:8001/some-path", OauthScope: "test scope",
				OCSPFailOpen:              OCSPFailOpenTrue,
				ValidateDefaultParameters: ConfigBoolTrue,
				ClientTimeout:             defaultClientTimeout,
				JWTClientTimeout:          defaultJWTClientTimeout,
				ExternalBrowserTimeout:    defaultExternalBrowserTimeout,
				CloudStorageTimeout:       defaultCloudStorageTimeout,
				IncludeRetryReason:        ConfigBoolTrue,
			},
			ocspMode: ocspModeFailOpen,
			err:      nil,
		},
		{
			dsn: "user:pass@account?oauthRedirectUri=http:%2F%2Flocalhost:8001%2Fsome-path&oauthClientId=testClientId&oauthClientSecret=testClientSecret&oauthAuthorizationUrl=http:%2F%2Fsomehost.com&oauthTokenRequestUrl=https:%2F%2Fsomehost2.com%2Fsomepath&oauthScope=test+scope&enableSingleUseRefreshTokens=true",
			config: &Config{
				Account: "account", User: "user", Password: "pass",
				Protocol: "https", Host: "account.snowflakecomputing.com", Port: 443,
				OauthClientID: "testClientId", OauthClientSecret: "testClientSecret", OauthAuthorizationURL: "http://somehost.com", OauthTokenRequestURL: "https://somehost2.com/somepath", OauthRedirectURI: "http://localhost:8001/some-path", OauthScope: "test scope",
				EnableSingleUseRefreshTokens: true,
				OCSPFailOpen:                 OCSPFailOpenTrue,
				ValidateDefaultParameters:    ConfigBoolTrue,
				ClientTimeout:                defaultClientTimeout,
				JWTClientTimeout:             defaultJWTClientTimeout,
				ExternalBrowserTimeout:       defaultExternalBrowserTimeout,
				CloudStorageTimeout:          defaultCloudStorageTimeout,
				IncludeRetryReason:           ConfigBoolTrue,
			},
			ocspMode: ocspModeFailOpen,
			err:      nil,
		},
		{
			dsn: "user:pass@host:123/db/schema?account=ac&protocol=http",
			config: &Config{
				Account: "ac", User: "user", Password: "pass",
				Protocol: "http", Host: "host", Port: 123,
				Database: "db", Schema: "schema",
				OCSPFailOpen:              OCSPFailOpenTrue,
				ValidateDefaultParameters: ConfigBoolTrue,
				ClientTimeout:             defaultClientTimeout,
				JWTClientTimeout:          defaultJWTClientTimeout,
				ExternalBrowserTimeout:    defaultExternalBrowserTimeout,
				CloudStorageTimeout:       defaultCloudStorageTimeout,
				IncludeRetryReason:        ConfigBoolTrue,
			},
			ocspMode: ocspModeFailOpen,
			err:      nil,
		},
		{
			dsn: "user@host:123/db/schema?account=ac&protocol=http",
			config: &Config{
				Account: "ac", User: "user", Password: "pass",
				Protocol: "http", Host: "host", Port: 123,
				Database: "db", Schema: "schema",
				OCSPFailOpen:              OCSPFailOpenTrue,
				ValidateDefaultParameters: ConfigBoolTrue,
				ClientTimeout:             defaultClientTimeout,
				JWTClientTimeout:          defaultJWTClientTimeout,
				ExternalBrowserTimeout:    defaultExternalBrowserTimeout,
				CloudStorageTimeout:       defaultCloudStorageTimeout,
				IncludeRetryReason:        ConfigBoolTrue,
			},
			ocspMode: ocspModeFailOpen,
			err:      errEmptyPassword(),
		},
		{
			dsn: "@host:123/db/schema?account=ac&protocol=http",
			config: &Config{
				Account: "ac", User: "user", Password: "pass",
				Protocol: "http", Host: "host", Port: 123,
				Database: "db", Schema: "schema",
				OCSPFailOpen:              OCSPFailOpenTrue,
				ValidateDefaultParameters: ConfigBoolTrue,
				ClientTimeout:             defaultClientTimeout,
				JWTClientTimeout:          defaultJWTClientTimeout,
				ExternalBrowserTimeout:    defaultExternalBrowserTimeout,
				CloudStorageTimeout:       defaultCloudStorageTimeout,
				IncludeRetryReason:        ConfigBoolTrue,
			},
			ocspMode: ocspModeFailOpen,
			err:      errEmptyUsername(),
		},
		{
			dsn: "@host:123/db/schema?account=ac&protocol=http&authenticator=oauth_authorization_code",
			config: &Config{
				Account: "ac", User: "user", Password: "pass",
				Protocol: "http", Host: "host", Port: 123,
				Database: "db", Schema: "schema",
				OCSPFailOpen:              OCSPFailOpenTrue,
				ValidateDefaultParameters: ConfigBoolTrue,
				ClientTimeout:             defaultClientTimeout,
				JWTClientTimeout:          defaultJWTClientTimeout,
				ExternalBrowserTimeout:    defaultExternalBrowserTimeout,
				CloudStorageTimeout:       defaultCloudStorageTimeout,
				IncludeRetryReason:        ConfigBoolTrue,
			},
			ocspMode: ocspModeFailOpen,
			err:      errEmptyOAuthParameters(),
		},
		{
			dsn: "user:pass@host:123/db/schema?protocol=http",
			config: &Config{
				Account: "ac", User: "user", Password: "pass",
				Protocol: "http", Host: "host", Port: 123,
				Database: "db", Schema: "schema",
				OCSPFailOpen:              OCSPFailOpenTrue,
				ValidateDefaultParameters: ConfigBoolTrue,
				ClientTimeout:             defaultClientTimeout,
				JWTClientTimeout:          defaultJWTClientTimeout,
				ExternalBrowserTimeout:    defaultExternalBrowserTimeout,
				CloudStorageTimeout:       defaultCloudStorageTimeout,
				IncludeRetryReason:        ConfigBoolTrue,
			},
			ocspMode: ocspModeFailOpen,
			err:      errEmptyAccount(),
		},
		{
			dsn: "user:@host:123/db/schema?protocol=http&authenticator=programmatic_access_token&account=ac",
			config: &Config{
				Account: "ac", User: "user", Password: "pass",
				Protocol: "http", Host: "host", Port: 123,
				Database: "db", Schema: "schema",
				OCSPFailOpen:              OCSPFailOpenTrue,
				ValidateDefaultParameters: ConfigBoolTrue,
				ClientTimeout:             defaultClientTimeout,
				JWTClientTimeout:          defaultJWTClientTimeout,
				ExternalBrowserTimeout:    defaultExternalBrowserTimeout,
				CloudStorageTimeout:       defaultCloudStorageTimeout,
				IncludeRetryReason:        ConfigBoolTrue,
			},
			ocspMode: ocspModeFailOpen,
			err:      errEmptyPasswordAndToken(),
		},
		{
			dsn: "u:p@a.snowflakecomputing.com/db/pa?account=a&protocol=https&role=r&timezone=UTC&warehouse=w",
			config: &Config{
				Account: "a", User: "u", Password: "p",
				Protocol: "https", Host: "a.snowflakecomputing.com", Port: 443,
				Database: "db", Schema: "pa", Role: "r", Warehouse: "w",
				OCSPFailOpen:              OCSPFailOpenTrue,
				ValidateDefaultParameters: ConfigBoolTrue,
				ClientTimeout:             defaultClientTimeout,
				JWTClientTimeout:          defaultJWTClientTimeout,
				ExternalBrowserTimeout:    defaultExternalBrowserTimeout,
				CloudStorageTimeout:       defaultCloudStorageTimeout,
				IncludeRetryReason:        ConfigBoolTrue,
			},
			ocspMode: ocspModeFailOpen,
			err:      nil,
		},
		{
			dsn: "u:p@a.snowflakecomputing.mil/db/pa?account=a",
			config: &Config{
				Account: "a", User: "u", Password: "p", Region: "",
				Protocol: "https", Host: "a.snowflakecomputing.mil", Port: 443,
				Database: "db", Schema: "pa",
				OCSPFailOpen:              OCSPFailOpenTrue,
				ValidateDefaultParameters: ConfigBoolTrue,
				ClientTimeout:             defaultClientTimeout,
				JWTClientTimeout:          defaultJWTClientTimeout,
				ExternalBrowserTimeout:    defaultExternalBrowserTimeout,
				CloudStorageTimeout:       defaultCloudStorageTimeout,
				IncludeRetryReason:        ConfigBoolTrue,
			},
			ocspMode: ocspModeFailOpen,
			err:      nil,
		},
		{
			dsn: "u:p@a.eu-faraway.snowflakecomputing.mil/db/pa?account=a&region=eu-faraway",
			config: &Config{
				Account: "a", User: "u", Password: "p", Region: "eu-faraway",
				Protocol: "https", Host: "a.eu-faraway.snowflakecomputing.mil", Port: 443,
				Database: "db", Schema: "pa",
				OCSPFailOpen:              OCSPFailOpenTrue,
				ValidateDefaultParameters: ConfigBoolTrue,
				ClientTimeout:             defaultClientTimeout,
				JWTClientTimeout:          defaultJWTClientTimeout,
				ExternalBrowserTimeout:    defaultExternalBrowserTimeout,
				CloudStorageTimeout:       defaultCloudStorageTimeout,
				IncludeRetryReason:        ConfigBoolTrue,
			},
			ocspMode: ocspModeFailOpen,
			err:      nil,
		},
		{
			dsn: "u:p@a.snowflakecomputing.gov.pl/db/pa?account=a",
			config: &Config{
				Account: "a", User: "u", Password: "p", Region: "",
				Protocol: "https", Host: "a.snowflakecomputing.gov.pl", Port: 443,
				Database: "db", Schema: "pa",
				OCSPFailOpen:              OCSPFailOpenTrue,
				ValidateDefaultParameters: ConfigBoolTrue,
				ClientTimeout:             defaultClientTimeout,
				JWTClientTimeout:          defaultJWTClientTimeout,
				ExternalBrowserTimeout:    defaultExternalBrowserTimeout,
				CloudStorageTimeout:       defaultCloudStorageTimeout,
				IncludeRetryReason:        ConfigBoolTrue,
			},
			ocspMode: ocspModeFailOpen,
			err:      nil,
		},
		{
			dsn: "u:p@a.snowflakecomputing.cn/db/pa?account=a",
			config: &Config{
				Account: "a", User: "u", Password: "p", Region: "",
				Protocol: "https", Host: "a.snowflakecomputing.cn", Port: 443,
				Database: "db", Schema: "pa",
				OCSPFailOpen:              OCSPFailOpenTrue,
				ValidateDefaultParameters: ConfigBoolTrue,
				ClientTimeout:             defaultClientTimeout,
				JWTClientTimeout:          defaultJWTClientTimeout,
				ExternalBrowserTimeout:    defaultExternalBrowserTimeout,
				CloudStorageTimeout:       defaultCloudStorageTimeout,
				IncludeRetryReason:        ConfigBoolTrue,
			},
			ocspMode: ocspModeFailOpen,
			err:      nil,
		},
		{
			dsn: "u:p@a.cn-region.snowflakecomputing.mil/db/pa?account=a&region=cn-region",
			config: &Config{
				Account: "a", User: "u", Password: "p", Region: "cn-region",
				Protocol: "https", Host: "a.cn-region.snowflakecomputing.mil", Port: 443,
				Database: "db", Schema: "pa",
				OCSPFailOpen:              OCSPFailOpenTrue,
				ValidateDefaultParameters: ConfigBoolTrue,
				ClientTimeout:             defaultClientTimeout,
				JWTClientTimeout:          defaultJWTClientTimeout,
				ExternalBrowserTimeout:    defaultExternalBrowserTimeout,
				CloudStorageTimeout:       defaultCloudStorageTimeout,
				IncludeRetryReason:        ConfigBoolTrue,
			},
			ocspMode: ocspModeFailOpen,
			err:      nil,
		},
		{
			dsn: "u:p@a.cn-region.snowflakecomputing.cn/db/pa?account=a&region=cn-region&protocol=https&role=r&timezone=UTC&warehouse=w",
			config: &Config{
				Account: "a", User: "u", Password: "p", Region: "cn-region",
				Protocol: "https", Host: "a.cn-region.snowflakecomputing.cn", Port: 443,
				Database: "db", Schema: "pa", Role: "r", Warehouse: "w",
				OCSPFailOpen:              OCSPFailOpenTrue,
				ValidateDefaultParameters: ConfigBoolTrue,
				ClientTimeout:             defaultClientTimeout,
				JWTClientTimeout:          defaultJWTClientTimeout,
				ExternalBrowserTimeout:    defaultExternalBrowserTimeout,
				CloudStorageTimeout:       defaultCloudStorageTimeout,
				IncludeRetryReason:        ConfigBoolTrue,
			},
			ocspMode: ocspModeFailOpen,
			err:      nil,
		},
		{
			dsn: "u:p@snowflake.local:9876?account=a&protocol=http",
			config: &Config{
				Account: "a", User: "u", Password: "p",
				Protocol: "http", Host: "snowflake.local", Port: 9876,
				OCSPFailOpen:              OCSPFailOpenTrue,
				ValidateDefaultParameters: ConfigBoolTrue,
				ClientTimeout:             defaultClientTimeout,
				JWTClientTimeout:          defaultJWTClientTimeout,
				ExternalBrowserTimeout:    defaultExternalBrowserTimeout,
				CloudStorageTimeout:       defaultCloudStorageTimeout,
				IncludeRetryReason:        ConfigBoolTrue,
			},
			ocspMode: ocspModeFailOpen,
			err:      nil,
		},
		{
			dsn: "snowflake.local:9876?account=a&protocol=http&authenticator=OAUTH",
			config: &Config{
				Account: "a", Authenticator: AuthTypeOAuth,
				Protocol: "http", Host: "snowflake.local", Port: 9876,
				OCSPFailOpen:              OCSPFailOpenTrue,
				ValidateDefaultParameters: ConfigBoolTrue,
				ClientTimeout:             defaultClientTimeout,
				JWTClientTimeout:          defaultJWTClientTimeout,
				ExternalBrowserTimeout:    defaultExternalBrowserTimeout,
				CloudStorageTimeout:       defaultCloudStorageTimeout,
				IncludeRetryReason:        ConfigBoolTrue,
			},
			ocspMode: ocspModeFailOpen,
			err:      nil,
		},
		{
			dsn: "snowflake.local:9876?account=a&protocol=http&authenticator=OAUTH_AUTHORIZATION_CODE&oauthClientId=testClientId&oauthClientSecret=testClientSecret",
			config: &Config{
				Account: "a", Authenticator: AuthTypeOAuthAuthorizationCode,
				Protocol: "http", Host: "snowflake.local", Port: 9876,
				OCSPFailOpen:              OCSPFailOpenTrue,
				ValidateDefaultParameters: ConfigBoolTrue,
				ClientTimeout:             defaultClientTimeout,
				JWTClientTimeout:          defaultJWTClientTimeout,
				ExternalBrowserTimeout:    defaultExternalBrowserTimeout,
				CloudStorageTimeout:       defaultCloudStorageTimeout,
				IncludeRetryReason:        ConfigBoolTrue,
				OauthClientID:             "testClientId",
				OauthClientSecret:         "testClientSecret",
			},
			ocspMode: ocspModeFailOpen,
			err:      nil,
		},
		{
			dsn: "snowflake.local:9876?account=a&protocol=http&authenticator=OAUTH_CLIENT_CREDENTIALS",
			config: &Config{
				Account: "a", Authenticator: AuthTypeOAuthClientCredentials,
				Protocol: "http", Host: "snowflake.local", Port: 9876,
				OCSPFailOpen:              OCSPFailOpenTrue,
				ValidateDefaultParameters: ConfigBoolTrue,
				ClientTimeout:             defaultClientTimeout,
				JWTClientTimeout:          defaultJWTClientTimeout,
				ExternalBrowserTimeout:    defaultExternalBrowserTimeout,
				CloudStorageTimeout:       defaultCloudStorageTimeout,
				IncludeRetryReason:        ConfigBoolTrue,
			},
			ocspMode: ocspModeFailOpen,
			err:      nil,
		},
		{
			dsn: "u:@a.snowflake.local:9876?account=a&protocol=http&authenticator=SNOWFLAKE_JWT",
			config: &Config{
				Account: "a", User: "u", Authenticator: AuthTypeJwt,
				Protocol: "http", Host: "a.snowflake.local", Port: 9876,
				OCSPFailOpen:              OCSPFailOpenTrue,
				ValidateDefaultParameters: ConfigBoolTrue,
				ClientTimeout:             defaultClientTimeout,
				JWTClientTimeout:          defaultJWTClientTimeout,
				ExternalBrowserTimeout:    defaultExternalBrowserTimeout,
				CloudStorageTimeout:       defaultCloudStorageTimeout,
				IncludeRetryReason:        ConfigBoolTrue,
			},
			ocspMode: ocspModeFailOpen,
			err:      nil,
		},

		{
			dsn: "u:p@a?database=d&jwtTimeout=20",
			config: &Config{
				Account: "a", User: "u", Password: "p",
				Protocol: "https", Host: "a.snowflakecomputing.com", Port: 443,
				Database: "d", Schema: "",
				JWTExpireTimeout:          20 * time.Second,
				OCSPFailOpen:              OCSPFailOpenTrue,
				ValidateDefaultParameters: ConfigBoolTrue,
				ClientTimeout:             defaultClientTimeout,
				JWTClientTimeout:          defaultJWTClientTimeout,
				ExternalBrowserTimeout:    defaultExternalBrowserTimeout,
				CloudStorageTimeout:       defaultCloudStorageTimeout,
				IncludeRetryReason:        ConfigBoolTrue,
			},
			ocspMode: ocspModeFailOpen,
		},
		{
			dsn: "u:p@a?database=d&externalBrowserTimeout=20&cloudStorageTimeout=7",
			config: &Config{
				Account: "a", User: "u", Password: "p",
				Protocol: "https", Host: "a.snowflakecomputing.com", Port: 443,
				Database: "d", Schema: "",
				ExternalBrowserTimeout:    20 * time.Second,
				CloudStorageTimeout:       7 * time.Second,
				OCSPFailOpen:              OCSPFailOpenTrue,
				ValidateDefaultParameters: ConfigBoolTrue,
				ClientTimeout:             defaultClientTimeout,
				JWTClientTimeout:          defaultJWTClientTimeout,
				IncludeRetryReason:        ConfigBoolTrue,
				MaxRetryCount:             defaultMaxRetryCount,
			},
			ocspMode: ocspModeFailOpen,
		},
		{
			dsn: "u:p@a?database=d&maxRetryCount=20",
			config: &Config{
				Account: "a", User: "u", Password: "p",
				Protocol: "https", Host: "a.snowflakecomputing.com", Port: 443,
				Database: "d", Schema: "",
				ExternalBrowserTimeout:    defaultExternalBrowserTimeout,
				OCSPFailOpen:              OCSPFailOpenTrue,
				ValidateDefaultParameters: ConfigBoolTrue,
				ClientTimeout:             defaultClientTimeout,
				JWTClientTimeout:          defaultJWTClientTimeout,
				CloudStorageTimeout:       defaultCloudStorageTimeout,
				IncludeRetryReason:        ConfigBoolTrue,
				MaxRetryCount:             20,
			},
			ocspMode: ocspModeFailOpen,
		},
		{
			dsn: "u:p@a?database=d",
			config: &Config{
				Account: "a", User: "u", Password: "p",
				Protocol: "https", Host: "a.snowflakecomputing.com", Port: 443,
				Database: "d", Schema: "",
				JWTExpireTimeout:          defaultJWTTimeout,
				OCSPFailOpen:              OCSPFailOpenTrue,
				ValidateDefaultParameters: ConfigBoolTrue,
				ClientTimeout:             defaultClientTimeout,
				JWTClientTimeout:          defaultJWTClientTimeout,
				ExternalBrowserTimeout:    defaultExternalBrowserTimeout,
				CloudStorageTimeout:       defaultCloudStorageTimeout,
				IncludeRetryReason:        ConfigBoolTrue,
			},
			ocspMode: ocspModeFailOpen,
		},
		{
			dsn: "u:p@snowflake.local:NNNN?account=a&protocol=http",
			config: &Config{
				Account: "a", User: "u", Password: "p",
				Protocol: "http", Host: "snowflake.local", Port: 9876,
				OCSPFailOpen:              OCSPFailOpenTrue,
				ValidateDefaultParameters: ConfigBoolTrue,
				ClientTimeout:             defaultClientTimeout,
				JWTClientTimeout:          defaultJWTClientTimeout,
				ExternalBrowserTimeout:    defaultExternalBrowserTimeout,
				CloudStorageTimeout:       defaultCloudStorageTimeout,
				IncludeRetryReason:        ConfigBoolTrue,
			},
			ocspMode: ocspModeFailOpen,
			err: &SnowflakeError{
				Message:     errMsgFailedToParsePort,
				MessageArgs: []interface{}{"NNNN"},
				Number:      ErrCodeFailedToParsePort,
			},
		},
		{
			dsn: "u:p@a?database=d&schema=s&role=r&application=aa&authenticator=snowflake&insecureMode=true&passcode=pp&passcodeInPassword=true",
			config: &Config{
				Account: "a", User: "u", Password: "p",
				Protocol: "https", Host: "a.snowflakecomputing.com", Port: 443,
				Database: "d", Schema: "s", Role: "r", Authenticator: AuthTypeSnowflake, Application: "aa",
				InsecureMode: true, Passcode: "pp", PasscodeInPassword: true,
				OCSPFailOpen:              OCSPFailOpenTrue,
				ValidateDefaultParameters: ConfigBoolTrue,
				ClientTimeout:             defaultClientTimeout,
				JWTClientTimeout:          defaultJWTClientTimeout,
				ExternalBrowserTimeout:    defaultExternalBrowserTimeout,
				CloudStorageTimeout:       defaultCloudStorageTimeout,
				IncludeRetryReason:        ConfigBoolTrue,
			},
			ocspMode: ocspModeInsecure,
			err:      nil,
		},
		{
			dsn: "u:p@a?database=d&schema=s&role=r&application=aa&authenticator=snowflake&disableOCSPChecks=true&passcode=pp&passcodeInPassword=true",
			config: &Config{
				Account: "a", User: "u", Password: "p",
				Protocol: "https", Host: "a.snowflakecomputing.com", Port: 443,
				Database: "d", Schema: "s", Role: "r", Authenticator: AuthTypeSnowflake, Application: "aa",
				DisableOCSPChecks: true, Passcode: "pp", PasscodeInPassword: true,
				OCSPFailOpen:              OCSPFailOpenTrue,
				ValidateDefaultParameters: ConfigBoolTrue,
				ClientTimeout:             defaultClientTimeout,
				JWTClientTimeout:          defaultJWTClientTimeout,
				ExternalBrowserTimeout:    defaultExternalBrowserTimeout,
				CloudStorageTimeout:       defaultCloudStorageTimeout,
				IncludeRetryReason:        ConfigBoolTrue,
			},
			ocspMode: ocspModeInsecure,
			err:      nil,
		},
		{
			dsn: "u:p@a?database=d&schema=s&role=r&application=aa&authenticator=snowflake&disableOCSPChecks=true&passcode=pp&passcodeInPassword=true",
			config: &Config{
				Account: "a", User: "u", Password: "p",
				Protocol: "https", Host: "a.snowflakecomputing.com", Port: 443,
				Database: "d", Schema: "s", Role: "r", Authenticator: AuthTypeSnowflake, Application: "aa",
				InsecureMode: false, DisableOCSPChecks: true, Passcode: "pp", PasscodeInPassword: true,
				OCSPFailOpen:              OCSPFailOpenTrue,
				ValidateDefaultParameters: ConfigBoolTrue,
				ClientTimeout:             defaultClientTimeout,
				JWTClientTimeout:          defaultJWTClientTimeout,
				ExternalBrowserTimeout:    defaultExternalBrowserTimeout,
				CloudStorageTimeout:       defaultCloudStorageTimeout,
				IncludeRetryReason:        ConfigBoolTrue,
			},
			ocspMode: ocspModeInsecure,
			err:      nil,
		},
		// disableOCSPChecks should take precedence over insecureMode
		{
			dsn: "u:p@a?database=d&schema=s&role=r&application=aa&authenticator=snowflake&disableOCSPChecks=false&insecureMode=true&passcode=pp&passcodeInPassword=true",
			config: &Config{
				Account: "a", User: "u", Password: "p",
				Protocol: "https", Host: "a.snowflakecomputing.com", Port: 443,
				Database: "d", Schema: "s", Role: "r", Authenticator: AuthTypeSnowflake, Application: "aa",
				DisableOCSPChecks: false, Passcode: "pp", PasscodeInPassword: true,
				OCSPFailOpen:              OCSPFailOpenTrue,
				ValidateDefaultParameters: ConfigBoolTrue,
				ClientTimeout:             defaultClientTimeout,
				JWTClientTimeout:          defaultJWTClientTimeout,
				ExternalBrowserTimeout:    defaultExternalBrowserTimeout,
				CloudStorageTimeout:       defaultCloudStorageTimeout,
				IncludeRetryReason:        ConfigBoolTrue,
			},
			ocspMode: ocspModeFailOpen,
			err:      nil,
		},
		{
			// schema should be ignored as no value is specified.
			dsn: "u:p@a?database=d&schema",
			config: &Config{
				Account: "a", User: "u", Password: "p",
				Protocol: "https", Host: "a.snowflakecomputing.com", Port: 443,
				Database: "d", Schema: "",
				OCSPFailOpen:              OCSPFailOpenTrue,
				ValidateDefaultParameters: ConfigBoolTrue,
				ClientTimeout:             defaultClientTimeout,
				JWTClientTimeout:          defaultJWTClientTimeout,
				ExternalBrowserTimeout:    defaultExternalBrowserTimeout,
				CloudStorageTimeout:       defaultCloudStorageTimeout,
				IncludeRetryReason:        ConfigBoolTrue,
			},
			ocspMode: ocspModeFailOpen,
			err:      nil,
		},
		{
			dsn:    "u:p@a?database= %Sd",
			config: &Config{},
			err:    url.EscapeError(`invalid URL escape`),
		},
		{
			dsn:    "u:p@a?schema= %Sd",
			config: &Config{},
			err:    url.EscapeError(`invalid URL escape`),
		},
		{
			dsn:    "u:p@a?warehouse= %Sd",
			config: &Config{},
			err:    url.EscapeError(`invalid URL escape`),
		},
		{
			dsn:    "u:p@a?role= %Sd",
			config: &Config{},
			err:    url.EscapeError(`invalid URL escape`),
		},
		{
			dsn:    ":/",
			config: &Config{},
			err: &SnowflakeError{
				Number: ErrCodeFailedToParsePort,
			},
		},
		{
			dsn:    "u:u@/+/+?account=+&=0",
			config: &Config{},
			err:    errEmptyAccount(),
		},
		{
			dsn:    "u:u@/+/+?account=+&=+&=+",
			config: &Config{},
			err:    errEmptyAccount(),
		},
		{
			dsn: "user%40%2F1:p%3A%40s@/db%2F?account=ac",
			config: &Config{
				Account: "ac", User: "user@/1", Password: "p:@s", Database: "db/",
				Protocol: "https", Host: "ac.snowflakecomputing.com", Port: 443,
				OCSPFailOpen:              OCSPFailOpenTrue,
				ValidateDefaultParameters: ConfigBoolTrue,
				ClientTimeout:             defaultClientTimeout,
				JWTClientTimeout:          defaultJWTClientTimeout,
				ExternalBrowserTimeout:    defaultExternalBrowserTimeout,
				CloudStorageTimeout:       defaultCloudStorageTimeout,
				IncludeRetryReason:        ConfigBoolTrue,
			},
			ocspMode: ocspModeFailOpen,
			err:      nil,
		},
		{
			dsn: fmt.Sprintf("u:p@ac.snowflake.local:9876?account=ac&protocol=http&authenticator=SNOWFLAKE_JWT&privateKey=%v", privKeyPKCS8),
			config: &Config{
				Account: "ac", User: "u", Password: "p",
				Authenticator: AuthTypeJwt, PrivateKey: testPrivKey,
				Protocol: "http", Host: "ac.snowflake.local", Port: 9876,
				OCSPFailOpen:              OCSPFailOpenTrue,
				ValidateDefaultParameters: ConfigBoolTrue,
				ClientTimeout:             defaultClientTimeout,
				JWTClientTimeout:          defaultJWTClientTimeout,
				ExternalBrowserTimeout:    defaultExternalBrowserTimeout,
				CloudStorageTimeout:       defaultCloudStorageTimeout,
				IncludeRetryReason:        ConfigBoolTrue,
			},
			ocspMode: ocspModeFailOpen,
			err:      nil,
		},
		{
			dsn: fmt.Sprintf("u:p@ac.snowflake.local:9876?account=ac&protocol=http&authenticator=%v", url.QueryEscape("https://ac.okta.com")),
			config: &Config{
				Account: "ac", User: "u", Password: "p",
				Authenticator: AuthTypeOkta,
				OktaURL: &url.URL{
					Scheme: "https",
					Host:   "ac.okta.com",
				},
				PrivateKey: testPrivKey,
				Protocol:   "http", Host: "ac.snowflake.local", Port: 9876,
				OCSPFailOpen:              OCSPFailOpenTrue,
				ValidateDefaultParameters: ConfigBoolTrue,
				ClientTimeout:             defaultClientTimeout,
				JWTClientTimeout:          defaultJWTClientTimeout,
				ExternalBrowserTimeout:    defaultExternalBrowserTimeout,
				CloudStorageTimeout:       defaultCloudStorageTimeout,
				IncludeRetryReason:        ConfigBoolTrue,
			},
			ocspMode: ocspModeFailOpen,
			err:      nil,
		},
		{
			dsn: fmt.Sprintf("u:p@ac.snowflake.local:9876?account=ac&protocol=http&authenticator=%v", url.QueryEscape("https://ac.some-host.com/custom-okta-url")),
			config: &Config{
				Account: "ac", User: "u", Password: "p",
				Authenticator: AuthTypeOkta,
				OktaURL: &url.URL{
					Scheme: "https",
					Host:   "ac.some-host.com",
					Path:   "/custom-okta-url",
				},
				PrivateKey: testPrivKey,
				Protocol:   "http", Host: "ac.snowflake.local", Port: 9876,
				OCSPFailOpen:              OCSPFailOpenTrue,
				ValidateDefaultParameters: ConfigBoolTrue,
				ClientTimeout:             defaultClientTimeout,
				JWTClientTimeout:          defaultJWTClientTimeout,
				ExternalBrowserTimeout:    defaultExternalBrowserTimeout,
				CloudStorageTimeout:       defaultCloudStorageTimeout,
				IncludeRetryReason:        ConfigBoolTrue,
			},
			ocspMode: ocspModeFailOpen,
			err:      nil,
		},
		{
			dsn: fmt.Sprintf("u:p@a.snowflake.local:9876?account=a&protocol=http&authenticator=SNOWFLAKE_JWT&privateKey=%v", privKeyPKCS1),
			config: &Config{
				Account: "a", User: "u", Password: "p",
				Authenticator: AuthTypeJwt, PrivateKey: testPrivKey,
				Protocol: "http", Host: "a.snowflake.local", Port: 9876,
				OCSPFailOpen:              OCSPFailOpenTrue,
				ValidateDefaultParameters: ConfigBoolTrue,
				ClientTimeout:             defaultClientTimeout,
				JWTClientTimeout:          defaultJWTClientTimeout,
				ExternalBrowserTimeout:    defaultExternalBrowserTimeout,
				CloudStorageTimeout:       defaultCloudStorageTimeout,
				IncludeRetryReason:        ConfigBoolTrue,
			},
			ocspMode: ocspModeFailOpen,
			err:      &SnowflakeError{Number: ErrCodePrivateKeyParseError},
		},
		{
			dsn: "user:pass@account/db/s?ocspFailOpen=true",
			config: &Config{
				Account: "account", User: "user", Password: "pass",
				Protocol: "https", Host: "account.snowflakecomputing.com", Port: 443,
				Database: "db", Schema: "s", OCSPFailOpen: OCSPFailOpenTrue,
				ValidateDefaultParameters: ConfigBoolTrue,
				ClientTimeout:             defaultClientTimeout,
				JWTClientTimeout:          defaultJWTClientTimeout,
				ExternalBrowserTimeout:    defaultExternalBrowserTimeout,
				CloudStorageTimeout:       defaultCloudStorageTimeout,
				IncludeRetryReason:        ConfigBoolTrue,
			},
			ocspMode: ocspModeFailOpen,
			err:      nil,
		},
		{
			dsn: "user:pass@account/db/s?ocspFailOpen=false",
			config: &Config{
				Account: "account", User: "user", Password: "pass",
				Protocol: "https", Host: "account.snowflakecomputing.com", Port: 443,
				Database: "db", Schema: "s", OCSPFailOpen: OCSPFailOpenFalse,
				ValidateDefaultParameters: ConfigBoolTrue,
				ClientTimeout:             defaultClientTimeout,
				JWTClientTimeout:          defaultJWTClientTimeout,
				ExternalBrowserTimeout:    defaultExternalBrowserTimeout,
				CloudStorageTimeout:       defaultCloudStorageTimeout,
				IncludeRetryReason:        ConfigBoolTrue,
			},
			ocspMode: ocspModeFailClosed,
			err:      nil,
		},
		{
			dsn: "user:pass@account/db/s?insecureMode=true&ocspFailOpen=false",
			config: &Config{
				Account: "account", User: "user", Password: "pass",
				Protocol: "https", Host: "account.snowflakecomputing.com", Port: 443,
				Database: "db", Schema: "s", OCSPFailOpen: OCSPFailOpenFalse, InsecureMode: true,
				ValidateDefaultParameters: ConfigBoolTrue,
				ClientTimeout:             defaultClientTimeout,
				JWTClientTimeout:          defaultJWTClientTimeout,
				ExternalBrowserTimeout:    defaultExternalBrowserTimeout,
				CloudStorageTimeout:       defaultCloudStorageTimeout,
				IncludeRetryReason:        ConfigBoolTrue,
			},
			ocspMode: ocspModeInsecure,
			err:      nil,
		},
		{
			dsn: "user:pass@account/db/s?validateDefaultParameters=true",
			config: &Config{
				Account: "account", User: "user", Password: "pass",
				Protocol: "https", Host: "account.snowflakecomputing.com", Port: 443,
				Database: "db", Schema: "s", ValidateDefaultParameters: ConfigBoolTrue, OCSPFailOpen: OCSPFailOpenTrue,
				ClientTimeout:          defaultClientTimeout,
				JWTClientTimeout:       defaultJWTClientTimeout,
				ExternalBrowserTimeout: defaultExternalBrowserTimeout,
				CloudStorageTimeout:    defaultCloudStorageTimeout,
				IncludeRetryReason:     ConfigBoolTrue,
			},
			ocspMode: ocspModeFailOpen,
			err:      nil,
		},
		{
			dsn: "user:pass@account/db/s?validateDefaultParameters=false",
			config: &Config{
				Account: "account", User: "user", Password: "pass",
				Protocol: "https", Host: "account.snowflakecomputing.com", Port: 443,
				Database: "db", Schema: "s", ValidateDefaultParameters: ConfigBoolFalse, OCSPFailOpen: OCSPFailOpenTrue,
				ClientTimeout:          defaultClientTimeout,
				JWTClientTimeout:       defaultJWTClientTimeout,
				ExternalBrowserTimeout: defaultExternalBrowserTimeout,
				CloudStorageTimeout:    defaultCloudStorageTimeout,
				IncludeRetryReason:     ConfigBoolTrue,
			},
			ocspMode: ocspModeFailOpen,
			err:      nil,
		},
		{
			dsn: "u:p@a.r.c.snowflakecomputing.com/db/s?account=a.r.c&validateDefaultParameters=false",
			config: &Config{
				Account: "a", User: "u", Password: "p",
				Protocol: "https", Host: "a.r.c.snowflakecomputing.com", Port: 443,
				Database: "db", Schema: "s", ValidateDefaultParameters: ConfigBoolFalse, OCSPFailOpen: OCSPFailOpenTrue,
				ClientTimeout:          defaultClientTimeout,
				JWTClientTimeout:       defaultJWTClientTimeout,
				ExternalBrowserTimeout: defaultExternalBrowserTimeout,
				CloudStorageTimeout:    defaultCloudStorageTimeout,
				IncludeRetryReason:     ConfigBoolTrue,
			},
			ocspMode: ocspModeFailOpen,
			err:      nil,
		},
		{
			dsn: "u:p@a.r.c.snowflakecomputing.com/db/s?account=a.r.c&clientTimeout=300&jwtClientTimeout=45&includeRetryReason=false",
			config: &Config{
				Account: "a", User: "u", Password: "p",
				Protocol: "https", Host: "a.r.c.snowflakecomputing.com", Port: 443,
				Database: "db", Schema: "s", ValidateDefaultParameters: ConfigBoolTrue, OCSPFailOpen: OCSPFailOpenTrue,
				ClientTimeout:            300 * time.Second,
				JWTClientTimeout:         45 * time.Second,
				ExternalBrowserTimeout:   defaultExternalBrowserTimeout,
				CloudStorageTimeout:      defaultCloudStorageTimeout,
				DisableQueryContextCache: false,
				IncludeRetryReason:       ConfigBoolFalse,
			},
			ocspMode: ocspModeFailOpen,
			err:      nil,
		},
		{
			dsn: "u:p@a.r.c.snowflakecomputing.com/db/s?account=a.r.c&tmpDirPath=%2Ftmp",
			config: &Config{
				Account: "a", User: "u", Password: "p",
				Protocol: "https", Host: "a.r.c.snowflakecomputing.com", Port: 443,
				Database: "db", Schema: "s", ValidateDefaultParameters: ConfigBoolTrue, OCSPFailOpen: OCSPFailOpenTrue,
				ClientTimeout:          defaultClientTimeout,
				JWTClientTimeout:       defaultJWTClientTimeout,
				ExternalBrowserTimeout: defaultExternalBrowserTimeout,
				CloudStorageTimeout:    defaultCloudStorageTimeout,
				TmpDirPath:             "/tmp",
				IncludeRetryReason:     ConfigBoolTrue,
			},
			ocspMode: ocspModeFailOpen,
			err:      nil,
		},
		{
			dsn: "u:p@a.r.c.snowflakecomputing.com/db/s?account=a.r.c&disableQueryContextCache=true",
			config: &Config{
				Account: "a", User: "u", Password: "p",
				Protocol: "https", Host: "a.r.c.snowflakecomputing.com", Port: 443,
				Database: "db", Schema: "s", ValidateDefaultParameters: ConfigBoolTrue, OCSPFailOpen: OCSPFailOpenTrue,
				ClientTimeout:            defaultClientTimeout,
				JWTClientTimeout:         defaultJWTClientTimeout,
				ExternalBrowserTimeout:   defaultExternalBrowserTimeout,
				CloudStorageTimeout:      defaultCloudStorageTimeout,
				DisableQueryContextCache: true,
				IncludeRetryReason:       ConfigBoolTrue,
			},
			ocspMode: ocspModeFailOpen,
			err:      nil,
		},
		{
			dsn: "u:p@a.r.c.snowflakecomputing.com/db/s?account=a.r.c&includeRetryReason=true",
			config: &Config{
				Account: "a", User: "u", Password: "p",
				Protocol: "https", Host: "a.r.c.snowflakecomputing.com", Port: 443,
				Database: "db", Schema: "s", ValidateDefaultParameters: ConfigBoolTrue, OCSPFailOpen: OCSPFailOpenTrue,
				ClientTimeout:          defaultClientTimeout,
				JWTClientTimeout:       defaultJWTClientTimeout,
				ExternalBrowserTimeout: defaultExternalBrowserTimeout,
				CloudStorageTimeout:    defaultCloudStorageTimeout,
				IncludeRetryReason:     ConfigBoolTrue,
			},
			ocspMode: ocspModeFailOpen,
			err:      nil,
		},
		{
			dsn: "u:p@a.r.c.snowflakecomputing.com/db/s?account=a.r.c&includeRetryReason=true&clientConfigFile=%2FUsers%2Fuser%2Fconfig.json",
			config: &Config{
				Account: "a", User: "u", Password: "p",
				Protocol: "https", Host: "a.r.c.snowflakecomputing.com", Port: 443,
				Database: "db", Schema: "s", ValidateDefaultParameters: ConfigBoolTrue, OCSPFailOpen: OCSPFailOpenTrue,
				ClientTimeout:          defaultClientTimeout,
				JWTClientTimeout:       defaultJWTClientTimeout,
				ExternalBrowserTimeout: defaultExternalBrowserTimeout,
				CloudStorageTimeout:    defaultCloudStorageTimeout,
				IncludeRetryReason:     ConfigBoolTrue,
				ClientConfigFile:       "/Users/user/config.json",
			},
			ocspMode: ocspModeFailOpen,
			err:      nil,
		},
		{
			dsn: "u:p@a.r.c.snowflakecomputing.com/db/s?account=a.r.c&includeRetryReason=true&clientConfigFile=c%3A%5CUsers%5Cuser%5Cconfig.json",
			config: &Config{
				Account: "a", User: "u", Password: "p",
				Protocol: "https", Host: "a.r.c.snowflakecomputing.com", Port: 443,
				Database: "db", Schema: "s", ValidateDefaultParameters: ConfigBoolTrue, OCSPFailOpen: OCSPFailOpenTrue,
				ClientTimeout:          defaultClientTimeout,
				JWTClientTimeout:       defaultJWTClientTimeout,
				ExternalBrowserTimeout: defaultExternalBrowserTimeout,
				CloudStorageTimeout:    defaultCloudStorageTimeout,
				IncludeRetryReason:     ConfigBoolTrue,
				ClientConfigFile:       "c:\\Users\\user\\config.json",
			},
			ocspMode: ocspModeFailOpen,
			err:      nil,
		},
		{
			dsn: "u:p@a.snowflakecomputing.com:443?authenticator=http%3A%2F%2Fsc.okta.com&ocspFailOpen=true&validateDefaultParameters=true",
			err: errFailedToParseAuthenticator(),
		},
		{
			dsn: "u:p@a.snowflake.local:9876?account=a&protocol=http&authenticator=EXTERNALBROWSER&disableConsoleLogin=true",
			config: &Config{
				Account: "a", User: "u", Password: "p",
				Authenticator: AuthTypeExternalBrowser,
				Protocol:      "http", Host: "a.snowflake.local", Port: 9876,
				OCSPFailOpen:              OCSPFailOpenTrue,
				ValidateDefaultParameters: ConfigBoolTrue,
				ClientTimeout:             defaultClientTimeout,
				JWTClientTimeout:          defaultJWTClientTimeout,
				ExternalBrowserTimeout:    defaultExternalBrowserTimeout,
				CloudStorageTimeout:       defaultCloudStorageTimeout,
				IncludeRetryReason:        ConfigBoolTrue,
				DisableConsoleLogin:       ConfigBoolTrue,
			},
			ocspMode: ocspModeFailOpen,
			err:      nil,
		},
		{
			dsn: "u:p@a.snowflake.local:9876?account=a&protocol=http&authenticator=EXTERNALBROWSER&disableConsoleLogin=false",
			config: &Config{
				Account: "a", User: "u", Password: "p",
				Authenticator: AuthTypeExternalBrowser,
				Protocol:      "http", Host: "a.snowflake.local", Port: 9876,
				OCSPFailOpen:              OCSPFailOpenTrue,
				ValidateDefaultParameters: ConfigBoolTrue,
				ClientTimeout:             defaultClientTimeout,
				JWTClientTimeout:          defaultJWTClientTimeout,
				ExternalBrowserTimeout:    defaultExternalBrowserTimeout,
				CloudStorageTimeout:       defaultCloudStorageTimeout,
				IncludeRetryReason:        ConfigBoolTrue,
				DisableConsoleLogin:       ConfigBoolFalse,
			},
			ocspMode: ocspModeFailOpen,
			err:      nil,
		},
		{
			dsn: "u:p@a.snowflake.local:9876?account=a&protocol=http&authenticator=EXTERNALBROWSER&disableSamlURLCheck=true",
			config: &Config{
				Account: "a", User: "u", Password: "p",
				Authenticator: AuthTypeExternalBrowser,
				Protocol:      "http", Host: "a.snowflake.local", Port: 9876,
				OCSPFailOpen:              OCSPFailOpenTrue,
				ValidateDefaultParameters: ConfigBoolTrue,
				ClientTimeout:             defaultClientTimeout,
				JWTClientTimeout:          defaultJWTClientTimeout,
				ExternalBrowserTimeout:    defaultExternalBrowserTimeout,
				CloudStorageTimeout:       defaultCloudStorageTimeout,
				IncludeRetryReason:        ConfigBoolTrue,
				DisableSamlURLCheck:       ConfigBoolTrue,
			},
			ocspMode: ocspModeFailOpen,
			err:      nil,
		},
		{
			dsn: "u:p@a.snowflake.local:9876?account=a&protocol=http&authenticator=EXTERNALBROWSER&disableSamlURLCheck=false",
			config: &Config{
				Account: "a", User: "u", Password: "p",
				Authenticator: AuthTypeExternalBrowser,
				Protocol:      "http", Host: "a.snowflake.local", Port: 9876,
				OCSPFailOpen:              OCSPFailOpenTrue,
				ValidateDefaultParameters: ConfigBoolTrue,
				ClientTimeout:             defaultClientTimeout,
				JWTClientTimeout:          defaultJWTClientTimeout,
				ExternalBrowserTimeout:    defaultExternalBrowserTimeout,
				CloudStorageTimeout:       defaultCloudStorageTimeout,
				IncludeRetryReason:        ConfigBoolTrue,
				DisableSamlURLCheck:       ConfigBoolFalse,
			},
			ocspMode: ocspModeFailOpen,
			err:      nil,
		},
		{
			dsn: "u:p@a.snowflake.local:9876?account=a&protocol=http&authenticator=PROGRAMMATIC_ACCESS_TOKEN&disableSamlURLCheck=false&token=t",
			config: &Config{
				Account: "a", User: "u", Password: "p",
				Authenticator: AuthTypePat,
				Protocol:      "http", Host: "a.snowflake.local", Port: 9876,
				OCSPFailOpen:              OCSPFailOpenTrue,
				ValidateDefaultParameters: ConfigBoolTrue,
				ClientTimeout:             defaultClientTimeout,
				JWTClientTimeout:          defaultJWTClientTimeout,
				ExternalBrowserTimeout:    defaultExternalBrowserTimeout,
				CloudStorageTimeout:       defaultCloudStorageTimeout,
				IncludeRetryReason:        ConfigBoolTrue,
				DisableSamlURLCheck:       ConfigBoolFalse,
				Token:                     "t",
			},
			ocspMode: ocspModeFailOpen,
			err:      nil,
		},
		{
			dsn: "u:p@a.snowflake.local:9876?account=a&certRevocationCheckMode=enabled&crlAllowCertificatesWithoutCrlURL=true&crlInMemoryCacheDisabled=true&crlOnDiskCacheDisabled=true&crlHttpClientTimeout=10",
			config: &Config{
				Account: "a", User: "u", Password: "p",
				Host: "a.snowflake.local", Port: 9876,
				Protocol:                          "https",
				OCSPFailOpen:                      OCSPFailOpenTrue,
				ValidateDefaultParameters:         ConfigBoolTrue,
				ClientTimeout:                     defaultClientTimeout,
				JWTClientTimeout:                  defaultJWTClientTimeout,
				ExternalBrowserTimeout:            defaultExternalBrowserTimeout,
				CloudStorageTimeout:               defaultCloudStorageTimeout,
				IncludeRetryReason:                ConfigBoolTrue,
				CertRevocationCheckMode:           CertRevocationCheckEnabled,
				CrlAllowCertificatesWithoutCrlURL: ConfigBoolTrue,
				CrlInMemoryCacheDisabled:          true,
				CrlOnDiskCacheDisabled:            true,
				CrlHTTPClientTimeout:              10 * time.Second,
			},
			ocspMode: ocspModeFailOpen,
		},
		{
			dsn: "user:pass@account/db?tlsConfigName=custom",
			err: &SnowflakeError{
				Number:  ErrCodeMissingTLSConfig,
				Message: fmt.Sprintf(errMsgMissingTLSConfig, "custom"),
			},
		},
	}

	for _, at := range []AuthType{AuthTypeExternalBrowser, AuthTypeOAuth} {
		testcases = append(testcases, tcParseDSN{
			dsn: fmt.Sprintf("@host:777/db/schema?account=ac&protocol=http&authenticator=%v", strings.ToLower(at.String())),
			config: &Config{
				Account: "ac", User: "", Password: "",
				Protocol: "http", Host: "host", Port: 777,
				Database: "db", Schema: "schema",
				OCSPFailOpen:              OCSPFailOpenTrue,
				ValidateDefaultParameters: ConfigBoolTrue,
				ClientTimeout:             defaultClientTimeout,
				JWTClientTimeout:          defaultJWTClientTimeout,
				ExternalBrowserTimeout:    defaultExternalBrowserTimeout,
				CloudStorageTimeout:       defaultCloudStorageTimeout,
				IncludeRetryReason:        ConfigBoolTrue,
				Authenticator:             at,
			},
			ocspMode: ocspModeFailOpen,
			err:      nil,
		})
	}

	for _, at := range []AuthType{AuthTypeSnowflake, AuthTypeUsernamePasswordMFA, AuthTypeJwt} {
		testcases = append(testcases, tcParseDSN{
			dsn: fmt.Sprintf("@host:888/db/schema?account=ac&protocol=http&authenticator=%v", strings.ToLower(at.String())),
			config: &Config{
				Account: "ac", User: "", Password: "",
				Protocol: "http", Host: "host", Port: 888,
				Database: "db", Schema: "schema",
				OCSPFailOpen:              OCSPFailOpenTrue,
				ValidateDefaultParameters: ConfigBoolTrue,
				ClientTimeout:             defaultClientTimeout,
				JWTClientTimeout:          defaultJWTClientTimeout,
				ExternalBrowserTimeout:    defaultExternalBrowserTimeout,
				CloudStorageTimeout:       defaultCloudStorageTimeout,
				IncludeRetryReason:        ConfigBoolTrue,
				Authenticator:             at,
			},
			ocspMode: ocspModeFailOpen,
			err:      errEmptyUsername(),
		})
	}

	for _, at := range []AuthType{AuthTypeSnowflake, AuthTypeUsernamePasswordMFA} {
		testcases = append(testcases, tcParseDSN{
			dsn: fmt.Sprintf("user@host:888/db/schema?account=ac&protocol=http&authenticator=%v", strings.ToLower(at.String())),
			config: &Config{
				Account: "ac", User: "user", Password: "",
				Protocol: "http", Host: "host", Port: 888,
				Database: "db", Schema: "schema",
				OCSPFailOpen:              OCSPFailOpenTrue,
				ValidateDefaultParameters: ConfigBoolTrue,
				ClientTimeout:             defaultClientTimeout,
				JWTClientTimeout:          defaultJWTClientTimeout,
				ExternalBrowserTimeout:    defaultExternalBrowserTimeout,
				CloudStorageTimeout:       defaultCloudStorageTimeout,
				IncludeRetryReason:        ConfigBoolTrue,
				Authenticator:             at,
			},
			ocspMode: ocspModeFailOpen,
			err:      errEmptyPassword(),
		})
	}

	for i, test := range testcases {
		t.Run(maskSecrets(test.dsn), func(t *testing.T) {
			cfg, err := ParseDSN(test.dsn)
			switch {
			case test.err == nil:
				assertNilF(t, err, fmt.Sprintf("%d: Failed to parse the DSN. dsn: %v", i, test.dsn))
				assertEqualE(t, cfg.Host, test.config.Host, fmt.Sprintf("Test %d: Host mismatch", i))
				assertEqualE(t, cfg.Account, test.config.Account, fmt.Sprintf("Test %d: Account mismatch", i))
				assertEqualE(t, cfg.User, test.config.User, fmt.Sprintf("Test %d: User mismatch", i))
				assertEqualE(t, cfg.Password, test.config.Password, fmt.Sprintf("Test %d: Password mismatch", i))
				assertEqualE(t, cfg.Database, test.config.Database, fmt.Sprintf("Test %d: Database mismatch", i))
				assertEqualE(t, cfg.Schema, test.config.Schema, fmt.Sprintf("Test %d: Schema mismatch", i))
				assertEqualE(t, cfg.Warehouse, test.config.Warehouse, fmt.Sprintf("Test %d: Warehouse mismatch", i))
				assertEqualE(t, cfg.Role, test.config.Role, fmt.Sprintf("Test %d: Role mismatch", i))
				assertEqualE(t, cfg.Region, test.config.Region, fmt.Sprintf("Test %d: Region mismatch", i))
				assertEqualE(t, cfg.Protocol, test.config.Protocol, fmt.Sprintf("Test %d: Protocol mismatch", i))
				assertEqualE(t, cfg.Passcode, test.config.Passcode, fmt.Sprintf("Test %d: Passcode mismatch", i))
				assertEqualE(t, cfg.PasscodeInPassword, test.config.PasscodeInPassword, fmt.Sprintf("Test %d: PasscodeInPassword mismatch", i))
				assertEqualE(t, cfg.Authenticator, test.config.Authenticator, fmt.Sprintf("Test %d: Authenticator mismatch", i))
				if test.config.Authenticator == AuthTypeOkta {
					assertEqualE(t, *cfg.OktaURL, *test.config.OktaURL, fmt.Sprintf("Test %d: OktaURL mismatch", i))
				}
				assertEqualE(t, cfg.OCSPFailOpen, test.config.OCSPFailOpen, fmt.Sprintf("Test %d: OCSPFailOpen mismatch", i))
				assertEqualE(t, cfg.ocspMode(), test.ocspMode, fmt.Sprintf("Test %d: OCSPMode mismatch", i))
				assertEqualE(t, cfg.ValidateDefaultParameters, test.config.ValidateDefaultParameters, fmt.Sprintf("Test %d: ValidateDefaultParameters mismatch", i))
				assertEqualE(t, cfg.ClientTimeout, test.config.ClientTimeout, fmt.Sprintf("Test %d: ClientTimeout mismatch", i))
				assertEqualE(t, cfg.JWTClientTimeout, test.config.JWTClientTimeout, fmt.Sprintf("Test %d: JWTClientTimeout mismatch", i))
				assertEqualE(t, cfg.ExternalBrowserTimeout, test.config.ExternalBrowserTimeout, fmt.Sprintf("Test %d: ExternalBrowserTimeout mismatch", i))
				assertEqualE(t, cfg.CloudStorageTimeout, test.config.CloudStorageTimeout, fmt.Sprintf("Test %d: CloudStorageTimeout mismatch", i))
				assertEqualE(t, cfg.TmpDirPath, test.config.TmpDirPath, fmt.Sprintf("Test %d: TmpDirPath mismatch", i))
				assertEqualE(t, cfg.DisableQueryContextCache, test.config.DisableQueryContextCache, fmt.Sprintf("Test %d: DisableQueryContextCache mismatch", i))
				assertEqualE(t, cfg.IncludeRetryReason, test.config.IncludeRetryReason, fmt.Sprintf("Test %d: IncludeRetryReason mismatch", i))
				assertEqualE(t, cfg.DisableConsoleLogin, test.config.DisableConsoleLogin, fmt.Sprintf("Test %d: DisableConsoleLogin mismatch", i))
				assertEqualE(t, cfg.DisableSamlURLCheck, test.config.DisableSamlURLCheck, fmt.Sprintf("Test %d: DisableSamlURLCheck mismatch", i))
				assertEqualE(t, cfg.OauthClientID, test.config.OauthClientID, fmt.Sprintf("Test %d: OauthClientID mismatch", i))
				assertEqualE(t, cfg.OauthClientSecret, test.config.OauthClientSecret, fmt.Sprintf("Test %d: OauthClientSecret mismatch", i))
				assertEqualE(t, cfg.OauthAuthorizationURL, test.config.OauthAuthorizationURL, fmt.Sprintf("Test %d: OauthAuthorizationURL mismatch", i))
				assertEqualE(t, cfg.OauthTokenRequestURL, test.config.OauthTokenRequestURL, fmt.Sprintf("Test %d: OauthTokenRequestURL mismatch", i))
				assertEqualE(t, cfg.OauthRedirectURI, test.config.OauthRedirectURI, fmt.Sprintf("Test %d: OauthRedirectURI mismatch", i))
				assertEqualE(t, cfg.OauthScope, test.config.OauthScope, fmt.Sprintf("Test %d: OauthScope mismatch", i))
				assertEqualE(t, cfg.EnableSingleUseRefreshTokens, test.config.EnableSingleUseRefreshTokens, fmt.Sprintf("Test %d: EnableSingleUseRefreshTokens mismatch", i))
				assertEqualE(t, cfg.Token, test.config.Token, "token")
				assertEqualE(t, cfg.ClientConfigFile, test.config.ClientConfigFile, "client config file")
				assertEqualE(t, cfg.CertRevocationCheckMode, test.config.CertRevocationCheckMode, "cert revocation check mode")
				assertEqualE(t, cfg.CrlAllowCertificatesWithoutCrlURL, test.config.CrlAllowCertificatesWithoutCrlURL, "crl allow certificates without crl url")
				assertEqualE(t, cfg.CrlInMemoryCacheDisabled, test.config.CrlInMemoryCacheDisabled, "crl in memory cache disabled")
				assertEqualE(t, cfg.CrlOnDiskCacheDisabled, test.config.CrlOnDiskCacheDisabled, "crl on disk cache disabled")
				assertEqualE(t, cfg.CrlHTTPClientTimeout, test.config.CrlHTTPClientTimeout, "crl http client timeout")
				assertEqualE(t, cfg.DisableTelemetry, test.config.DisableTelemetry, "disable telemetry")
			case test.err != nil:
				driverErrE, okE := test.err.(*SnowflakeError)
				driverErrG, okG := err.(*SnowflakeError)
				if okE && !okG || !okE && okG {
					t.Fatalf("%d: Wrong error. expected: %v, got: %v", i, test.err, err)
				}
				if okE && okG {
					if driverErrE.Number != driverErrG.Number {
						t.Fatalf("%d: Wrong error number. expected: %v, got: %v", i, driverErrE.Number, driverErrG.Number)
					}
				} else {
					t1 := reflect.TypeOf(err)
					t2 := reflect.TypeOf(test.err)
					if t1 != t2 {
						t.Fatalf("%d: Wrong error. expected: %T:%v, got: %T:%v", i, test.err, test.err, err, err)
					}
				}
			}

		})
	}
}

type tcDSN struct {
	cfg *Config
	dsn string
	err error
}

func TestDSN(t *testing.T) {
	tmfmt := "MM-DD-YYYY"
	testcases := []tcDSN{
		{
			cfg: &Config{
				User:     "u",
				Password: "p",
				Account:  "a-aofnadsf.somewhere.azure",
			},
			dsn: "u:p@a-aofnadsf.somewhere.azure.snowflakecomputing.com:443?ocspFailOpen=true&region=somewhere.azure&validateDefaultParameters=true",
		},
		{
			cfg: &Config{
				User:     "u",
				Password: "p",
				Account:  "a-aofnadsf.global",
			},
			dsn: "u:p@a-aofnadsf.global.snowflakecomputing.com:443?ocspFailOpen=true&region=global&validateDefaultParameters=true",
		},
		{
			cfg: &Config{
				User:     "u",
				Password: "p",
				Account:  "a-aofnadsf.global",
				Region:   "us-west-2",
			},
			dsn: "u:p@a-aofnadsf.global.snowflakecomputing.com:443?ocspFailOpen=true&region=global&validateDefaultParameters=true",
		},
		{
			cfg: &Config{
				User:     "u",
				Password: "p",
				Account:  "account-name",
				Region:   "cn-region",
			},
			dsn: "u:p@account-name.cn-region.snowflakecomputing.cn:443?ocspFailOpen=true&region=cn-region&validateDefaultParameters=true",
		},
		{
			cfg: &Config{
				User:     "u",
				Password: "p",
				Account:  "account-name.cn-region",
			},
			dsn: "u:p@account-name.cn-region.snowflakecomputing.cn:443?ocspFailOpen=true&region=cn-region&validateDefaultParameters=true",
		},
		{
			cfg: &Config{
				User:     "u",
				Password: "p",
				Account:  "account-name.cn-region",
				Host:     "account-name.cn-region.snowflakecomputing.cn",
			},
			dsn: "u:p@account-name.cn-region.snowflakecomputing.cn:443?account=account-name&ocspFailOpen=true&region=cn-region&validateDefaultParameters=true",
		},
		{
			cfg: &Config{
				User:     "u",
				Password: "p",
				Account:  "account.us-west-2",
			},
			dsn: "u:p@account.snowflakecomputing.com:443?ocspFailOpen=true&validateDefaultParameters=true",
		},
		{
			cfg: &Config{
				User:     "u",
				Password: "p",
				Account:  "account_us-west-2",
			},
			dsn: "u:p@account_us-west-2.snowflakecomputing.com:443?ocspFailOpen=true&validateDefaultParameters=true",
		},
		{
			cfg: &Config{
				User:     "u",
				Password: "p",
				Account:  "account-name",
				Host:     "account-name.snowflakecomputing.mil",
			},
			dsn: "u:p@account-name.snowflakecomputing.mil:443?account=account-name&ocspFailOpen=true&validateDefaultParameters=true",
		},
		{
			cfg: &Config{
				User:     "u",
				Password: "p",
				Account:  "account-name",
				Host:     "account-name.snowflakecomputing.gov.pl",
			},
			dsn: "u:p@account-name.snowflakecomputing.gov.pl:443?account=account-name&ocspFailOpen=true&validateDefaultParameters=true",
		},
		{
			cfg: &Config{
				User:     "u",
				Password: "p",
				Account:  "a-aofnadsf.global",
				Region:   "r",
			},
			err: errRegionConflict(),
		},
		{
			cfg: &Config{
				User:     "u",
				Password: "p",
				Account:  "a",
			},
			dsn: "u:p@a.snowflakecomputing.com:443?ocspFailOpen=true&validateDefaultParameters=true",
		},
		{
			cfg: &Config{
				User:     "u",
				Password: "p",
				Account:  "a",
				Region:   "us-west-2",
			},
			dsn: "u:p@a.snowflakecomputing.com:443?ocspFailOpen=true&validateDefaultParameters=true",
		},
		{
			cfg: &Config{
				User:     "u",
				Password: "p",
				Account:  "a",
				Region:   "r",
			},
			dsn: "u:p@a.r.snowflakecomputing.com:443?ocspFailOpen=true&region=r&validateDefaultParameters=true",
		},
		{
			cfg: &Config{
				User:                  "u",
				Password:              "p",
				Account:               "a",
				Region:                "r",
				OauthClientID:         "testClientId",
				OauthClientSecret:     "testClientSecret",
				OauthAuthorizationURL: "http://somehost.com",
				OauthTokenRequestURL:  "https://somehost2.com/somepath",
				OauthRedirectURI:      "http://localhost:8001/some-path",
				OauthScope:            "test scope",
			},
			dsn: "u:p@a.r.snowflakecomputing.com:443?oauthAuthorizationUrl=http%3A%2F%2Fsomehost.com&oauthClientId=testClientId&oauthClientSecret=testClientSecret&oauthRedirectUri=http%3A%2F%2Flocalhost%3A8001%2Fsome-path&oauthScope=test+scope&oauthTokenRequestUrl=https%3A%2F%2Fsomehost2.com%2Fsomepath&ocspFailOpen=true&region=r&validateDefaultParameters=true",
		},
		{
			cfg: &Config{
				User:                         "u",
				Password:                     "p",
				Account:                      "a",
				Region:                       "r",
				OauthClientID:                "testClientId",
				OauthClientSecret:            "testClientSecret",
				OauthAuthorizationURL:        "http://somehost.com",
				OauthTokenRequestURL:         "https://somehost2.com/somepath",
				OauthRedirectURI:             "http://localhost:8001/some-path",
				OauthScope:                   "test scope",
				EnableSingleUseRefreshTokens: true,
			},
			dsn: "u:p@a.r.snowflakecomputing.com:443?enableSingleUseRefreshTokens=true&oauthAuthorizationUrl=http%3A%2F%2Fsomehost.com&oauthClientId=testClientId&oauthClientSecret=testClientSecret&oauthRedirectUri=http%3A%2F%2Flocalhost%3A8001%2Fsome-path&oauthScope=test+scope&oauthTokenRequestUrl=https%3A%2F%2Fsomehost2.com%2Fsomepath&ocspFailOpen=true&region=r&validateDefaultParameters=true",
		},
		{
			cfg: &Config{
				User:                   "u",
				Password:               "p",
				Account:                "a",
				Region:                 "r",
				ExternalBrowserTimeout: 20 * time.Second,
				CloudStorageTimeout:    7 * time.Second,
			},
			dsn: "u:p@a.r.snowflakecomputing.com:443?cloudStorageTimeout=7&externalBrowserTimeout=20&ocspFailOpen=true&region=r&validateDefaultParameters=true",
		},
		{
			cfg: &Config{
				User:     "",
				Password: "p",
				Account:  "a",
			},
			err: errEmptyUsername(),
		},
		{
			cfg: &Config{
				User:     "u",
				Password: "",
				Account:  "a",
			},
			err: errEmptyPassword(),
		},
		{
			cfg: &Config{
				User:     "u",
				Password: "p",
				Account:  "",
			},
			err: errEmptyAccount(),
		},
		{
			cfg: &Config{
				User:          "u",
				Password:      "p",
				Account:       "ac",
				Authenticator: AuthTypeOAuthAuthorizationCode,
			},
			err: errEmptyOAuthParameters(),
		},
		{
			cfg: &Config{
				User:     "u",
				Password: "p",
				Account:  "a.e",
			},
			dsn: "u:p@a.e.snowflakecomputing.com:443?ocspFailOpen=true&region=e&validateDefaultParameters=true",
		},
		{
			cfg: &Config{
				User:     "u",
				Password: "p",
				Account:  "a.e",
				Region:   "us-west-2",
			},
			dsn: "u:p@a.e.snowflakecomputing.com:443?ocspFailOpen=true&region=e&validateDefaultParameters=true",
		},
		{
			cfg: &Config{
				User:     "u",
				Password: "p",
				Account:  "a.e",
				Region:   "r",
			},
			err: errRegionConflict(),
		},
		{
			cfg: &Config{
				User:               "u",
				Password:           "p",
				Account:            "a",
				Database:           "db",
				Schema:             "sc",
				Role:               "ro",
				Region:             "b",
				Authenticator:      AuthTypeSnowflake,
				Passcode:           "db",
				PasscodeInPassword: true,
				LoginTimeout:       10 * time.Second,
				RequestTimeout:     300 * time.Second,
				Application:        "special go",
			},
			dsn: "u:p@a.b.snowflakecomputing.com:443?application=special+go&database=db&loginTimeout=10&ocspFailOpen=true&passcode=db&passcodeInPassword=true&region=b&requestTimeout=300&role=ro&schema=sc&validateDefaultParameters=true",
		},
		{
			cfg: &Config{
				Account:                       "ac",
				User:                          "u",
				Password:                      "p",
				Database:                      "db",
				Authenticator:                 AuthTypeWorkloadIdentityFederation,
				Host:                          "ac.snowflakecomputing.com",
				WorkloadIdentityProvider:      "azure",
				WorkloadIdentityEntraResource: "https://example.com/default",
			},
			dsn: "u:p@ac.snowflakecomputing.com:443?account=ac&authenticator=workload_identity&database=db&ocspFailOpen=true&validateDefaultParameters=true&workloadIdentityEntraResource=https%3A%2F%2Fexample.com%2Fdefault&workloadIdentityProvider=azure",
		},
		{
			cfg: &Config{
				User:                           "u",
				Password:                       "p",
				Account:                        "a",
				Authenticator:                  AuthTypeExternalBrowser,
				ClientStoreTemporaryCredential: ConfigBoolTrue,
			},
			dsn: "u:p@a.snowflakecomputing.com:443?authenticator=externalbrowser&clientStoreTemporaryCredential=true&ocspFailOpen=true&validateDefaultParameters=true",
		},
		{
			cfg: &Config{
				User:                           "u",
				Password:                       "p",
				Account:                        "a",
				Authenticator:                  AuthTypeExternalBrowser,
				ClientStoreTemporaryCredential: ConfigBoolFalse,
			},
			dsn: "u:p@a.snowflakecomputing.com:443?authenticator=externalbrowser&clientStoreTemporaryCredential=false&ocspFailOpen=true&validateDefaultParameters=true",
		},
		{
			cfg: &Config{
				User:                           "u",
				Password:                       "p",
				Account:                        "a",
				Token:                          "t",
				Authenticator:                  AuthTypePat,
				ClientStoreTemporaryCredential: ConfigBoolFalse,
			},
			dsn: "u:p@a.snowflakecomputing.com:443?authenticator=programmatic_access_token&clientStoreTemporaryCredential=false&ocspFailOpen=true&token=t&validateDefaultParameters=true",
		},
		{
			cfg: &Config{
				User:                           "u",
				Password:                       "p",
				Account:                        "a",
				Authenticator:                  AuthTypeOAuthAuthorizationCode,
				OauthClientID:                  "testClientId",
				OauthClientSecret:              "testClientSecret",
				ClientStoreTemporaryCredential: ConfigBoolFalse,
			},
			dsn: "u:p@a.snowflakecomputing.com:443?authenticator=oauth_authorization_code&clientStoreTemporaryCredential=false&oauthClientId=testClientId&oauthClientSecret=testClientSecret&ocspFailOpen=true&validateDefaultParameters=true",
		},
		{
			cfg: &Config{
				User:                           "u",
				Password:                       "p",
				Account:                        "a",
				Authenticator:                  AuthTypeOAuthClientCredentials,
				ClientStoreTemporaryCredential: ConfigBoolFalse,
			},
			dsn: "u:p@a.snowflakecomputing.com:443?authenticator=oauth_client_credentials&clientStoreTemporaryCredential=false&ocspFailOpen=true&validateDefaultParameters=true",
		},
		{
			cfg: &Config{
				User:          "u",
				Password:      "p",
				Account:       "a",
				Authenticator: AuthTypeOkta,
				OktaURL: &url.URL{
					Scheme: "https",
					Host:   "sc.okta.com",
				},
			},
			dsn: "u:p@a.snowflakecomputing.com:443?authenticator=https%3A%2F%2Fsc.okta.com&ocspFailOpen=true&validateDefaultParameters=true",
		},
		{
			cfg: &Config{
				User:     "u",
				Password: "p",
				Account:  "a.e",
				Params: map[string]*string{
					"TIMESTAMP_OUTPUT_FORMAT": &tmfmt,
				},
			},
			dsn: "u:p@a.e.snowflakecomputing.com:443?TIMESTAMP_OUTPUT_FORMAT=MM-DD-YYYY&ocspFailOpen=true&region=e&validateDefaultParameters=true",
		},
		{
			cfg: &Config{
				User:     "u",
				Password: ":@abc",
				Account:  "a.e",
				Params: map[string]*string{
					"TIMESTAMP_OUTPUT_FORMAT": &tmfmt,
				},
			},
			dsn: "u:%3A%40abc@a.e.snowflakecomputing.com:443?TIMESTAMP_OUTPUT_FORMAT=MM-DD-YYYY&ocspFailOpen=true&region=e&validateDefaultParameters=true",
		},
		{
			cfg: &Config{
				User:         "u",
				Password:     "p",
				Account:      "a",
				OCSPFailOpen: OCSPFailOpenTrue,
			},
			dsn: "u:p@a.snowflakecomputing.com:443?ocspFailOpen=true&validateDefaultParameters=true",
		},
		{
			cfg: &Config{
				User:         "u",
				Password:     "p",
				Account:      "a",
				OCSPFailOpen: OCSPFailOpenFalse,
			},
			dsn: "u:p@a.snowflakecomputing.com:443?ocspFailOpen=false&validateDefaultParameters=true",
		},
		{
			cfg: &Config{
				User:                      "u",
				Password:                  "p",
				Account:                   "a",
				ValidateDefaultParameters: ConfigBoolFalse,
			},
			dsn: "u:p@a.snowflakecomputing.com:443?ocspFailOpen=true&validateDefaultParameters=false",
		},
		{
			cfg: &Config{
				User:                      "u",
				Password:                  "p",
				Account:                   "a",
				ValidateDefaultParameters: ConfigBoolTrue,
			},
			dsn: "u:p@a.snowflakecomputing.com:443?ocspFailOpen=true&validateDefaultParameters=true",
		},
		{
			cfg: &Config{
				User:         "u",
				Password:     "p",
				Account:      "a",
				InsecureMode: true,
			},
			dsn: "u:p@a.snowflakecomputing.com:443?insecureMode=true&ocspFailOpen=true&validateDefaultParameters=true",
		},
		{
			cfg: &Config{
				User:              "u",
				Password:          "p",
				Account:           "a",
				DisableOCSPChecks: true,
			},
			dsn: "u:p@a.snowflakecomputing.com:443?disableOCSPChecks=true&ocspFailOpen=true&validateDefaultParameters=true",
		},
		{
			cfg: &Config{
				User:              "u",
				Password:          "p",
				Account:           "a",
				InsecureMode:      true,
				DisableOCSPChecks: false,
			},
			dsn: "u:p@a.snowflakecomputing.com:443?insecureMode=true&ocspFailOpen=true&validateDefaultParameters=true",
		},
		{
			cfg: &Config{
				User:              "u",
				Password:          "p",
				Account:           "a",
				InsecureMode:      false,
				DisableOCSPChecks: true,
			},
			dsn: "u:p@a.snowflakecomputing.com:443?disableOCSPChecks=true&ocspFailOpen=true&validateDefaultParameters=true",
		},
		{
			cfg: &Config{
				User:                         "u",
				Password:                     "p",
				Account:                      "a",
				DisableOCSPChecks:            true,
				ConnectionDiagnosticsEnabled: true,
			},
			dsn: "u:p@a.snowflakecomputing.com:443?connectionDiagnosticsEnabled=true&disableOCSPChecks=true&ocspFailOpen=true&validateDefaultParameters=true",
		},
		{
			cfg: &Config{
				User:     "u",
				Password: "p",
				Account:  "a.b.c",
			},
			dsn: "u:p@a.b.c.snowflakecomputing.com:443?ocspFailOpen=true&region=b.c&validateDefaultParameters=true",
		},
		{
			cfg: &Config{
				User:     "u",
				Password: "p",
				Account:  "account.snowflakecomputing.com",
			},
			dsn: "u:p@account.snowflakecomputing.com.snowflakecomputing.com:443?ocspFailOpen=true&region=snowflakecomputing.com&validateDefaultParameters=true",
		},
		{
			cfg: &Config{
				User:     "u",
				Password: "p",
				Account:  "a.b.c",
				Region:   "us-west-2",
			},
			dsn: "u:p@a.b.c.snowflakecomputing.com:443?ocspFailOpen=true&region=b.c&validateDefaultParameters=true",
		},
		{
			cfg: &Config{
				User:     "u",
				Password: "p",
				Account:  "a.b.c",
				Region:   "r",
			},
			err: errRegionConflict(),
		},
		{
			cfg: &Config{
				User:             "u",
				Password:         "p",
				Account:          "a.b.c",
				ClientTimeout:    400 * time.Second,
				JWTClientTimeout: 60 * time.Second,
			},
			dsn: "u:p@a.b.c.snowflakecomputing.com:443?clientTimeout=400&jwtClientTimeout=60&ocspFailOpen=true&region=b.c&validateDefaultParameters=true",
		},
		{
			cfg: &Config{
				User:             "u",
				Password:         "p",
				Account:          "a.b.c",
				ClientTimeout:    400 * time.Second,
				JWTExpireTimeout: 30 * time.Second,
			},
			dsn: "u:p@a.b.c.snowflakecomputing.com:443?clientTimeout=400&jwtTimeout=30&ocspFailOpen=true&region=b.c&validateDefaultParameters=true",
		},
		{
			cfg: &Config{
				User:     "u",
				Password: "p",
				Account:  "a.b.c",
				Protocol: "http",
			},
			dsn: "u:p@a.b.c.snowflakecomputing.com:443?ocspFailOpen=true&protocol=http&region=b.c&validateDefaultParameters=true",
		},
		{
			cfg: &Config{
				User:     "u",
				Password: "p",
				Account:  "a.b.c",
				Tracing:  "debug",
			},
			dsn: "u:p@a.b.c.snowflakecomputing.com:443?ocspFailOpen=true&region=b.c&tracing=debug&validateDefaultParameters=true",
		},
		{
			cfg: &Config{
				User:                  "u",
				Password:              "p",
				Account:               "a.b.c",
				Authenticator:         AuthTypeUsernamePasswordMFA,
				ClientRequestMfaToken: ConfigBoolTrue,
			},
			dsn: "u:p@a.b.c.snowflakecomputing.com:443?authenticator=username_password_mfa&clientRequestMfaToken=true&ocspFailOpen=true&region=b.c&validateDefaultParameters=true",
		},
		{
			cfg: &Config{
				User:                  "u",
				Password:              "p",
				Account:               "a.b.c",
				Authenticator:         AuthTypeUsernamePasswordMFA,
				ClientRequestMfaToken: ConfigBoolFalse,
			},
			dsn: "u:p@a.b.c.snowflakecomputing.com:443?authenticator=username_password_mfa&clientRequestMfaToken=false&ocspFailOpen=true&region=b.c&validateDefaultParameters=true",
		},
		{
			cfg: &Config{
				User:      "u",
				Password:  "p",
				Account:   "a.b.c",
				Warehouse: "wh",
			},
			dsn: "u:p@a.b.c.snowflakecomputing.com:443?ocspFailOpen=true&region=b.c&validateDefaultParameters=true&warehouse=wh",
		},
		{
			cfg: &Config{
				User:     "u",
				Password: "p",
				Account:  "a.b.c",
				Token:    "t",
			},
			dsn: "u:p@a.b.c.snowflakecomputing.com:443?ocspFailOpen=true&region=b.c&token=t&validateDefaultParameters=true",
		},
		{
			cfg: &Config{
				User:          "u",
				Password:      "p",
				Account:       "a.b.c",
				Authenticator: AuthTypeTokenAccessor,
			},
			dsn: "u:p@a.b.c.snowflakecomputing.com:443?authenticator=tokenaccessor&ocspFailOpen=true&region=b.c&validateDefaultParameters=true",
		},
		{
			cfg: &Config{
				User:       "u",
				Password:   "p",
				Account:    "a.b.c",
				TmpDirPath: "/tmp",
			},
			dsn: "u:p@a.b.c.snowflakecomputing.com:443?ocspFailOpen=true&region=b.c&tmpDirPath=%2Ftmp&validateDefaultParameters=true",
		},
		{
			cfg: &Config{
				User:               "u",
				Password:           "p",
				Account:            "a.b.c",
				IncludeRetryReason: ConfigBoolFalse,
				MaxRetryCount:      30,
			},
			dsn: "u:p@a.b.c.snowflakecomputing.com:443?includeRetryReason=false&maxRetryCount=30&ocspFailOpen=true&region=b.c&validateDefaultParameters=true",
		},
		{
			cfg: &Config{
				User:                     "u",
				Password:                 "p",
				Account:                  "a.b.c",
				DisableQueryContextCache: true,
				IncludeRetryReason:       ConfigBoolTrue,
			},
			dsn: "u:p@a.b.c.snowflakecomputing.com:443?disableQueryContextCache=true&ocspFailOpen=true&region=b.c&validateDefaultParameters=true",
		},
		{
			cfg: &Config{
				User:               "u",
				Password:           "p",
				Account:            "a.b.c",
				IncludeRetryReason: ConfigBoolFalse,
			},
			dsn: "u:p@a.b.c.snowflakecomputing.com:443?includeRetryReason=false&ocspFailOpen=true&region=b.c&validateDefaultParameters=true",
		},
		{
			cfg: &Config{
				User:               "u",
				Password:           "p",
				Account:            "a.b.c",
				IncludeRetryReason: ConfigBoolTrue,
			},
			dsn: "u:p@a.b.c.snowflakecomputing.com:443?ocspFailOpen=true&region=b.c&validateDefaultParameters=true",
		},
		{
			cfg: &Config{
				User:               "u",
				Password:           "p",
				Account:            "a.b.c",
				IncludeRetryReason: ConfigBoolTrue,
				ClientConfigFile:   "/Users/user/config.json",
			},
			dsn: "u:p@a.b.c.snowflakecomputing.com:443?clientConfigFile=%2FUsers%2Fuser%2Fconfig.json&ocspFailOpen=true&region=b.c&validateDefaultParameters=true",
		},
		{
			cfg: &Config{
				User:               "u",
				Password:           "p",
				Account:            "a.b.c",
				IncludeRetryReason: ConfigBoolTrue,
				ClientConfigFile:   "c:\\Users\\user\\config.json",
			},
			dsn: "u:p@a.b.c.snowflakecomputing.com:443?clientConfigFile=c%3A%5CUsers%5Cuser%5Cconfig.json&ocspFailOpen=true&region=b.c&validateDefaultParameters=true",
		},
		{
			cfg: &Config{
				User:                "u",
				Password:            "p",
				Account:             "a.b.c",
				Authenticator:       AuthTypeExternalBrowser,
				DisableConsoleLogin: ConfigBoolTrue,
			},
			dsn: "u:p@a.b.c.snowflakecomputing.com:443?authenticator=externalbrowser&disableConsoleLogin=true&ocspFailOpen=true&region=b.c&validateDefaultParameters=true",
		},
		{
			cfg: &Config{
				User:                "u",
				Password:            "p",
				Account:             "a.b.c",
				Authenticator:       AuthTypeExternalBrowser,
				DisableConsoleLogin: ConfigBoolFalse,
			},
			dsn: "u:p@a.b.c.snowflakecomputing.com:443?authenticator=externalbrowser&disableConsoleLogin=false&ocspFailOpen=true&region=b.c&validateDefaultParameters=true",
		},
		{
			cfg: &Config{
				User:                "u",
				Password:            "p",
				Account:             "a.b.c",
				Authenticator:       AuthTypeExternalBrowser,
				DisableSamlURLCheck: ConfigBoolTrue,
			},
			dsn: "u:p@a.b.c.snowflakecomputing.com:443?authenticator=externalbrowser&disableSamlURLCheck=true&ocspFailOpen=true&region=b.c&validateDefaultParameters=true",
		},
		{
			cfg: &Config{
				User:                "u",
				Password:            "p",
				Account:             "a.b.c",
				Authenticator:       AuthTypeExternalBrowser,
				DisableSamlURLCheck: ConfigBoolFalse,
			},
			dsn: "u:p@a.b.c.snowflakecomputing.com:443?authenticator=externalbrowser&disableSamlURLCheck=false&ocspFailOpen=true&region=b.c&validateDefaultParameters=true",
		},
		{
			cfg: &Config{
				User:                              "u",
				Password:                          "p",
				Account:                           "a.b.c",
				CertRevocationCheckMode:           CertRevocationCheckEnabled,
				CrlAllowCertificatesWithoutCrlURL: ConfigBoolTrue,
				CrlInMemoryCacheDisabled:          true,
				CrlOnDiskCacheDisabled:            true,
				CrlHTTPClientTimeout:              5 * time.Second,
			},
			dsn: "u:p@a.b.c.snowflakecomputing.com:443?certRevocationCheckMode=ENABLED&crlAllowCertificatesWithoutCrlURL=true&crlHttpClientTimeout=5&crlInMemoryCacheDisabled=true&crlOnDiskCacheDisabled=true&ocspFailOpen=true&region=b.c&validateDefaultParameters=true",
		},
		{
			cfg: &Config{
				User:          "u",
				Password:      "p",
				Account:       "a.b.c",
				TLSConfigName: "custom",
			},
			dsn: "u:p@a.b.c.snowflakecomputing.com:443?ocspFailOpen=true&region=b.c&tlsConfigName=custom&validateDefaultParameters=true",
		},
	}
	for _, test := range testcases {
		t.Run(maskSecrets(test.dsn), func(t *testing.T) {
			if test.cfg.TLSConfigName != "" && test.err == nil {
				err := RegisterTLSConfig(test.cfg.TLSConfigName, &tls.Config{})
				assertNilF(t, err, "Failed to register test TLS config")
				defer func() {
					_ = DeregisterTLSConfig(test.cfg.TLSConfigName)
				}()
			}
			dsn, err := DSN(test.cfg)
			if test.err == nil && err == nil {
				if dsn != test.dsn {
					assertEqualF(t, "failed to get DSN. expected: %v, got:\n %v", maskSecrets(test.dsn), maskSecrets(dsn))
				}
				_, err := ParseDSN(dsn)
				assertNilF(t, err, "failed to parse DSN. dsn:", dsn)
			}
			if test.err != nil {
				assertNotNilF(t, err, fmt.Sprintf("expected error. dsn: %v, expected err: %v", maskSecrets(test.dsn), maskSecrets(test.err.Error())))
			}
			if test.err == nil {
				assertNilF(t, err, "failed to match")
			}
		})
	}
}

func TestParsePrivateKeyFromFileMissingFile(t *testing.T) {
	_, err := parsePrivateKeyFromFile("nonexistent")

	if err == nil {
		t.Error("should report error for nonexistent file")
	}
}

func TestParsePrivateKeyFromFileIncorrectData(t *testing.T) {
	pemFile := createTmpFile(t, "exampleKey.pem", []byte("gibberish"))
	_, err := parsePrivateKeyFromFile(pemFile)

	if err == nil {
		t.Error("should report error for wrong data in file")
	}
}

func TestParsePrivateKeyFromFileNotRSAPrivateKey(t *testing.T) {
	// Generate an ECDSA private key for testing
	ecdsaPrivateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate ECDSA private key: %v", err)
	}

	ecdsaPrivateKeyBytes, err := x509.MarshalECPrivateKey(ecdsaPrivateKey)
	if err != nil {
		t.Fatalf("failed to marshal ECDSA private key: %v", err)
	}
	pemBlock := &pem.Block{
		Type:  "EC PRIVATE KEY",
		Bytes: ecdsaPrivateKeyBytes,
	}
	pemData := pem.EncodeToMemory(pemBlock)

	// Write the PEM data to a temporary file
	pemFile := createTmpFile(t, "ecdsaKey.pem", pemData)

	// Attempt to parse the private key
	_, err = parsePrivateKeyFromFile(pemFile)
	if err == nil {
		t.Error("expected an error when trying to parse an ECDSA private key as RSA")
	}
}

func TestParsePrivateKeyFromFile(t *testing.T) {
	generatedKey, _ := rsa.GenerateKey(cr.Reader, 1024)
	pemKey, _ := x509.MarshalPKCS8PrivateKey(generatedKey)
	pemData := pem.EncodeToMemory(
		&pem.Block{
			Type:  "RSA PRIVATE KEY",
			Bytes: pemKey,
		},
	)
	keyFile := createTmpFile(t, "exampleKey.pem", pemData)
	defer os.Remove(keyFile)

	parsedKey, err := parsePrivateKeyFromFile(keyFile)
	if err != nil {
		t.Errorf("unable to parse pam file from path: %v, err: %v", keyFile, err)
	} else if !parsedKey.Equal(generatedKey) {
		t.Errorf("generated key does not equal to parsed key from file\ngeneratedKey=%v\nparsedKey=%v",
			generatedKey, parsedKey)
	}
}

func createTmpFile(t *testing.T, fileName string, content []byte) string {
	tempFile, _ := os.CreateTemp("", fileName)
	_, err := tempFile.Write(content)
	assertNilF(t, err)
	absolutePath := tempFile.Name()
	return absolutePath
}

type configParamToValue struct {
	configParam string
	value       string
}

func TestGetConfigFromEnv(t *testing.T) {
	envMap := map[string]configParamToValue{
		"SF_TEST_ACCOUNT":     {"Account", "account"},
		"SF_TEST_USER":        {"User", "user"},
		"SF_TEST_PASSWORD":    {"Password", "password"},
		"SF_TEST_ROLE":        {"Role", "role"},
		"SF_TEST_HOST":        {"Host", "host"},
		"SF_TEST_PORT":        {"Port", "8080"},
		"SF_TEST_PROTOCOL":    {"Protocol", "http"},
		"SF_TEST_WAREHOUSE":   {"Warehouse", "warehouse"},
		"SF_TEST_DATABASE":    {"Database", "database"},
		"SF_TEST_REGION":      {"Region", "region"},
		"SF_TEST_PASSCODE":    {"Passcode", "passcode"},
		"SF_TEST_SCHEMA":      {"Schema", "schema"},
		"SF_TEST_APPLICATION": {"Application", "application"},
	}
	var properties = make([]*ConfigParam, len(envMap))
	i := 0
	for key, ctv := range envMap {
		os.Setenv(key, ctv.value)
		cfgParam := ConfigParam{ctv.configParam, key, true}
		properties[i] = &cfgParam
		i++
	}
	defer func() {
		for key := range envMap {
			os.Unsetenv(key)
		}
	}()

	cfg, err := GetConfigFromEnv(properties)
	if err != nil {
		t.Errorf("unable to parse env variables to Config, err: %v", err)
	}

	err = checkConfig(*cfg, envMap)
	if err != nil {
		t.Error(err)
	}
}

func checkConfig(cfg Config, envMap map[string]configParamToValue) error {
	appendError := func(errArray []string, envName string, expected string, received string) []string {
		errArray = append(errArray, fmt.Sprintf("field %v expected value: %v, received value: %v", envName, expected, received))
		return errArray
	}

	value := reflect.ValueOf(cfg)
	typeOfCfg := value.Type()
	cfgValues := make(map[string]interface{}, value.NumField())
	for i := 0; i < value.NumField(); i++ {
		if value.Field(i).CanInterface() {
			cfgValues[typeOfCfg.Field(i).Name] = value.Field(i).Interface()
		}
	}

	var errArray []string
	for key, ctv := range envMap {
		if ctv.configParam == "Port" {
			if portStr := strconv.Itoa(cfgValues[ctv.configParam].(int)); portStr != ctv.value {
				errArray = appendError(errArray, key, ctv.value, cfgValues[ctv.configParam].(string))
			}
		} else if cfgValues[ctv.configParam] != ctv.value {
			errArray = appendError(errArray, key, ctv.value, cfgValues[ctv.configParam].(string))
		}
	}

	if errArray != nil {
		return errors.New(strings.Join(errArray, "\n"))
	}

	return nil
}

func TestConfigValidateTmpDirPath(t *testing.T) {
	cfg := &Config{
		TmpDirPath: "/not/existing",
	}
	if err := cfg.Validate(); err == nil {
		t.Fatalf("Should fail on not existing TmpDirPath")
	}
}

func TestExtractAccountName(t *testing.T) {
	testcases := map[string]string{
		"myaccount":                          "MYACCOUNT",
		"myaccount.eu-central-1":             "MYACCOUNT",
		"myaccount.eu-central-1.privatelink": "MYACCOUNT",
		"myorg-myaccount":                    "MYORG-MYACCOUNT",
		"myorg-myaccount.privatelink":        "MYORG-MYACCOUNT",
		"myorg-my-account":                   "MYORG-MY-ACCOUNT",
		"myorg-my-account.privatelink":       "MYORG-MY-ACCOUNT",
		"myorg-my_account":                   "MYORG-MY_ACCOUNT",
		"myorg-my_account.privatelink":       "MYORG-MY_ACCOUNT",
	}

	for account, expected := range testcases {
		t.Run(account, func(t *testing.T) {
			accountPart := extractAccountName(account)
			if accountPart != expected {
				t.Fatalf("extractAccountName returned unexpected response (%v), should be %v", accountPart, expected)
			}
		})
	}
}

func TestUrlDecodeIfNeeded(t *testing.T) {
	testcases := map[string]string{
		"query_tag":             "query_tag",
		"%24my_custom_variable": "$my_custom_variable",
	}
	for param, expected := range testcases {
		t.Run(param, func(t *testing.T) {
			decodedParam := urlDecodeIfNeeded(param)
			assertEqualE(t, decodedParam, expected)
		})
	}
}

func TestUrlDecodeIfNeededE2E(t *testing.T) {
	// Skip this test when using JWT authentication globally to prevent unexpected behavior
	if os.Getenv("SNOWFLAKE_TEST_AUTHENTICATOR") == "SNOWFLAKE_JWT" {
		t.Skip("Skipping URL decode test when JWT is configured globally")
	}
	customVarName := "CUSTOM_VARIABLE"
	customVarValue := "test"
	myQueryTag := "mytag"
	testPort, err := strconv.Atoi(os.Getenv("SNOWFLAKE_TEST_PORT"))
	if err != nil {
		testPort = 443
	}
	cfg := &Config{
		Account:       os.Getenv("SNOWFLAKE_TEST_ACCOUNT"),
		Host:          os.Getenv("SNOWFLAKE_TEST_HOST"),
		Port:          testPort,
		Protocol:      os.Getenv("SNOWFLAKE_TEST_PROTOCOL"),
		User:          os.Getenv("SNOWFLAKE_TEST_USER"),
		Password:      os.Getenv("SNOWFLAKE_TEST_PASSWORD"),
		Authenticator: AuthTypeSnowflake, // Force password authentication
		PrivateKey:    nil,               // Ensure no private key
		Params:        map[string]*string{"$" + customVarName: &customVarValue, "query_tag": &myQueryTag},
	}
	mydsn, err := DSN(cfg)
	assertNilE(t, err, "TestUrlDecodeIfNeededE2E failed to create DSN from Config")
	db, err := sql.Open("snowflake", mydsn)
	assertNilE(t, err, "TestUrlDecodeIfNeededE2E failed to connect.")
	defer db.Close()
	query := "SHOW VARIABLES;"
	rows, err := db.Query(query)
	assertNilE(t, err, "TestUrlDecodeIfNeededE2E failed to run SHOW VARIABLES query.")
	defer rows.Close()
	var v1, v2, v3, v4, v5, v6, v7 any
	assertTrueE(t, rows.Next(), "TestUrlDecodeIfNeededE2E query run but no rows were returned.")
	err = rows.Scan(&v1, &v2, &v3, &v4, &v5, &v6, &v7)
	assertNilE(t, err, "TestUrlDecodeIfNeededE2E failed to get result.")
	assertDeepEqualE(t, v4, customVarName, "TestUrlDecodeIfNeededE2E variable name retrieved from the test did not match")
	assertDeepEqualE(t, v5, customVarValue, "TestUrlDecodeIfNeededE2E variable value retrieved from the test did not match")
	assertNilE(t, rows.Err(), "TestUrlDecodeIfNeededE2E ERROR getting rows.")
}
func TestDSNParsingWithTLSConfig(t *testing.T) {
	// Clean up any existing registry
	tlsConfigLock.Lock()
	tlsConfigRegistry = make(map[string]*tls.Config)
	tlsConfigLock.Unlock()

	// Register test TLS config
	testTLSConfig := tls.Config{
		InsecureSkipVerify: true,
		ServerName:         "custom.test.com",
	}
	err := RegisterTLSConfig("custom", &testTLSConfig)
	assertNilF(t, err, "Failed to register test TLS config")
	defer func() {
		err := DeregisterTLSConfig("custom")
		assertNilF(t, err, "Failed to deregister test TLS config")
	}()

	testCases := []struct {
		name     string
		dsn      string
		expected string
		err      bool
	}{
		{
			name:     "Basic TLS config parameter",
			dsn:      "user:pass@account/db?tlsConfigName=custom",
			expected: "custom",
			err:      false,
		},
		{
			name:     "TLS config with other parameters",
			dsn:      "user:pass@account/db?tlsConfigName=custom&warehouse=wh&role=admin",
			expected: "custom",
			err:      false,
		},
		{
			name: "No TLS config parameter",
			dsn:  "user:pass@account/db?warehouse=wh",
			err:  false,
		},
		{
			name: "Nonexistent TLS config",
			dsn:  "user:pass@account/db?tlsConfigName=nonexistent",
			err:  true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := ParseDSN(tc.dsn)
			if tc.err {
				assertNotNilF(t, err, "ParseDSN should have failed but did not")
			} else {
				assertNilF(t, err, "ParseDSN failed")
				// For DSN parsing, the TLS config should be resolved and set directly
				assertEqualF(t, cfg.TLSConfigName, tc.expected, "TLSConfigName mismatch")
			}

		})
	}
}
