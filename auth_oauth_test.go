package gosnowflake

import (
	"context"
	"database/sql"
	"errors"
	"golang.org/x/oauth2"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"
)

func TestUnitOAuthAuthorizationCode(t *testing.T) {
	skipOnMac(t, "keychain requires password")
	roundTripper := newCountingRoundTripper(snowflakeNoOcspTransport)
	httpClient := &http.Client{
		Transport: roundTripper,
	}
	cfg := &Config{
		User:                           "testUser",
		Role:                           "ANALYST",
		OauthClientID:                  "testClientId",
		OauthClientSecret:              "testClientSecret",
		OauthAuthorizationURL:          wiremock.baseURL() + "/oauth/authorize",
		OauthTokenRequestURL:           wiremock.baseURL() + "/oauth/token",
		OauthRedirectURI:               "http://localhost:1234/snowflake/oauth-redirect",
		Transporter:                    roundTripper,
		ClientStoreTemporaryCredential: ConfigBoolTrue,
		ExternalBrowserTimeout:         defaultExternalBrowserTimeout,
	}
	client, err := newOauthClient(context.WithValue(context.Background(), oauth2.HTTPClient, httpClient), cfg)
	assertNilF(t, err)
	cacheTokenSpec := newOAuthAccessTokenSpec(wiremock.connectionConfig().OauthTokenRequestURL, wiremock.connectionConfig().User)

	t.Run("Success", func(t *testing.T) {
		credentialsStorage.deleteCredential(cacheTokenSpec)
		wiremock.registerMappings(t, newWiremockMapping("oauth2/authorization_code/successful_flow.json"))
		authCodeProvider := &nonInteractiveAuthorizationCodeProvider{}
		client.authorizationCodeProviderFactory = func() authorizationCodeProvider {
			return authCodeProvider
		}
		token, err := client.authenticateByOAuthAuthorizationCode()
		assertNilF(t, err)
		assertEqualE(t, token, "access-token-123")
		time.Sleep(100 * time.Millisecond)
		authCodeProvider.assertResponseBodyContains("OAuth authentication completed successfully.")
	})

	t.Run("Store access token in cache", func(t *testing.T) {
		roundTripper.reset()
		credentialsStorage.deleteCredential(cacheTokenSpec)
		wiremock.registerMappings(t, newWiremockMapping("oauth2/authorization_code/successful_flow.json"))
		authCodeProvider := &nonInteractiveAuthorizationCodeProvider{}
		client.authorizationCodeProviderFactory = func() authorizationCodeProvider {
			return authCodeProvider
		}
		_, err = client.authenticateByOAuthAuthorizationCode()
		assertNilF(t, err)
		assertEqualE(t, credentialsStorage.getCredential(cacheTokenSpec), "access-token-123")
	})

	t.Run("Use cache for consecutive calls", func(t *testing.T) {
		roundTripper.reset()
		credentialsStorage.setCredential(cacheTokenSpec, "access-token-123")
		wiremock.registerMappings(t, newWiremockMapping("oauth2/authorization_code/successful_flow.json"))
		authCodeProvider := &nonInteractiveAuthorizationCodeProvider{}
		for i := 0; i < 3; i++ {
			client, err := newOauthClient(context.WithValue(context.Background(), oauth2.HTTPClient, httpClient), cfg)
			assertNilF(t, err)
			client.authorizationCodeProviderFactory = func() authorizationCodeProvider {
				return authCodeProvider
			}
			_, err = client.authenticateByOAuthAuthorizationCode()
			assertNilF(t, err)
		}
		assertEqualE(t, authCodeProvider.responseBody, "")
		assertEqualE(t, roundTripper.postReqCount[cfg.OauthTokenRequestURL], 0)
	})

	t.Run("InvalidState", func(t *testing.T) {
		credentialsStorage.deleteCredential(cacheTokenSpec)
		wiremock.registerMappings(t, newWiremockMapping("oauth2/authorization_code/successful_flow.json"))
		authCodeProvider := &nonInteractiveAuthorizationCodeProvider{
			tamperWithState: true,
		}
		client.authorizationCodeProviderFactory = func() authorizationCodeProvider {
			return authCodeProvider
		}
		_, err = client.authenticateByOAuthAuthorizationCode()
		assertEqualE(t, err.Error(), "invalid oauth state received")
		time.Sleep(100 * time.Millisecond)
		authCodeProvider.assertResponseBodyContains("invalid oauth state received")
	})

	t.Run("ErrorFromIdPWhileGettingCode", func(t *testing.T) {
		credentialsStorage.deleteCredential(cacheTokenSpec)
		wiremock.registerMappings(t, newWiremockMapping("oauth2/authorization_code/error_from_idp.json"))
		authCodeProvider := &nonInteractiveAuthorizationCodeProvider{}
		client.authorizationCodeProviderFactory = func() authorizationCodeProvider {
			return authCodeProvider
		}
		_, err = client.authenticateByOAuthAuthorizationCode()
		assertEqualE(t, err.Error(), "error while getting authentication from oauth: some error. Details: some error desc")
		time.Sleep(100 * time.Millisecond)
		authCodeProvider.assertResponseBodyContains("error while getting authentication from oauth: some error. Details: some error desc")
	})

	t.Run("ErrorFromProviderWhileGettingCode", func(t *testing.T) {
		authCodeProvider := &nonInteractiveAuthorizationCodeProvider{
			triggerError: "test error",
		}
		client.authorizationCodeProviderFactory = func() authorizationCodeProvider {
			return authCodeProvider
		}
		_, err = client.authenticateByOAuthAuthorizationCode()
		assertEqualE(t, err.Error(), "test error")
	})

	t.Run("InvalidCode", func(t *testing.T) {
		credentialsStorage.deleteCredential(cacheTokenSpec)
		wiremock.registerMappings(t, newWiremockMapping("oauth2/authorization_code/invalid_code.json"))
		authCodeProvider := &nonInteractiveAuthorizationCodeProvider{}
		client.authorizationCodeProviderFactory = func() authorizationCodeProvider {
			return authCodeProvider
		}
		_, err = client.authenticateByOAuthAuthorizationCode()
		assertNotNilE(t, err)
		assertEqualE(t, err.(*oauth2.RetrieveError).ErrorCode, "invalid_grant")
		assertEqualE(t, err.(*oauth2.RetrieveError).ErrorDescription, "The authorization code is invalid or has expired.")
		time.Sleep(100 * time.Millisecond)
		authCodeProvider.assertResponseBodyContains("invalid_grant")
	})

	t.Run("timeout", func(t *testing.T) {
		wiremock.registerMappings(t, newWiremockMapping("oauth2/authorization_code/successful_flow.json"))
		client.cfg.ExternalBrowserTimeout = 2 * time.Second
		authCodeProvider := &nonInteractiveAuthorizationCodeProvider{
			sleepTime: 3 * time.Second,
		}
		client.authorizationCodeProviderFactory = func() authorizationCodeProvider {
			return authCodeProvider
		}
		_, err = client.authenticateByOAuthAuthorizationCode()
		assertNotNilE(t, err)
		assertStringContainsE(t, err.Error(), "timed out")
		time.Sleep(2 * time.Second) // awaiting timeout
	})
}

func TestUnitOAuthClientCredentials(t *testing.T) {
	skipOnMac(t, "keychain requires password")
	cacheTokenSpec := newOAuthAccessTokenSpec(wiremock.connectionConfig().OauthTokenRequestURL, wiremock.connectionConfig().User)
	crt := newCountingRoundTripper(SnowflakeTransport)
	httpClient := http.Client{
		Transport: crt,
	}
	cfgFactory := func() *Config {
		return &Config{
			User:                           "testUser",
			Role:                           "ANALYST",
			OauthClientID:                  "testClientId",
			OauthClientSecret:              "testClientSecret",
			OauthTokenRequestURL:           wiremock.baseURL() + "/oauth/token",
			Transporter:                    crt,
			ClientStoreTemporaryCredential: ConfigBoolTrue,
		}
	}
	client, err := newOauthClient(context.WithValue(context.Background(), oauth2.HTTPClient, httpClient), cfgFactory())
	assertNilF(t, err)

	t.Run("success", func(t *testing.T) {
		credentialsStorage.deleteCredential(cacheTokenSpec)
		wiremock.registerMappings(t, newWiremockMapping("oauth2/client_credentials/successful_flow.json"))
		token, err := client.authenticateByOAuthClientCredentials()
		assertNilF(t, err)
		assertEqualE(t, token, "access-token-123")
	})

	t.Run("should store token in cache", func(t *testing.T) {
		crt.reset()
		credentialsStorage.deleteCredential(cacheTokenSpec)
		wiremock.registerMappings(t, newWiremockMapping("oauth2/client_credentials/successful_flow.json"))
		token, err := client.authenticateByOAuthClientCredentials()
		assertNilF(t, err)
		assertEqualE(t, token, "access-token-123")

		client, err := newOauthClient(context.Background(), cfgFactory())
		assertNilF(t, err)
		token, err = client.authenticateByOAuthClientCredentials()
		assertNilF(t, err)
		assertEqualE(t, token, "access-token-123")

		assertEqualE(t, crt.postReqCount[cfgFactory().OauthTokenRequestURL], 1)
	})

	t.Run("consecutive calls should take token from cache", func(t *testing.T) {
		crt.reset()
		credentialsStorage.setCredential(cacheTokenSpec, "access-token-123")
		for i := 0; i < 3; i++ {
			client, err := newOauthClient(context.Background(), cfgFactory())
			assertNilF(t, err)
			token, err := client.authenticateByOAuthClientCredentials()
			assertNilF(t, err)
			assertEqualE(t, token, "access-token-123")
		}
		assertEqualE(t, crt.postReqCount[cfgFactory().OauthTokenRequestURL], 0)
	})

	t.Run("disabling cache", func(t *testing.T) {
		cfg := cfgFactory()
		cfg.ClientStoreTemporaryCredential = ConfigBoolFalse
		credentialsStorage.deleteCredential(cacheTokenSpec)
		wiremock.registerMappings(t, newWiremockMapping("oauth2/client_credentials/successful_flow.json"))
		client, err := newOauthClient(context.Background(), cfg)
		assertNilF(t, err)
		token, err := client.authenticateByOAuthClientCredentials()
		assertNilF(t, err)
		assertEqualE(t, token, "access-token-123")

		client, err = newOauthClient(context.Background(), cfg)
		assertNilF(t, err)
		token, err = client.authenticateByOAuthClientCredentials()
		assertNilF(t, err)
		assertEqualE(t, token, "access-token-123")

		assertEqualE(t, crt.postReqCount[cfg.OauthTokenRequestURL], 2)
	})

	t.Run("invalid_client", func(t *testing.T) {
		credentialsStorage.deleteCredential(cacheTokenSpec)
		wiremock.registerMappings(t, newWiremockMapping("oauth2/client_credentials/invalid_client.json"))
		_, err = client.authenticateByOAuthClientCredentials()
		assertNotNilF(t, err)
		oauth2Err := err.(*oauth2.RetrieveError)
		assertEqualE(t, oauth2Err.ErrorCode, "invalid_client")
		assertEqualE(t, oauth2Err.ErrorDescription, "The client secret supplied for a confidential client is invalid.")
	})
}

func TestAuthorizationCodeFlow(t *testing.T) {
	if runningOnGithubAction() && runningOnLinux() {
		t.Skip("Github blocks writing to file system")
	}
	skipOnMac(t, "keychain requires password")
	currentDefaultAuthorizationCodeProviderFactory := defaultAuthorizationCodeProviderFactory
	defer func() {
		defaultAuthorizationCodeProviderFactory = currentDefaultAuthorizationCodeProviderFactory
	}()
	defaultAuthorizationCodeProviderFactory = func() authorizationCodeProvider {
		return &nonInteractiveAuthorizationCodeProvider{
			t:  t,
			mu: sync.Mutex{},
		}
	}
	roundTripper := newCountingRoundTripper(snowflakeNoOcspTransport)

	t.Run("successful flow", func(t *testing.T) {
		wiremock.registerMappings(t,
			newWiremockMapping("oauth2/authorization_code/successful_flow.json"),
			newWiremockMapping("oauth2/login_request.json"),
			newWiremockMapping("select1.json"))
		cfg := wiremock.connectionConfig()
		cfg.Role = "ANALYST"
		cfg.Authenticator = AuthTypeOAuthAuthorizationCode
		cfg.OauthRedirectURI = "http://localhost:1234/snowflake/oauth-redirect"
		cfg.Transporter = roundTripper
		connector := NewConnector(SnowflakeDriver{}, *cfg)
		db := sql.OpenDB(connector)
		runSmokeQuery(t, db)
	})

	t.Run("should use cached access token", func(t *testing.T) {
		roundTripper.reset()
		wiremock.registerMappings(t,
			newWiremockMapping("oauth2/authorization_code/successful_flow.json"),
			newWiremockMapping("oauth2/login_request.json"),
			newWiremockMapping("select1.json"))
		cfg := wiremock.connectionConfig()
		cfg.Role = "ANALYST"
		cfg.Authenticator = AuthTypeOAuthAuthorizationCode
		cfg.OauthRedirectURI = "http://localhost:1234/snowflake/oauth-redirect"
		cfg.Transporter = roundTripper
		oauthAccessTokenSpec := newOAuthAccessTokenSpec(cfg.OauthTokenRequestURL, cfg.User)
		credentialsStorage.deleteCredential(oauthAccessTokenSpec)
		connector := NewConnector(SnowflakeDriver{}, *cfg)
		db := sql.OpenDB(connector)
		conn1, err := db.Conn(context.Background())
		assertNilF(t, err)
		defer conn1.Close()
		conn2, err := db.Conn(context.Background())
		assertNilF(t, err)
		defer conn2.Close()
		runSmokeQueryWithConn(t, conn1)
		runSmokeQueryWithConn(t, conn2)
		assertEqualE(t, roundTripper.postReqCount[cfg.OauthTokenRequestURL], 1)
	})

	t.Run("should update cache with new token when the old one expired", func(t *testing.T) {
		roundTripper.reset()
		wiremock.registerMappings(t,
			newWiremockMapping("oauth2/login_request_with_expired_access_token.json"),
			newWiremockMapping("oauth2/authorization_code/successful_flow.json"),
			newWiremockMapping("oauth2/login_request.json"),
			newWiremockMapping("select1.json"))
		cfg := wiremock.connectionConfig()
		cfg.Role = "ANALYST"
		cfg.Authenticator = AuthTypeOAuthAuthorizationCode
		cfg.OauthRedirectURI = "http://localhost:1234/snowflake/oauth-redirect"
		cfg.Transporter = roundTripper
		oauthAccessTokenSpec := newOAuthAccessTokenSpec(cfg.OauthTokenRequestURL, cfg.User)
		credentialsStorage.setCredential(oauthAccessTokenSpec, "expired-token")
		connector := NewConnector(SnowflakeDriver{}, *cfg)
		db := sql.OpenDB(connector)
		runSmokeQuery(t, db)
		assertEqualE(t, roundTripper.postReqCount[cfg.OauthTokenRequestURL], 1)
		assertEqualE(t, credentialsStorage.getCredential(oauthAccessTokenSpec), "access-token-123")
	})
}

func TestClientCredentialsFlow(t *testing.T) {
	if runningOnGithubAction() && runningOnLinux() {
		t.Skip("Github blocks writing to file system")
	}
	skipOnMac(t, "keychain requires password")
	currentDefaultAuthorizationCodeProviderFactory := defaultAuthorizationCodeProviderFactory
	defer func() {
		defaultAuthorizationCodeProviderFactory = currentDefaultAuthorizationCodeProviderFactory
	}()
	defaultAuthorizationCodeProviderFactory = func() authorizationCodeProvider {
		return &nonInteractiveAuthorizationCodeProvider{
			t:  t,
			mu: sync.Mutex{},
		}
	}
	roundTripper := newCountingRoundTripper(snowflakeNoOcspTransport)

	cfg := wiremock.connectionConfig()
	cfg.Role = "ANALYST"
	cfg.Authenticator = AuthTypeOAuthClientCredentials
	cfg.Transporter = roundTripper

	oauthAccessTokenSpec := newOAuthAccessTokenSpec(cfg.OauthTokenRequestURL, cfg.User)

	t.Run("successful flow", func(t *testing.T) {
		credentialsStorage.deleteCredential(oauthAccessTokenSpec)
		wiremock.registerMappings(t,
			newWiremockMapping("oauth2/client_credentials/successful_flow.json"),
			newWiremockMapping("oauth2/login_request.json"),
			newWiremockMapping("select1.json"))
		connector := NewConnector(SnowflakeDriver{}, *cfg)
		db := sql.OpenDB(connector)
		runSmokeQuery(t, db)
	})

	t.Run("should use cached access token", func(t *testing.T) {
		roundTripper.reset()
		wiremock.registerMappings(t,
			newWiremockMapping("oauth2/client_credentials/successful_flow.json"),
			newWiremockMapping("oauth2/login_request.json"),
			newWiremockMapping("select1.json"))
		credentialsStorage.deleteCredential(oauthAccessTokenSpec)
		connector := NewConnector(SnowflakeDriver{}, *cfg)
		db := sql.OpenDB(connector)
		conn1, err := db.Conn(context.Background())
		assertNilF(t, err)
		defer conn1.Close()
		conn2, err := db.Conn(context.Background())
		assertNilF(t, err)
		defer conn2.Close()
		runSmokeQueryWithConn(t, conn1)
		runSmokeQueryWithConn(t, conn2)
		assertEqualE(t, roundTripper.postReqCount[cfg.OauthTokenRequestURL], 1)
	})

	t.Run("should update cache with new token when the old one expired", func(t *testing.T) {
		roundTripper.reset()
		wiremock.registerMappings(t,
			newWiremockMapping("oauth2/login_request_with_expired_access_token.json"),
			newWiremockMapping("oauth2/client_credentials/successful_flow.json"),
			newWiremockMapping("oauth2/login_request.json"),
			newWiremockMapping("select1.json"))

		credentialsStorage.setCredential(oauthAccessTokenSpec, "expired-token")
		connector := NewConnector(SnowflakeDriver{}, *cfg)
		db := sql.OpenDB(connector)
		runSmokeQuery(t, db)
		assertEqualE(t, roundTripper.postReqCount[cfg.OauthTokenRequestURL], 1)
		assertEqualE(t, credentialsStorage.getCredential(oauthAccessTokenSpec), "access-token-123")
	})
}

type nonInteractiveAuthorizationCodeProvider struct {
	t               *testing.T
	tamperWithState bool
	triggerError    string
	responseBody    string
	mu              sync.Mutex
	sleepTime       time.Duration
}

func (provider *nonInteractiveAuthorizationCodeProvider) run(authorizationURL string) error {
	if provider.sleepTime != 0 {
		time.Sleep(provider.sleepTime)
		return errors.New("ignore me")
	}
	if provider.triggerError != "" {
		return errors.New(provider.triggerError)
	}
	go func() {
		resp, err := http.Get(authorizationURL)
		assertNilF(provider.t, err)
		assertEqualE(provider.t, resp.StatusCode, http.StatusOK)
		respBody, err := io.ReadAll(resp.Body)
		assertNilF(provider.t, err)
		provider.mu.Lock()
		defer provider.mu.Unlock()
		provider.responseBody = string(respBody)
	}()
	return nil
}

func (provider *nonInteractiveAuthorizationCodeProvider) createState() string {
	if provider.tamperWithState {
		return "invalidState"
	}
	return "testState"
}

func (provider *nonInteractiveAuthorizationCodeProvider) createCodeVerifier() string {
	return "testCodeVerifier"
}

func (provider *nonInteractiveAuthorizationCodeProvider) assertResponseBodyContains(str string) {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	assertStringContainsE(provider.t, provider.responseBody, str)
}
