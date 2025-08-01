package gosnowflake

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"golang.org/x/oauth2"
)

func TestUnitOAuthAuthorizationCode(t *testing.T) {
	skipOnMac(t, "keychain requires password")
	roundTripper := newCountingRoundTripper(snowflakeNoRevocationCheckTransport)
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
	client, err := newOauthClient(context.WithValue(context.Background(), oauth2.HTTPClient, httpClient), cfg, &snowflakeConn{})
	assertNilF(t, err)
	accessTokenSpec := newOAuthAccessTokenSpec(wiremock.connectionConfig().OauthTokenRequestURL, wiremock.connectionConfig().User)
	refreshTokenSpec := newOAuthRefreshTokenSpec(wiremock.connectionConfig().OauthTokenRequestURL, wiremock.connectionConfig().User)

	t.Run("Success", func(t *testing.T) {
		err := credentialsStorage.deleteCredential(accessTokenSpec)
		assertNilF(t, err)
		err = credentialsStorage.deleteCredential(refreshTokenSpec)
		assertNilF(t, err)
		wiremock.registerMappings(t, newWiremockMapping("oauth2/authorization_code/successful_flow.json"))
		authCodeProvider := &nonInteractiveAuthorizationCodeProvider{t: t}
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
		err := credentialsStorage.deleteCredential(accessTokenSpec)
		assertNilF(t, err)
		err = credentialsStorage.deleteCredential(refreshTokenSpec)
		assertNilF(t, err)
		wiremock.registerMappings(t, newWiremockMapping("oauth2/authorization_code/successful_flow.json"))
		authCodeProvider := &nonInteractiveAuthorizationCodeProvider{}
		client.authorizationCodeProviderFactory = func() authorizationCodeProvider {
			return authCodeProvider
		}
		_, err = client.authenticateByOAuthAuthorizationCode()
		assertNilF(t, err)
		cred, err := credentialsStorage.getCredential(accessTokenSpec)
		assertEqualE(t, cred, "access-token-123")
	})

	t.Run("Use cache for consecutive calls", func(t *testing.T) {
		roundTripper.reset()
		err := credentialsStorage.setCredential(accessTokenSpec, "access-token-123")
		assertNilF(t, err)
		wiremock.registerMappings(t, newWiremockMapping("oauth2/authorization_code/successful_flow.json"))
		authCodeProvider := &nonInteractiveAuthorizationCodeProvider{}
		for i := 0; i < 3; i++ {
			client, err := newOauthClient(context.WithValue(context.Background(), oauth2.HTTPClient, httpClient), cfg, &snowflakeConn{})
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
		err := credentialsStorage.deleteCredential(accessTokenSpec)
		assertNilF(t, err)
		err = credentialsStorage.deleteCredential(refreshTokenSpec)
		assertNilF(t, err)
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
		err := credentialsStorage.deleteCredential(accessTokenSpec)
		assertNilF(t, err)
		err = credentialsStorage.deleteCredential(refreshTokenSpec)
		assertNilF(t, err)
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
		err := credentialsStorage.deleteCredential(accessTokenSpec)
		assertNilF(t, err)
		err = credentialsStorage.deleteCredential(refreshTokenSpec)
		assertNilF(t, err)
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
		err := credentialsStorage.deleteCredential(accessTokenSpec)
		assertNilF(t, err)
		err = credentialsStorage.deleteCredential(refreshTokenSpec)
		assertNilF(t, err)
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
	client, err := newOauthClient(context.WithValue(context.Background(), oauth2.HTTPClient, httpClient), cfgFactory(), &snowflakeConn{})
	assertNilF(t, err)

	t.Run("success", func(t *testing.T) {
		err := credentialsStorage.deleteCredential(cacheTokenSpec)
		assertNilF(t, err)
		wiremock.registerMappings(t, newWiremockMapping("oauth2/client_credentials/successful_flow.json"))
		token, err := client.authenticateByOAuthClientCredentials()
		assertNilF(t, err)
		assertEqualE(t, token, "access-token-123")
	})

	t.Run("should store token in cache", func(t *testing.T) {
		crt.reset()
		err := credentialsStorage.deleteCredential(cacheTokenSpec)
		assertNilF(t, err)
		wiremock.registerMappings(t, newWiremockMapping("oauth2/client_credentials/successful_flow.json"))
		token, err := client.authenticateByOAuthClientCredentials()
		assertNilF(t, err)
		assertEqualE(t, token, "access-token-123")

		client, err := newOauthClient(context.Background(), cfgFactory(), &snowflakeConn{})
		assertNilF(t, err)
		token, err = client.authenticateByOAuthClientCredentials()
		assertNilF(t, err)
		assertEqualE(t, token, "access-token-123")

		assertEqualE(t, crt.postReqCount[cfgFactory().OauthTokenRequestURL], 1)
	})

	t.Run("consecutive calls should take token from cache", func(t *testing.T) {
		crt.reset()
		err := credentialsStorage.setCredential(cacheTokenSpec, "access-token-123")
		assertNilF(t, err)
		for i := 0; i < 3; i++ {
			client, err := newOauthClient(context.Background(), cfgFactory(), &snowflakeConn{})
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
		err := credentialsStorage.deleteCredential(cacheTokenSpec)
		assertNilF(t, err)
		wiremock.registerMappings(t, newWiremockMapping("oauth2/client_credentials/successful_flow.json"))
		client, err := newOauthClient(context.Background(), cfg, &snowflakeConn{})
		assertNilF(t, err)
		token, err := client.authenticateByOAuthClientCredentials()
		assertNilF(t, err)
		assertEqualE(t, token, "access-token-123")

		client, err = newOauthClient(context.Background(), cfg, &snowflakeConn{})
		assertNilF(t, err)
		token, err = client.authenticateByOAuthClientCredentials()
		assertNilF(t, err)
		assertEqualE(t, token, "access-token-123")

		assertEqualE(t, crt.postReqCount[cfg.OauthTokenRequestURL], 2)
	})

	t.Run("invalid_client", func(t *testing.T) {
		err := credentialsStorage.deleteCredential(cacheTokenSpec)
		assertNilF(t, err)
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
	roundTripper := newCountingRoundTripper(snowflakeNoRevocationCheckTransport)

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
		oauthAccessTokenSpec := newOAuthAccessTokenSpec(cfg.OauthTokenRequestURL, cfg.User)
		oauthRefreshTokenSpec := newOAuthRefreshTokenSpec(cfg.OauthTokenRequestURL, cfg.User)
		err := credentialsStorage.deleteCredential(oauthAccessTokenSpec)
		assertNilF(t, err)
		err = credentialsStorage.deleteCredential(oauthRefreshTokenSpec)
		assertNilF(t, err)
		connector := NewConnector(SnowflakeDriver{}, *cfg)
		db := sql.OpenDB(connector)
		runSmokeQuery(t, db)
	})

	t.Run("successful flow with single-use refresh token enabled", func(t *testing.T) {
		wiremock.registerMappings(t,
			newWiremockMapping("oauth2/authorization_code/successful_flow_with_single_use_refresh_token.json"),
			newWiremockMapping("oauth2/login_request.json"),
			newWiremockMapping("select1.json"))
		cfg := wiremock.connectionConfig()
		cfg.Role = "ANALYST"
		cfg.Authenticator = AuthTypeOAuthAuthorizationCode
		cfg.OauthRedirectURI = "http://localhost:1234/snowflake/oauth-redirect"
		cfg.Transporter = roundTripper
		cfg.EnableSingleUseRefreshTokens = true
		oauthAccessTokenSpec := newOAuthAccessTokenSpec(cfg.OauthTokenRequestURL, cfg.User)
		oauthRefreshTokenSpec := newOAuthRefreshTokenSpec(cfg.OauthTokenRequestURL, cfg.User)
		err := credentialsStorage.deleteCredential(oauthAccessTokenSpec)
		assertNilF(t, err)
		err = credentialsStorage.deleteCredential(oauthRefreshTokenSpec)
		assertNilF(t, err)
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
		oauthRefreshTokenSpec := newOAuthRefreshTokenSpec(cfg.OauthTokenRequestURL, cfg.User)
		err := credentialsStorage.deleteCredential(oauthAccessTokenSpec)
		assertNilF(t, err)
		err = credentialsStorage.deleteCredential(oauthRefreshTokenSpec)
		assertNilF(t, err)
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

	t.Run("should update cache with new token when the old one expired if refresh token is missing", func(t *testing.T) {
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
		oauthRefreshTokenSpec := newOAuthRefreshTokenSpec(cfg.OauthTokenRequestURL, cfg.User)
		err := credentialsStorage.setCredential(oauthAccessTokenSpec, "expired-token")
		assertNilF(t, err)
		err = credentialsStorage.deleteCredential(oauthRefreshTokenSpec)
		assertNilF(t, err)
		connector := NewConnector(SnowflakeDriver{}, *cfg)
		db := sql.OpenDB(connector)
		runSmokeQuery(t, db)
		assertEqualE(t, roundTripper.postReqCount[cfg.OauthTokenRequestURL], 1)
		cred, err := credentialsStorage.getCredential(oauthAccessTokenSpec)
		assertNilF(t, err)
		assertEqualE(t, cred, "access-token-123")
	})

	t.Run("if access token is missing and refresh token is present, should run refresh token flow", func(t *testing.T) {
		roundTripper.reset()
		cfg := wiremock.connectionConfig()
		cfg.OauthScope = "session:role:ANALYST offline_access"
		cfg.Authenticator = AuthTypeOAuthAuthorizationCode
		cfg.OauthRedirectURI = "http://localhost:1234/snowflake/oauth-redirect"
		cfg.Transporter = roundTripper
		oauthAccessTokenSpec := newOAuthAccessTokenSpec(cfg.OauthTokenRequestURL, cfg.User)
		oauthRefreshTokenSpec := newOAuthRefreshTokenSpec(cfg.OauthTokenRequestURL, cfg.User)
		err := credentialsStorage.deleteCredential(oauthAccessTokenSpec)
		assertNilF(t, err)
		err = credentialsStorage.setCredential(oauthRefreshTokenSpec, "refresh-token-123")
		assertNilF(t, err)
		wiremock.registerMappings(t, newWiremockMapping("oauth2/login_request_with_expired_access_token.json"),
			newWiremockMapping("oauth2/refresh_token/successful_flow.json"),
			newWiremockMapping("oauth2/authorization_code/successful_flow.json"),
			newWiremockMapping("oauth2/login_request.json"),
			newWiremockMapping("select1.json"))
		connector := NewConnector(SnowflakeDriver{}, *cfg)
		db := sql.OpenDB(connector)
		runSmokeQuery(t, db)
		assertEqualE(t, roundTripper.postReqCount[cfg.OauthTokenRequestURL], 1) // only refresh token
		cred, err := credentialsStorage.getCredential(oauthAccessTokenSpec)
		assertNilF(t, err)
		assertEqualE(t, cred, "access-token-123")
		cred, err = credentialsStorage.getCredential(oauthRefreshTokenSpec)
		assertNilF(t, err)
		cred, err = credentialsStorage.getCredential(oauthRefreshTokenSpec)
		assertNilF(t, err)
		assertEqualE(t, cred, "refresh-token-123a")
	})

	t.Run("if access token is expired and refresh token is present, should run refresh token flow", func(t *testing.T) {
		roundTripper.reset()
		cfg := wiremock.connectionConfig()
		cfg.OauthScope = "session:role:ANALYST offline_access"
		cfg.Authenticator = AuthTypeOAuthAuthorizationCode
		cfg.OauthRedirectURI = "http://localhost:1234/snowflake/oauth-redirect"
		cfg.Transporter = roundTripper
		oauthAccessTokenSpec := newOAuthAccessTokenSpec(cfg.OauthTokenRequestURL, cfg.User)
		oauthRefreshTokenSpec := newOAuthRefreshTokenSpec(cfg.OauthTokenRequestURL, cfg.User)
		err := credentialsStorage.setCredential(oauthAccessTokenSpec, "expired-token")
		assertNilF(t, err)
		err = credentialsStorage.setCredential(oauthRefreshTokenSpec, "refresh-token-123")
		assertNilF(t, err)
		wiremock.registerMappings(t, newWiremockMapping("oauth2/login_request_with_expired_access_token.json"),
			newWiremockMapping("oauth2/refresh_token/successful_flow.json"),
			newWiremockMapping("oauth2/authorization_code/successful_flow.json"),
			newWiremockMapping("oauth2/login_request.json"),
			newWiremockMapping("select1.json"))
		connector := NewConnector(SnowflakeDriver{}, *cfg)
		db := sql.OpenDB(connector)
		runSmokeQuery(t, db)
		assertEqualE(t, roundTripper.postReqCount[cfg.OauthTokenRequestURL], 1) // only refresh token
		cred, err := credentialsStorage.getCredential(oauthAccessTokenSpec)
		assertNilF(t, err)
		assertEqualE(t, cred, "access-token-123")
		cred, err = credentialsStorage.getCredential(oauthRefreshTokenSpec)
		assertNilF(t, err)
		assertEqualE(t, cred, "refresh-token-123a")
	})

	t.Run("if new refresh token is not returned, should keep old one", func(t *testing.T) {
		roundTripper.reset()
		cfg := wiremock.connectionConfig()
		cfg.OauthScope = "session:role:ANALYST offline_access"
		cfg.Authenticator = AuthTypeOAuthAuthorizationCode
		cfg.OauthRedirectURI = "http://localhost:1234/snowflake/oauth-redirect"
		cfg.Transporter = roundTripper
		oauthAccessTokenSpec := newOAuthAccessTokenSpec(cfg.OauthTokenRequestURL, cfg.User)
		oauthRefreshTokenSpec := newOAuthRefreshTokenSpec(cfg.OauthTokenRequestURL, cfg.User)
		err := credentialsStorage.setCredential(oauthAccessTokenSpec, "expired-token")
		assertNilF(t, err)
		err = credentialsStorage.setCredential(oauthRefreshTokenSpec, "refresh-token-123")
		assertNilF(t, err)
		wiremock.registerMappings(t, newWiremockMapping("oauth2/login_request_with_expired_access_token.json"),
			newWiremockMapping("oauth2/refresh_token/successful_flow_without_new_refresh_token.json"),
			newWiremockMapping("oauth2/authorization_code/successful_flow.json"),
			newWiremockMapping("oauth2/login_request.json"),
			newWiremockMapping("select1.json"))
		connector := NewConnector(SnowflakeDriver{}, *cfg)
		db := sql.OpenDB(connector)
		runSmokeQuery(t, db)
		assertEqualE(t, roundTripper.postReqCount[cfg.OauthTokenRequestURL], 1) // only refresh token
		cred, err := credentialsStorage.getCredential(oauthAccessTokenSpec)
		assertNilF(t, err)
		assertEqualE(t, cred, "access-token-123")
		cred, err = credentialsStorage.getCredential(oauthRefreshTokenSpec)
		assertNilF(t, err)
		assertEqualE(t, cred, "refresh-token-123")
	})

	t.Run("if refreshing token failed, run normal flow", func(t *testing.T) {
		roundTripper.reset()
		cfg := wiremock.connectionConfig()
		cfg.OauthScope = "session:role:ANALYST offline_access"
		cfg.Authenticator = AuthTypeOAuthAuthorizationCode
		cfg.OauthRedirectURI = "http://localhost:1234/snowflake/oauth-redirect"
		cfg.Transporter = roundTripper
		oauthAccessTokenSpec := newOAuthAccessTokenSpec(cfg.OauthTokenRequestURL, cfg.User)
		oauthRefreshTokenSpec := newOAuthRefreshTokenSpec(cfg.OauthTokenRequestURL, cfg.User)
		err := credentialsStorage.setCredential(oauthAccessTokenSpec, "expired-token")
		assertNilF(t, err)
		err = credentialsStorage.setCredential(oauthRefreshTokenSpec, "expired-refresh-token")
		assertNilF(t, err)
		wiremock.registerMappings(t, newWiremockMapping("oauth2/login_request_with_expired_access_token.json"),
			newWiremockMapping("oauth2/refresh_token/invalid_refresh_token.json"),
			newWiremockMapping("oauth2/authorization_code/successful_flow_with_offline_access.json"),
			newWiremockMapping("oauth2/login_request.json"),
			newWiremockMapping("select1.json"))
		connector := NewConnector(SnowflakeDriver{}, *cfg)
		db := sql.OpenDB(connector)
		runSmokeQuery(t, db)
		assertEqualE(t, roundTripper.postReqCount[cfg.OauthTokenRequestURL], 2) // only refresh token fails, then authorization code
		cred, err := credentialsStorage.getCredential(oauthAccessTokenSpec)
		assertNilF(t, err)
		assertEqualE(t, cred, "access-token-123")
		cred, err = credentialsStorage.getCredential(oauthRefreshTokenSpec)
		assertNilF(t, err)
		assertEqualE(t, cred, "refresh-token-123")
	})

	t.Run("if secure storage is disabled, run normal flow", func(t *testing.T) {
		roundTripper.reset()
		cfg := wiremock.connectionConfig()
		cfg.OauthScope = "session:role:ANALYST offline_access"
		cfg.Authenticator = AuthTypeOAuthAuthorizationCode
		cfg.OauthRedirectURI = "http://localhost:1234/snowflake/oauth-redirect"
		cfg.Transporter = roundTripper
		cfg.ClientStoreTemporaryCredential = ConfigBoolFalse
		oauthAccessTokenSpec := newOAuthAccessTokenSpec(cfg.OauthTokenRequestURL, cfg.User)
		oauthRefreshTokenSpec := newOAuthRefreshTokenSpec(cfg.OauthTokenRequestURL, cfg.User)
		err := credentialsStorage.setCredential(oauthAccessTokenSpec, "old-access-token")
		assertNilF(t, err)
		err = credentialsStorage.setCredential(oauthRefreshTokenSpec, "old-refresh-token")
		assertNilF(t, err)
		wiremock.registerMappings(t, newWiremockMapping("oauth2/authorization_code/successful_flow_with_offline_access.json"),
			newWiremockMapping("oauth2/login_request.json"),
			newWiremockMapping("select1.json"))
		connector := NewConnector(SnowflakeDriver{}, *cfg)
		db := sql.OpenDB(connector)
		runSmokeQuery(t, db)
		assertEqualE(t, roundTripper.postReqCount[cfg.OauthTokenRequestURL], 1) // only access token token
		cred, err := credentialsStorage.getCredential(oauthAccessTokenSpec)
		assertNilF(t, err)
		assertEqualE(t, cred, "old-access-token")
		cred, err = credentialsStorage.getCredential(oauthRefreshTokenSpec)
		assertNilF(t, err)
		assertEqualE(t, cred, "old-refresh-token")
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
	roundTripper := newCountingRoundTripper(snowflakeNoRevocationCheckTransport)

	cfg := wiremock.connectionConfig()
	cfg.Role = "ANALYST"
	cfg.Authenticator = AuthTypeOAuthClientCredentials
	cfg.Transporter = roundTripper

	oauthAccessTokenSpec := newOAuthAccessTokenSpec(cfg.OauthTokenRequestURL, cfg.User)
	oauthRefreshTokenSpec := newOAuthRefreshTokenSpec(cfg.OauthTokenRequestURL, cfg.User)

	t.Run("successful flow", func(t *testing.T) {
		err := credentialsStorage.deleteCredential(oauthAccessTokenSpec)
		assertNilF(t, err)
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
		err := credentialsStorage.deleteCredential(oauthAccessTokenSpec)
		assertNilF(t, err)
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

		err := credentialsStorage.setCredential(oauthAccessTokenSpec, "expired-token")
		assertNilF(t, err)
		connector := NewConnector(SnowflakeDriver{}, *cfg)
		db := sql.OpenDB(connector)
		runSmokeQuery(t, db)
		assertEqualE(t, roundTripper.postReqCount[cfg.OauthTokenRequestURL], 1)
		cred, err := credentialsStorage.getCredential(oauthAccessTokenSpec)
		assertNilF(t, err)
		assertEqualE(t, cred, "access-token-123")
	})

	t.Run("should not use refresh token, but ask for fresh access token", func(t *testing.T) {
		roundTripper.reset()
		wiremock.registerMappings(t,
			newWiremockMapping("oauth2/login_request_with_expired_access_token.json"),
			newWiremockMapping("oauth2/client_credentials/successful_flow.json"),
			newWiremockMapping("oauth2/login_request.json"),
			newWiremockMapping("select1.json"))

		err := credentialsStorage.setCredential(oauthAccessTokenSpec, "expired-token")
		assertNilF(t, err)
		err = credentialsStorage.setCredential(oauthRefreshTokenSpec, "refresh-token-123")
		assertNilF(t, err)
		connector := NewConnector(SnowflakeDriver{}, *cfg)
		db := sql.OpenDB(connector)
		runSmokeQuery(t, db)
		assertEqualE(t, roundTripper.postReqCount[cfg.OauthTokenRequestURL], 1)
		cred, err := credentialsStorage.getCredential(oauthAccessTokenSpec)
		assertNilF(t, err)
		assertEqualE(t, cred, "access-token-123")
		cred, err = credentialsStorage.getCredential(oauthRefreshTokenSpec)
		assertNilF(t, err)
		assertEqualE(t, cred, "refresh-token-123")
	})

	t.Run("should not use access token if token cache is disabled", func(t *testing.T) {
		roundTripper.reset()
		wiremock.registerMappings(t,
			newWiremockMapping("oauth2/login_request_with_expired_access_token.json"),
			newWiremockMapping("oauth2/client_credentials/successful_flow.json"),
			newWiremockMapping("oauth2/login_request.json"),
			newWiremockMapping("select1.json"))

		err := credentialsStorage.setCredential(oauthAccessTokenSpec, "access-token-123")
		assertNilF(t, err)
		cfg.ClientStoreTemporaryCredential = ConfigBoolFalse
		connector := NewConnector(SnowflakeDriver{}, *cfg)
		db := sql.OpenDB(connector)
		runSmokeQuery(t, db)
		assertEqualE(t, roundTripper.postReqCount[cfg.OauthTokenRequestURL], 1)
		cred, err := credentialsStorage.getCredential(oauthAccessTokenSpec)
		assertNilF(t, err)
		assertEqualE(t, cred, "access-token-123")
	})
}

func TestEligibleForDefaultClientCredentials(t *testing.T) {
	tests := []struct {
		name        string
		oauthClient *oauthClient
		expected    bool
	}{
		{
			name: "Client credentials not supplied and Snowflake as IdP",
			oauthClient: &oauthClient{
				cfg: &Config{
					Host:                  "example.snowflakecomputing.com",
					OauthClientID:         "",
					OauthClientSecret:     "",
					OauthAuthorizationURL: "https://example.snowflakecomputing.com/oauth/authorize",
					OauthTokenRequestURL:  "https://example.snowflakecomputing.com/oauth/token",
				},
			},
			expected: true,
		},
		{
			name: "Client credentials not supplied and empty URLs (defaults to Snowflake)",
			oauthClient: &oauthClient{
				cfg: &Config{
					Host:                  "example.snowflakecomputing.com",
					OauthClientID:         "",
					OauthClientSecret:     "",
					OauthAuthorizationURL: "",
					OauthTokenRequestURL:  "",
				},
			},
			expected: true,
		},
		{
			name: "Client credentials supplied",
			oauthClient: &oauthClient{
				cfg: &Config{
					Host:                  "example.snowflakecomputing.com",
					OauthClientID:         "testClientID",
					OauthClientSecret:     "testClientSecret",
					OauthAuthorizationURL: "https://example.snowflakecomputing.com/oauth/authorize",
					OauthTokenRequestURL:  "https://example.snowflakecomputing.com/oauth/token",
				},
			},
			expected: false,
		},
		{
			name: "Only client ID supplied",
			oauthClient: &oauthClient{
				cfg: &Config{
					Host:                  "example.snowflakecomputing.com",
					OauthClientID:         "testClientID",
					OauthClientSecret:     "",
					OauthAuthorizationURL: "https://example.snowflakecomputing.com/oauth/authorize",
					OauthTokenRequestURL:  "https://example.snowflakecomputing.com/oauth/token",
				},
			},
			expected: false,
		},
		{
			name: "Non-Snowflake IdP",
			oauthClient: &oauthClient{
				cfg: &Config{
					Host:                  "example.snowflakecomputing.com",
					OauthClientID:         "",
					OauthClientSecret:     "",
					OauthAuthorizationURL: "https://example.com/oauth/authorize",
					OauthTokenRequestURL:  "https://example.com/oauth/token",
				},
			},
			expected: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := test.oauthClient.eligibleForDefaultClientCredentials()
			if result != test.expected {
				t.Errorf("expected %v, got %v", test.expected, result)
			}
		})
	}
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
