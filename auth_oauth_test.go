package gosnowflake

import (
	"context"
	"errors"
	"golang.org/x/oauth2"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"
)

func TestUnitOAuthAuthorizationCode(t *testing.T) {
	client, err := newOauthClient(context.Background(), &Config{
		Role:                  "ANALYST",
		OauthClientID:         "testClientId",
		OauthClientSecret:     "testClientSecret",
		OauthAuthorizationURL: wiremock.baseURL() + "/oauth/authorize",
		OauthTokenRequestURL:  wiremock.baseURL() + "/oauth/token",
		OauthRedirectURI:      "http://localhost:1234/snowflake/oauth-redirect",
	})
	assertNilF(t, err)

	t.Run("Success", func(t *testing.T) {
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

	t.Run("InvalidState", func(t *testing.T) {
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
}

type nonInteractiveAuthorizationCodeProvider struct {
	t               *testing.T
	tamperWithState bool
	triggerError    string
	responseBody    string
	mu              sync.Mutex
}

func (provider *nonInteractiveAuthorizationCodeProvider) run(authorizationURL string) error {
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
