package gosnowflake

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	sfconfig "github.com/snowflakedb/gosnowflake/v2/internal/config"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestGetTokenFromResponseFail(t *testing.T) {
	response := "GET /?fakeToken=fakeEncodedSamlToken HTTP/1.1\r\n" +
		"Host: localhost:54001\r\n" +
		"Connection: keep-alive\r\n" +
		"Upgrade-Insecure-Requests: 1\r\n" +
		"User-Agent: userAgentStr\r\n" +
		"Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8\r\n" +
		"Referer: https://myaccount.snowflakecomputing.com/fed/login\r\n" +
		"Accept-Encoding: gzip, deflate, br\r\n" +
		"Accept-Language: en-US,en;q=0.9\r\n\r\n"

	_, err := getTokenFromResponse(response)
	if err == nil {
		t.Errorf("Should have failed parsing the malformed response.")
	}
}

func TestGetTokenFromResponse(t *testing.T) {
	response := "GET /?token=GETtokenFromResponse HTTP/1.1\r\n" +
		"Host: localhost:54001\r\n" +
		"Connection: keep-alive\r\n" +
		"Upgrade-Insecure-Requests: 1\r\n" +
		"User-Agent: userAgentStr\r\n" +
		"Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8\r\n" +
		"Referer: https://myaccount.snowflakecomputing.com/fed/login\r\n" +
		"Accept-Encoding: gzip, deflate, br\r\n" +
		"Accept-Language: en-US,en;q=0.9\r\n\r\n"

	expected := "GETtokenFromResponse"

	token, err := getTokenFromResponse(response)
	if err != nil {
		t.Errorf("Failed to get the token. Err: %#v", err)
	}
	if token != expected {
		t.Errorf("Expected: %s, found: %s", expected, token)
	}
}

func TestBuildResponse(t *testing.T) {
	resp, err := buildResponse(fmt.Sprintf(samlSuccessHTML, "Go"))
	assertNilF(t, err)
	bytes := resp.Bytes()
	respStr := string(bytes[:])
	if !strings.Contains(respStr, "Your identity was confirmed and propagated to Snowflake Go.\nYou can close this window now and go back where you started from.") {
		t.Fatalf("failed to build response")
	}
}

func postAuthExternalBrowserError(_ context.Context, _ *snowflakeRestful, _ map[string]string, _ []byte, _ time.Duration) (*authResponse, error) {
	return &authResponse{}, errors.New("failed to get SAML response")
}

func postAuthExternalBrowserErrorDelayed(_ context.Context, _ *snowflakeRestful, _ map[string]string, _ []byte, _ time.Duration) (*authResponse, error) {
	time.Sleep(2 * time.Second)
	return &authResponse{}, errors.New("failed to get SAML response")
}

func postAuthExternalBrowserFail(_ context.Context, _ *snowflakeRestful, _ map[string]string, _ []byte, _ time.Duration) (*authResponse, error) {
	return &authResponse{
		Success: false,
		Message: "external browser auth failed",
	}, nil
}

func postAuthExternalBrowserFailWithCode(_ context.Context, _ *snowflakeRestful, _ map[string]string, _ []byte, _ time.Duration) (*authResponse, error) {
	return &authResponse{
		Success: false,
		Message: "failed to connect to db",
		Code:    "260008",
	}, nil
}

func TestUnitAuthenticateByExternalBrowser(t *testing.T) {
	authenticator := "externalbrowser"
	application := "testapp"
	account := "testaccount"
	user := "u"
	timeout := sfconfig.DefaultExternalBrowserTimeout
	sr := &snowflakeRestful{
		Protocol:         "https",
		Host:             "abc.com",
		Port:             443,
		FuncPostAuthSAML: postAuthExternalBrowserError,
		TokenAccessor:    getSimpleTokenAccessor(),
	}
	_, _, err := authenticateByExternalBrowser(context.Background(), sr, authenticator, application, account, user, timeout, ConfigBoolTrue)
	if err == nil {
		t.Fatal("should have failed.")
	}
	sr.FuncPostAuthSAML = postAuthExternalBrowserFail
	_, _, err = authenticateByExternalBrowser(context.Background(), sr, authenticator, application, account, user, timeout, ConfigBoolTrue)
	if err == nil {
		t.Fatal("should have failed.")
	}
	sr.FuncPostAuthSAML = postAuthExternalBrowserFailWithCode
	_, _, err = authenticateByExternalBrowser(context.Background(), sr, authenticator, application, account, user, timeout, ConfigBoolTrue)
	if err == nil {
		t.Fatal("should have failed.")
	}
	driverErr, ok := err.(*SnowflakeError)
	if !ok {
		t.Fatalf("should be snowflake error. err: %v", err)
	}
	if driverErr.Number != ErrCodeFailedToConnect {
		t.Fatalf("unexpected error code. expected: %v, got: %v", ErrCodeFailedToConnect, driverErr.Number)
	}
}

func TestAuthenticationTimeout(t *testing.T) {
	authenticator := "externalbrowser"
	application := "testapp"
	account := "testaccount"
	user := "u"
	timeout := 1 * time.Second
	sr := &snowflakeRestful{
		Protocol:         "https",
		Host:             "abc.com",
		Port:             443,
		FuncPostAuthSAML: postAuthExternalBrowserErrorDelayed,
		TokenAccessor:    getSimpleTokenAccessor(),
	}
	_, _, err := authenticateByExternalBrowser(context.Background(), sr, authenticator, application, account, user, timeout, ConfigBoolTrue)
	assertEqualE(t, err.Error(), "authentication timed out", err.Error())
}

func Test_createLocalTCPListener(t *testing.T) {
	listener, err := createLocalTCPListener(0)
	if err != nil {
		t.Fatalf("createLocalTCPListener() failed: %v", err)
	}
	if listener == nil {
		t.Fatal("createLocalTCPListener() returned nil listener")
	}

	// Close the listener after the test.
	defer listener.Close()
}

func TestUnitGetLoginURL(t *testing.T) {
	expectedScheme := "https"
	expectedHost := "abc.com:443"
	user := "u"
	callbackPort := 123
	sr := &snowflakeRestful{
		Protocol:      "https",
		Host:          "abc.com",
		Port:          443,
		TokenAccessor: getSimpleTokenAccessor(),
	}

	loginURL, proofKey, err := getLoginURL(sr, user, callbackPort)
	assertNilF(t, err, "failed to get login URL")
	assertNotNilF(t, len(proofKey), "proofKey should be non-empty string")

	urlPtr, err := url.Parse(loginURL)
	assertNilF(t, err, "failed to parse the login URL")
	assertEqualF(t, urlPtr.Scheme, expectedScheme)
	assertEqualF(t, urlPtr.Host, expectedHost)
	assertEqualF(t, urlPtr.Path, consoleLoginRequestPath)
	assertStringContainsF(t, urlPtr.RawQuery, "login_name")
	assertStringContainsF(t, urlPtr.RawQuery, "browser_mode_redirect_port")
	assertStringContainsF(t, urlPtr.RawQuery, "proof_key")
}

func TestExternalBrowserOriginMatchesAccount(t *testing.T) {
	tests := []struct {
		name       string
		accountURL string
		origin     string
		expected   bool
	}{
		{name: "exact HTTPS origin", accountURL: "https://account.example.com:443", origin: "https://account.example.com:443", expected: true},
		{name: "implicit HTTPS port", accountURL: "https://account.example.com:443", origin: "https://account.example.com", expected: true},
		{name: "implicit HTTP port", accountURL: "http://account.example.com:80", origin: "http://account.example.com", expected: true},
		{name: "trailing slash path", accountURL: "https://account.example.com:443", origin: "https://account.example.com/", expected: true},
		{name: "non-empty path", accountURL: "https://account.example.com:443", origin: "https://account.example.com/extra", expected: false},
		{name: "wrong scheme", accountURL: "https://account.example.com:443", origin: "http://account.example.com", expected: false},
		{name: "wrong host", accountURL: "https://account.example.com:443", origin: "https://other.example.com", expected: false},
		{name: "suffix lookalike", accountURL: "https://account.example.com:443", origin: "https://account.example.com.other.test", expected: false},
		{name: "wrong port", accountURL: "https://account.example.com:443", origin: "https://account.example.com:8443", expected: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			accountURL, err := url.Parse(tt.accountURL)
			assertNilF(t, err, "failed to parse account URL")
			assertEqualF(t, externalBrowserOriginMatchesAccount(tt.origin, accountURL), tt.expected, "origin match result")
		})
	}
}

func TestExternalBrowserCallbackOriginHandling(t *testing.T) {
	accountURL, err := url.Parse("https://account.example.com:443")
	assertNilF(t, err, "failed to parse account URL")

	tests := []struct {
		name       string
		request    string
		expectCORS bool
	}{
		{
			name:    "originless GET",
			request: "GET /?token=originless HTTP/1.1\r\nHost: localhost\r\n\r\n",
		},
		{
			name:    "null Origin",
			request: "GET /?token=null-origin HTTP/1.1\r\nHost: localhost\r\nOrigin: null\r\n\r\n",
		},
		{
			name:       "matching Origin",
			request:    "GET /?token=matching HTTP/1.1\r\nHost: localhost\r\nOrigin: https://account.example.com\r\n\r\n",
			expectCORS: true,
		},
		{
			name:    "body Origin ignored with CRLF",
			request: "GET /?token=body-crlf HTTP/1.1\r\nHost: localhost\r\nContent-Length: 35\r\n\r\nOrigin: https://foreign.example.com",
		},
		{
			name:    "body Origin ignored with LF",
			request: "GET /?token=body-lf HTTP/1.1\nHost: localhost\nContent-Length: 35\n\nOrigin: https://foreign.example.com",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			token, response := runExternalBrowserCallback(t, accountURL, tt.request)
			assertEqualF(t, token, strings.TrimPrefix(strings.Fields(tt.request)[1], "/?token="), "callback token")
			assertEqualF(t, strings.Contains(response, "Access-Control-Allow-Origin"), tt.expectCORS, "CORS response header")
		})
	}
}

func TestExternalBrowserPreflightValidation(t *testing.T) {
	accountURL, err := url.Parse("https://account.example.com:443")
	assertNilF(t, err, "failed to parse account URL")
	tests := []struct {
		name     string
		headers  string
		expected bool
	}{
		{
			name:     "POST with requested Content-Type",
			headers:  "Origin: https://account.example.com\r\nAccess-Control-Request-Method: POST\r\nAccess-Control-Request-Headers: Content-Type\r\n",
			expected: true,
		},
		{
			name:     "case-insensitive method and header",
			headers:  "Origin: https://account.example.com\r\nAccess-Control-Request-Method: post\r\nAccess-Control-Request-Headers: content-type\r\n",
			expected: true,
		},
		{
			name:     "requested headers omitted",
			headers:  "Origin: https://account.example.com\r\nAccess-Control-Request-Method: POST\r\n",
			expected: true,
		},
		{
			name:     "foreign Origin",
			headers:  "Origin: https://foreign.example.com\r\nAccess-Control-Request-Method: POST\r\nAccess-Control-Request-Headers: Content-Type\r\n",
			expected: false,
		},
		{
			name:     "wrong requested method",
			headers:  "Origin: https://account.example.com\r\nAccess-Control-Request-Method: GET\r\nAccess-Control-Request-Headers: Content-Type\r\n",
			expected: false,
		},
		{
			name:     "unsupported requested header",
			headers:  "Origin: https://account.example.com\r\nAccess-Control-Request-Method: POST\r\nAccess-Control-Request-Headers: Content-Type, X-Custom\r\n",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request, err := http.ReadRequest(bufio.NewReader(strings.NewReader(
				"OPTIONS / HTTP/1.1\r\nHost: localhost\r\n" + tt.headers + "\r\n",
			)))
			assertNilF(t, err, "failed to parse preflight request")
			assertEqualF(t, validExternalBrowserPreflight(request, accountURL), tt.expected, "preflight validity")
		})
	}
}

func TestExternalBrowserCallbackPreflight(t *testing.T) {
	accountURL, err := url.Parse("https://account.example.com:443")
	assertNilF(t, err, "failed to parse account URL")
	listener, err := createLocalTCPListener(0)
	assertNilF(t, err, "failed to create callback listener")
	defer listener.Close()

	result := startExternalBrowserCallback(context.Background(), listener, accountURL)

	response := sendExternalBrowserRequest(t, listener,
		"OPTIONS / HTTP/1.1\r\nHost: localhost\r\nOrigin: https://account.example.com\r\n"+
			"Access-Control-Request-Method: POST\r\nAccess-Control-Request-Headers: Content-Type\r\n\r\n")
	assertStringContainsF(t, response, "200 OK", "preflight status")
	assertStringContainsF(t, response, "Access-Control-Allow-Origin: https://account.example.com", "preflight origin")
	assertStringContainsF(t, response, "Access-Control-Allow-Methods: POST, GET, OPTIONS", "preflight method")
	assertStringContainsF(t, response, "Access-Control-Allow-Headers: Content-Type", "preflight headers")

	select {
	case <-result:
		assertFalseF(t, true, "preflight must not complete the callback listener")
	default:
	}

	sendExternalBrowserRequest(t, listener,
		"GET /?token=after-preflight HTTP/1.1\r\nHost: localhost\r\nOrigin: https://account.example.com\r\n\r\n")
	callback := <-result
	assertNilF(t, callback.err, "callback failed")
	assertEqualF(t, callback.token, "after-preflight", "callback token")
}

func TestExternalBrowserCallbackMatchingOriginJSONPost(t *testing.T) {
	accountURL, err := url.Parse("https://account.example.com:443")
	assertNilF(t, err, "failed to parse account URL")
	body := `{"token":"json-token","consent":true}`
	request := fmt.Sprintf(
		"POST / HTTP/1.1\r\nHost: localhost\r\nOrigin: https://account.example.com\r\n"+
			"Content-Type: application/json\r\nContent-Length: %d\r\n\r\n%s",
		len(body), body,
	)
	token, response := runExternalBrowserCallback(t, accountURL, request)
	assertEqualF(t, token, "json-token", "callback token")
	assertStringContainsF(t, response, "200 OK", "POST callback status")
	assertStringContainsF(t, response, "Access-Control-Allow-Origin: https://account.example.com", "POST CORS origin")
}

func TestExternalBrowserCallbackTrailingSlashOrigin(t *testing.T) {
	accountURL, err := url.Parse("https://account.example.com:443")
	assertNilF(t, err, "failed to parse account URL")
	token, response := runExternalBrowserCallback(t, accountURL,
		"GET /?token=slash-origin HTTP/1.1\r\nHost: localhost\r\nOrigin: https://account.example.com/\r\n\r\n")
	assertEqualF(t, token, "slash-origin", "callback token")
	assertStringContainsF(t, response, "200 OK", "trailing-slash origin status")
	assertStringContainsF(t, response, "Access-Control-Allow-Origin: https://account.example.com/", "trailing-slash CORS origin")
}

func TestTokenFromPostCallbackBody(t *testing.T) {
	assertEqualF(t, tokenFromJSONCallbackBody([]byte(`{"token":"json-token","consent":true}`)), "json-token", "JSON token")
	assertEqualF(t, tokenFromJSONCallbackBody([]byte(`{"token":""}`)), "", "empty JSON token")
	assertEqualF(t, tokenFromJSONCallbackBody([]byte(`not-json`)), "", "invalid JSON")
	assertEqualF(t, tokenFromFormCallbackBody("token=form-token&extra=val"), "form-token", "form token")
	assertEqualF(t, tokenFromFormCallbackBody("extra=val"), "", "form without token")
	assertEqualF(t, tokenFromFormCallbackBody("token=hello%20world"), "hello world", "form token URL-decoded")
	assertEqualF(t, tokenFromFormCallbackBody("token=hello%2Bworld"), "hello+world", "form token plus sign decoded")
}

func TestExternalBrowserCallbackPostRequiresMatchingOrigin(t *testing.T) {
	accountURL, err := url.Parse("https://account.example.com:443")
	assertNilF(t, err, "failed to parse account URL")
	body := `{"token":"post-token"}`

	tests := []struct {
		name    string
		headers string
	}{
		{
			name:    "no Origin header",
			headers: "",
		},
		{
			name:    "null Origin",
			headers: "Origin: null\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			listener, err := createLocalTCPListener(0)
			assertNilF(t, err, "failed to create callback listener")
			defer listener.Close()

			result := startExternalBrowserCallback(context.Background(), listener, accountURL)

			request := fmt.Sprintf(
				"POST / HTTP/1.1\r\nHost: localhost\r\n%sContent-Type: application/json\r\nContent-Length: %d\r\n\r\n%s",
				tt.headers, len(body), body,
			)
			response := sendExternalBrowserRequest(t, listener, request)
			assertStringContainsF(t, response, "403 Forbidden", "POST without matching Origin must be rejected")

			select {
			case <-result:
				assertFalseF(t, true, "POST without matching Origin must not complete the callback listener")
			default:
			}

			sendExternalBrowserRequest(t, listener,
				fmt.Sprintf(
					"POST / HTTP/1.1\r\nHost: localhost\r\nOrigin: https://account.example.com\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n%s",
					len(body), body,
				),
			)
			callback := waitExternalBrowserCallback(t, result)
			assertNilF(t, callback.err, "callback failed")
			assertEqualF(t, callback.token, "post-token", "callback token after rejected request")
		})
	}
}

func TestExternalBrowserCallbackForeignOriginThenValid(t *testing.T) {
	accountURL, err := url.Parse("https://account.example.com:443")
	assertNilF(t, err, "failed to parse account URL")
	listener, err := createLocalTCPListener(0)
	assertNilF(t, err, "failed to create callback listener")
	defer listener.Close()

	result := startExternalBrowserCallback(context.Background(), listener, accountURL)

	sendExternalBrowserRequest(t, listener,
		"GET /?token=foreign HTTP/1.1\r\nHost: localhost\r\nOrigin: https://foreign.example.com\r\n\r\n")
	select {
	case <-result:
		assertFalseF(t, true, "foreign Origin must not complete the callback listener")
	default:
	}

	sendExternalBrowserRequest(t, listener,
		"GET /?token=valid HTTP/1.1\r\nHost: localhost\r\nOrigin: https://account.example.com\r\n\r\n")
	callback := <-result
	assertNilF(t, callback.err, "callback failed")
	assertEqualF(t, callback.token, "valid", "callback token")
}

func TestExternalBrowserCallbackContinuesAfterInvalidConnection(t *testing.T) {
	accountURL, err := url.Parse("https://account.example.com:443")
	assertNilF(t, err, "failed to parse account URL")

	tests := []struct {
		name           string
		invalidRequest string
		connectOnly    bool
	}{
		{name: "connect then close", connectOnly: true},
		{name: "malformed request", invalidRequest: "not an HTTP request\r\n\r\n"},
		{name: "GET without token", invalidRequest: "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"},
		{name: "favicon", invalidRequest: "GET /favicon.ico HTTP/1.1\r\nHost: localhost\r\n\r\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			listener, err := createLocalTCPListener(0)
			assertNilF(t, err, "failed to create callback listener")
			defer listener.Close()
			result := startExternalBrowserCallback(context.Background(), listener, accountURL)

			if tt.connectOnly {
				conn, err := net.Dial("tcp", listener.Addr().String())
				assertNilF(t, err, "failed to connect to callback listener")
				assertNilF(t, conn.Close(), "failed to close callback connection")
			} else {
				response := sendExternalBrowserRequest(t, listener, tt.invalidRequest)
				assertStringContainsF(t, response, "400 Bad Request", "invalid callback response")
			}

			sendExternalBrowserRequest(t, listener,
				"GET /?token=valid-after-invalid HTTP/1.1\r\nHost: localhost\r\n\r\n")
			callback := waitExternalBrowserCallback(t, result)
			assertNilF(t, callback.err, "callback failed")
			assertEqualF(t, callback.token, "valid-after-invalid", "callback token")
		})
	}
}

func TestExternalBrowserCallbackTimeoutClosesSlowReader(t *testing.T) {
	accountURL, err := url.Parse("https://account.example.com:443")
	assertNilF(t, err, "failed to parse account URL")
	listener, err := createLocalTCPListener(0)
	assertNilF(t, err, "failed to create callback listener")

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	result := startExternalBrowserCallback(ctx, listener, accountURL)

	conn, err := net.Dial("tcp", listener.Addr().String())
	assertNilF(t, err, "failed to connect to callback listener")
	_, err = io.WriteString(conn, "GET /?token=slow HTTP/1.1\r\nHost:")
	assertNilF(t, err, "failed to write partial callback request")

	started := time.Now()
	callback := waitExternalBrowserCallback(t, result)
	assertErrIsF(t, callback.err, context.DeadlineExceeded, "callback should stop at its deadline")
	assertTrueF(t, time.Since(started) < time.Second, "callback deadline should promptly unblock the read")

	err = listener.SetDeadline(time.Now().Add(time.Second))
	if err == nil {
		_, err = listener.Accept()
	}
	assertTrueF(t, errors.Is(err, net.ErrClosed), "callback listener should be closed after timeout")
	assertNilF(t, conn.Close(), "failed to close slow callback connection")
}

func TestExternalBrowserCallbackCancellationClosesSlowReader(t *testing.T) {
	accountURL, err := url.Parse("https://account.example.com:443")
	assertNilF(t, err, "failed to parse account URL")
	listener, err := createLocalTCPListener(0)
	assertNilF(t, err, "failed to create callback listener")

	ctx, cancel := context.WithCancel(context.Background())
	result := startExternalBrowserCallback(ctx, listener, accountURL)
	conn, err := net.Dial("tcp", listener.Addr().String())
	assertNilF(t, err, "failed to connect to callback listener")
	_, err = io.WriteString(conn, "GET /?token=slow HTTP/1.1\r\nHost:")
	assertNilF(t, err, "failed to write partial callback request")

	cancel()
	callback := waitExternalBrowserCallback(t, result)
	assertErrIsF(t, callback.err, context.Canceled, "callback should stop when canceled")
	assertNilF(t, conn.Close(), "failed to close slow callback connection")
}

type externalBrowserCallbackResult struct {
	token string
	err   error
}

func startExternalBrowserCallback(ctx context.Context, listener *net.TCPListener, accountURL *url.URL) <-chan externalBrowserCallbackResult {
	result := make(chan externalBrowserCallbackResult, 1)
	go func() {
		token, callbackErr := receiveExternalBrowserCallback(ctx, listener, accountURL, "Go")
		result <- externalBrowserCallbackResult{token: token, err: callbackErr}
	}()
	return result
}

func waitExternalBrowserCallback(t *testing.T, result <-chan externalBrowserCallbackResult) externalBrowserCallbackResult {
	t.Helper()
	select {
	case callback := <-result:
		return callback
	case <-time.After(2 * time.Second):
		assertFalseF(t, true, "timed out waiting for callback listener")
		return externalBrowserCallbackResult{}
	}
}

func runExternalBrowserCallback(t *testing.T, accountURL *url.URL, request string) (string, string) {
	t.Helper()
	listener, err := createLocalTCPListener(0)
	assertNilF(t, err, "failed to create callback listener")
	defer listener.Close()

	result := startExternalBrowserCallback(context.Background(), listener, accountURL)

	response := sendExternalBrowserRequest(t, listener, request)
	callback := waitExternalBrowserCallback(t, result)
	assertNilF(t, callback.err, "callback failed")
	return callback.token, response
}

func sendExternalBrowserRequest(t *testing.T, listener *net.TCPListener, request string) string {
	t.Helper()
	conn, err := net.Dial("tcp", listener.Addr().String())
	assertNilF(t, err, "failed to connect to callback listener")
	_, err = io.WriteString(conn, request)
	assertNilF(t, err, "failed to write callback request")
	response, err := io.ReadAll(conn)
	assertNilF(t, err, "failed to read callback response")
	assertNilF(t, conn.Close(), "failed to close callback connection")
	return string(response)
}

type nonInteractiveSamlResponseProvider struct {
	t *testing.T
}

func (provider *nonInteractiveSamlResponseProvider) run(url string) error {
	go func() {
		resp, err := http.Get(url)
		assertNilF(provider.t, err)
		assertEqualE(provider.t, resp.StatusCode, http.StatusOK)
	}()
	return nil
}

// Observe Accept so the real callback cannot accidentally win the race against
// the idle connection in the regression test.
type externalBrowserObservedListener struct {
	net.Listener
	accepted chan struct{}
}

func (l *externalBrowserObservedListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err == nil {
		l.accepted <- struct{}{}
	}
	return conn, err
}

func TestExternalBrowserCallbackIgnoresIdleConnection(t *testing.T) {
	for _, initialRequest := range []string{
		"",
		"GET /?token=incomplete HTTP/1.1\r\nHost:",
		"POST / HTTP/1.1\r\nHost: localhost\r\nOrigin: https://account.example.com\r\nContent-Length: 100\r\n\r\n{",
	} {
		t.Run(fmt.Sprintf("initialBytes=%d", len(initialRequest)), func(t *testing.T) {
			accountURL, err := url.Parse("https://account.example.com:443")
			assertNilF(t, err)
			listener, err := createLocalTCPListener(0)
			assertNilF(t, err)
			defer listener.Close()
			observed := &externalBrowserObservedListener{listener, make(chan struct{}, 4)}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			result := make(chan externalBrowserCallbackResult, 1)
			go func() {
				token, err := receiveExternalBrowserCallback(ctx, observed, accountURL, "Go")
				result <- externalBrowserCallbackResult{token: token, err: err}
			}()

			idle, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second)
			assertNilF(t, err)
			defer idle.Close()
			_, err = io.WriteString(idle, initialRequest)
			assertNilF(t, err)
			select {
			case <-observed.accepted:
			case <-ctx.Done():
				t.Fatal("idle connection was not accepted")
			}

			client := &http.Client{Timeout: time.Second}
			baseURL := "http://" + listener.Addr().String()
			// A browser must also be able to complete its CORS preflight while an
			// earlier connection is idle or waiting for the rest of its body.
			preflight, err := http.NewRequest(http.MethodOptions, baseURL, nil)
			assertNilF(t, err)
			preflight.Header.Set("Origin", "https://account.example.com")
			preflight.Header.Set("Access-Control-Request-Method", "POST")
			resp, err := client.Do(preflight)
			assertNilF(t, err, "idle connection blocked the preflight")
			resp.Body.Close()
			assertEqualF(t, resp.StatusCode, http.StatusOK)
			assertEqualF(t, resp.Header.Get("Access-Control-Allow-Origin"), "https://account.example.com")

			expected := strings.Repeat("saml", 4096) + "+/=%2B"
			resp, err = client.Get(baseURL + "/?token=" + url.QueryEscape(expected))
			assertNilF(t, err, "idle connection blocked the authentication callback")
			body, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			assertNilF(t, err)
			assertEqualF(t, resp.StatusCode, http.StatusOK)
			assertEqualF(t, string(body), fmt.Sprintf(samlSuccessHTML, "Go"))
			callback := waitExternalBrowserCallback(t, result)
			assertNilF(t, callback.err)
			assertEqualF(t, callback.token, url.QueryEscape(expected))
			assertExternalBrowserSocketClosed(t, idle)
			conn, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second)
			if err == nil {
				conn.Close()
				t.Fatal("callback listener remained open after authentication")
			}
		})
	}
}

func assertExternalBrowserSocketClosed(t *testing.T, conn net.Conn) {
	t.Helper()
	assertNilF(t, conn.SetReadDeadline(time.Now().Add(time.Second)))
	_, err := io.Copy(io.Discard, conn)
	if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
		t.Fatal("callback connection remained open")
	}
}

func TestExternalBrowserCallbackCancellationClosesPostBody(t *testing.T) {
	accountURL, err := url.Parse("https://account.example.com")
	assertNilF(t, err)
	listener, err := createLocalTCPListener(0)
	assertNilF(t, err)
	defer listener.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := startExternalBrowserCallback(ctx, listener, accountURL)
	conn, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second)
	assertNilF(t, err)
	defer conn.Close()
	_, err = io.WriteString(conn, "POST / HTTP/1.1\r\nHost: localhost\r\nOrigin: https://account.example.com\r\nContent-Length: 100\r\n\r\n{")
	assertNilF(t, err)
	cancel()
	callback := waitExternalBrowserCallback(t, result)
	assertErrIsF(t, callback.err, context.Canceled)
	assertExternalBrowserSocketClosed(t, conn)
}
