package gosnowflake

import (
	"context"
	"errors"
	"fmt"
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
	timeout := defaultExternalBrowserTimeout
	sr := &snowflakeRestful{
		Protocol:         "https",
		Host:             "abc.com",
		Port:             443,
		FuncPostAuthSAML: postAuthExternalBrowserError,
		TokenAccessor:    getSimpleTokenAccessor(),
	}
	lease, _ := credentialsStorage.acquireLease()
	_, _, err := authenticateByExternalBrowser(context.Background(), lease, sr, authenticator, application, account, user, timeout, ConfigBoolTrue)
	if err == nil {
		t.Fatal("should have failed.")
	}
	sr.FuncPostAuthSAML = postAuthExternalBrowserFail
	_, _, err = authenticateByExternalBrowser(context.Background(), lease, sr, authenticator, application, account, user, timeout, ConfigBoolTrue)
	if err == nil {
		t.Fatal("should have failed.")
	}
	sr.FuncPostAuthSAML = postAuthExternalBrowserFailWithCode
	_, _, err = authenticateByExternalBrowser(context.Background(), lease, sr, authenticator, application, account, user, timeout, ConfigBoolTrue)
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
	lease, _ := credentialsStorage.acquireLease()
	_, _, err := authenticateByExternalBrowser(context.Background(), lease, sr, authenticator, application, account, user, timeout, ConfigBoolTrue)
	assertEqualE(t, err.Error(), "authentication timed out", err.Error())
}

func Test_createLocalTCPListener(t *testing.T) {
	listener, err := createLocalTCPListener(context.Background(), 0)
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

type nonInteractiveSamlResponseProvider struct {
	t *testing.T
}

func (provider *nonInteractiveSamlResponseProvider) run(url string) (string, error) {
	go func() {
		resp, err := http.Get(url)
		assertNilF(provider.t, err)
		assertEqualE(provider.t, resp.StatusCode, http.StatusOK)
	}()
	return "", nil
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
		select {
		case l.accepted <- struct{}{}:
		default:
		}
	}
	return conn, err
}

func TestExternalBrowserCallbackIgnoresIdleConnection(t *testing.T) {
	for _, initialRequest := range []string{
		"",
		"GET /?token=incomplete HTTP/1.1\r\nHost:",
		"POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 100\r\n\r\n{",
	} {
		t.Run(fmt.Sprintf("initialBytes=%d", len(initialRequest)), func(t *testing.T) {
			listener, err := createLocalTCPListener(context.Background(), 0)
			assertNilF(t, err)
			defer listener.Close()
			observed := &externalBrowserObservedListener{listener, make(chan struct{}, 4)}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			result := make(chan externalBrowserCallbackResult, 1)
			go func() {
				token, err := receiveExternalBrowserCallback(ctx, observed, "Go")
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
				assertFalseF(t, true, "idle connection was not accepted")
			}

			client := &http.Client{Timeout: time.Second}
			baseURL := "http://" + listener.Addr().String()
			expected := strings.Repeat("saml", 4096) + "+/=%2B"
			resp, err := client.Get(baseURL + "/?token=" + url.QueryEscape(expected))
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
			}
			assertNotNilF(t, err, "callback listener remained open after authentication")
		})
	}
}

func TestExternalBrowserCallbackRejectsInvalidRequests(t *testing.T) {
	listener, err := createLocalTCPListener(context.Background(), 0)
	assertNilF(t, err)
	defer listener.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result := startExternalBrowserCallback(ctx, listener)

	client := &http.Client{Timeout: time.Second}
	baseURL := "http://" + listener.Addr().String()
	for _, tc := range []struct {
		method, path string
		status       int
	}{
		{http.MethodGet, "/favicon.ico", http.StatusBadRequest},
		{http.MethodGet, "/?other=1", http.StatusBadRequest},
		{http.MethodPost, "/?token=abc", http.StatusMethodNotAllowed},
	} {
		req, err := http.NewRequest(tc.method, baseURL+tc.path, nil)
		assertNilF(t, err)
		resp, err := client.Do(req)
		assertNilF(t, err)
		resp.Body.Close()
		assertEqualF(t, resp.StatusCode, tc.status)
	}

	// The receiver keeps waiting after invalid requests.
	resp, err := client.Get(baseURL + "/?token=abc")
	assertNilF(t, err)
	resp.Body.Close()
	assertEqualF(t, resp.StatusCode, http.StatusOK)
	callback := waitExternalBrowserCallback(t, result)
	assertNilF(t, callback.err)
	assertEqualF(t, callback.token, "abc")
}

func startExternalBrowserCallback(ctx context.Context, listener net.Listener) chan externalBrowserCallbackResult {
	result := make(chan externalBrowserCallbackResult, 1)
	go func() {
		token, err := receiveExternalBrowserCallback(ctx, listener, "Go")
		result <- externalBrowserCallbackResult{token: token, err: err}
	}()
	return result
}

func waitExternalBrowserCallback(t *testing.T, result chan externalBrowserCallbackResult) externalBrowserCallbackResult {
	t.Helper()
	select {
	case callback := <-result:
		return callback
	case <-time.After(5 * time.Second):
		t.Fatal("external browser callback did not return")
		return externalBrowserCallbackResult{}
	}
}

func assertExternalBrowserSocketClosed(t *testing.T, conn net.Conn) {
	t.Helper()
	assertNilF(t, conn.SetReadDeadline(time.Now().Add(time.Second)))
	_, err := io.Copy(io.Discard, conn)
	netErr, ok := err.(net.Error)
	assertFalseF(t, ok && netErr.Timeout(), "callback connection remained open")
}

func TestExternalBrowserCallbackCancellationClosesPartialRequest(t *testing.T) {
	listener, err := createLocalTCPListener(context.Background(), 0)
	assertNilF(t, err)
	defer listener.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := startExternalBrowserCallback(ctx, listener)
	conn, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second)
	assertNilF(t, err)
	defer conn.Close()
	_, err = io.WriteString(conn, "GET /?token=incomplete HTTP/1.1\r\nHost:")
	assertNilF(t, err)
	cancel()
	callback := waitExternalBrowserCallback(t, result)
	assertErrIsF(t, callback.err, context.Canceled)
	assertExternalBrowserSocketClosed(t, conn)
}
