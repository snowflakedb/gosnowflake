package gosnowflake

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/browser"
	errors2 "github.com/snowflakedb/gosnowflake/v2/internal/errors"
)

const (
	samlSuccessHTML = `<!DOCTYPE html><html><head><meta charset="UTF-8"/>
<title>SAML Response for Snowflake</title></head>
<body>
Your identity was confirmed and propagated to Snowflake %v.
You can close this window now and go back where you started from.
</body></html>`

	bufSize = 8192
)

// Builds a response to show to the user after successfully
// getting a response from Snowflake.
func buildResponse(body string) (bytes.Buffer, error) {
	t := &http.Response{
		Status:        "200 OK",
		StatusCode:    200,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Body:          io.NopCloser(bytes.NewBufferString(body)),
		ContentLength: int64(len(body)),
		Request:       nil,
		Header:        make(http.Header),
	}
	var b bytes.Buffer
	err := t.Write(&b)
	return b, err
}

// This opens a socket that listens on all available unicast
// and any anycast IP addresses locally. By specifying "0", we are
// able to bind to a free port.
func createLocalTCPListener(port int) (*net.TCPListener, error) {
	logger.Debugf("creating local TCP listener on port %v", port)
	allAddressesListener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%v", port))
	if err != nil {
		logger.Warnf("error while setting up 0.0.0.0 listener: %v", err)
		return nil, err
	}
	logger.Debug("Closing 0.0.0.0 tcp listener")
	if err := allAddressesListener.Close(); err != nil {
		logger.Errorf("error while closing TCP listener. %v", err)
		return nil, err
	}

	l, err := net.Listen("tcp", fmt.Sprintf("localhost:%v", port))
	if err != nil {
		logger.Warnf("error while setting up listener: %v", err)
		return nil, err
	}

	tcpListener, ok := l.(*net.TCPListener)
	if !ok {
		return nil, fmt.Errorf("failed to assert type as *net.TCPListener")
	}

	return tcpListener, nil
}

// Opens a browser window (or new tab) with the configured login Url.
// This can / will fail if running inside a shell with no display, ie
// ssh'ing into a box attempting to authenticate via external browser.
func openBrowser(browserURL string) error {
	parsedURL, err := url.ParseRequestURI(browserURL)
	if err != nil {
		logger.Errorf("error parsing url %v, err: %v", browserURL, err)
		return err
	}
	if parsedURL.Scheme != "http" && parsedURL.Scheme != "https" {
		return fmt.Errorf("invalid browser URL: %v", browserURL)
	}
	err = browser.OpenURL(browserURL)
	if err != nil {
		logger.Errorf("failed to open a browser. err: %v", err)
		return err
	}
	return nil
}

// Gets the IDP Url and Proof Key from Snowflake.
// Note: FuncPostAuthSaml will return a fully qualified error if
// there is something wrong getting data from Snowflake.
func getIdpURLProofKey(
	ctx context.Context,
	sr *snowflakeRestful,
	authenticator string,
	application string,
	account string,
	user string,
	callbackPort int) (string, string, error) {

	headers := make(map[string]string)
	headers[httpHeaderContentType] = headerContentTypeApplicationJSON
	headers[httpHeaderAccept] = headerContentTypeApplicationJSON
	headers[httpHeaderUserAgent] = userAgent

	clientEnvironment := newAuthRequestClientEnvironment()
	clientEnvironment.Application = application

	requestMain := authRequestData{
		ClientAppID:             clientType,
		ClientAppVersion:        SnowflakeGoDriverVersion,
		AccountName:             account,
		LoginName:               user,
		ClientEnvironment:       clientEnvironment,
		Authenticator:           authenticator,
		BrowserModeRedirectPort: strconv.Itoa(callbackPort),
	}

	authRequest := authRequest{
		Data: requestMain,
	}

	jsonBody, err := json.Marshal(authRequest)
	if err != nil {
		logger.WithContext(ctx).Errorf("failed to serialize json. err: %v", err)
		return "", "", err
	}

	respd, err := sr.FuncPostAuthSAML(ctx, sr, headers, jsonBody, sr.LoginTimeout)
	if err != nil {
		return "", "", err
	}
	if !respd.Success {
		logger.WithContext(ctx).Error("Authentication FAILED")
		sr.TokenAccessor.SetTokens("", "", -1)
		code, err := strconv.Atoi(respd.Code)
		if err != nil {
			return "", "", err
		}
		return "", "", &SnowflakeError{
			Number:   code,
			SQLState: SQLStateConnectionRejected,
			Message:  respd.Message,
		}
	}
	return respd.Data.SSOURL, respd.Data.ProofKey, nil
}

// Gets the login URL for multiple SAML
func getLoginURL(sr *snowflakeRestful, user string, callbackPort int) (string, string, error) {
	proofKey := generateProofKey()

	params := &url.Values{}
	params.Add("login_name", user)
	params.Add("browser_mode_redirect_port", strconv.Itoa(callbackPort))
	params.Add("proof_key", proofKey)
	url := sr.getFullURL(consoleLoginRequestPath, params)

	return url.String(), proofKey, nil
}

func generateProofKey() string {
	randomness := getSecureRandom(32)
	return base64.StdEncoding.WithPadding(base64.StdPadding).EncodeToString(randomness)
}

type authenticateByExternalBrowserResult struct {
	escapedSamlResponse []byte
	proofKey            []byte
	err                 error
}

func authenticateByExternalBrowser(ctx context.Context, sr *snowflakeRestful, authenticator string, application string,
	account string, user string, externalBrowserTimeout time.Duration, disableConsoleLogin ConfigBool) ([]byte, []byte, error) {
	authCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	resultChan := make(chan authenticateByExternalBrowserResult, 1)
	go GoroutineWrapper(
		authCtx,
		func() {
			resultChan <- doAuthenticateByExternalBrowser(authCtx, sr, authenticator, application, account, user, disableConsoleLogin)
		},
	)
	select {
	case <-time.After(externalBrowserTimeout):
		return nil, nil, errors.New("authentication timed out")
	case result := <-resultChan:
		return result.escapedSamlResponse, result.proofKey, result.err
	}
}

// Authentication by an external browser takes place via the following:
//   - the golang snowflake driver communicates to Snowflake that the user wishes to
//     authenticate via external browser
//   - snowflake sends back the IDP Url configured at the Snowflake side for the
//     provided account, or use the multiple SAML way via console login
//   - the default browser is opened to that URL
//   - user authenticates at the IDP, and is redirected to Snowflake
//   - Snowflake directs the user back to the driver
//   - authenticate is complete!
func doAuthenticateByExternalBrowser(ctx context.Context, sr *snowflakeRestful, authenticator string, application string, account string, user string, disableConsoleLogin ConfigBool) authenticateByExternalBrowserResult {
	l, err := createLocalTCPListener(0)
	if err != nil {
		return authenticateByExternalBrowserResult{nil, nil, err}
	}
	defer func() {
		if closeErr := l.Close(); closeErr != nil && !errors.Is(closeErr, net.ErrClosed) {
			logger.Errorf("error while closing TCP listener for external browser (%v). %v", l.Addr().String(), closeErr)
		}
	}()

	callbackPort := l.Addr().(*net.TCPAddr).Port

	var loginURL string
	var proofKey string
	if disableConsoleLogin == ConfigBoolTrue {
		// Gets the IDP URL and Proof Key from Snowflake
		loginURL, proofKey, err = getIdpURLProofKey(ctx, sr, authenticator, application, account, user, callbackPort)
	} else {
		// Multiple SAML way to do authentication via console login
		loginURL, proofKey, err = getLoginURL(sr, user, callbackPort)
	}

	if err != nil {
		return authenticateByExternalBrowserResult{nil, nil, err}
	}

	if err = defaultSamlResponseProvider().run(loginURL); err != nil {
		return authenticateByExternalBrowserResult{nil, nil, err}
	}

	encodedSamlResponse, err := waitForSamlResponse(ctx, l, application)
	if err != nil {
		return authenticateByExternalBrowserResult{nil, nil, err}
	}

	escapedSamlResponse, err := url.QueryUnescape(encodedSamlResponse)
	if err != nil {
		logger.WithContext(ctx).Errorf("unable to unescape saml response. err: %v", err)
		return authenticateByExternalBrowserResult{nil, nil, err}
	}
	return authenticateByExternalBrowserResult{[]byte(escapedSamlResponse), []byte(proofKey), nil}
}

func waitForSamlResponse(ctx context.Context, l net.Listener, application string) (string, error) {
	encodedChan := make(chan string, 1)
	errChan := make(chan error, 1)

	server := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			encoded, ok := encodedTokenFromRequest(r)
			if !ok {
				http.Error(w, "invalid external-browser callback", http.StatusBadRequest)
				return
			}

			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			w.Header().Set("Connection", "close")
			w.WriteHeader(http.StatusOK)
			if _, err := io.WriteString(w, fmt.Sprintf(samlSuccessHTML, application)); err != nil {
				logger.WithContext(ctx).Debugf("failed to write external-browser success response: %v", err)
			}
			if err := http.NewResponseController(w).Flush(); err != nil {
				logger.WithContext(ctx).Debugf("failed to flush external-browser success response: %v", err)
			}

			select {
			case encodedChan <- encoded:
			default:
			}
		}),
	}
	defer func() {
		_ = server.Close()
	}()

	go func() {
		if err := server.Serve(l); err != nil && !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, net.ErrClosed) {
			select {
			case errChan <- &SnowflakeError{
				Number:      ErrFailedToGetExternalBrowserResponse,
				SQLState:    SQLStateConnectionRejected,
				Message:     errors2.ErrMsgFailedToGetExternalBrowserResponse,
				MessageArgs: []any{err},
			}:
			default:
			}
		}
	}()

	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case encoded := <-encodedChan:
		return encoded, nil
	case err := <-errChan:
		return "", err
	}
}

// encodedTokenFromRequest returns the token without URL-decoding it. The caller
// performs exactly one QueryUnescape after the callback has been selected.
func encodedTokenFromRequest(r *http.Request) (string, bool) {
	if r.Method != http.MethodGet || r.URL.Path != "/" {
		return "", false
	}
	for _, field := range strings.Split(r.URL.RawQuery, "&") {
		name, value, found := strings.Cut(field, "=")
		if !found || value == "" {
			continue
		}
		decodedName, err := url.QueryUnescape(name)
		if err == nil && decodedName == "token" {
			return value, true
		}
	}
	return "", false
}

type samlResponseProvider interface {
	run(url string) error
}

type externalBrowserSamlResponseProvider struct {
}

func (e externalBrowserSamlResponseProvider) run(url string) error {
	return openBrowser(url)
}

var defaultSamlResponseProvider = func() samlResponseProvider {
	return &externalBrowserSamlResponseProvider{}
}
