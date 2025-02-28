package gosnowflake

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

const (
	oauthSuccessHTML = `<!DOCTYPE html><html><head><meta charset="UTF-8"/>
<title>OAuth for Snowflake</title></head>
<body>
OAuth authentication completed successfully.
</body></html>`
)

type oauthClient struct {
	ctx context.Context
	cfg *Config

	port                int
	redirectURITemplate string

	authorizationCodeProviderFactory func() authorizationCodeProvider
}

func newOauthClient(ctx context.Context, cfg *Config) (*oauthClient, error) {
	port := 0
	if cfg.OauthRedirectURI != "" {
		uri, err := url.Parse(cfg.OauthRedirectURI)
		if err != nil {
			return nil, err
		}
		portStr := uri.Port()
		if portStr != "" {
			if port, err = strconv.Atoi(portStr); err != nil {
				return nil, err
			}
		}
	}

	redirectURITemplate := ""
	if cfg.OauthRedirectURI == "" {
		redirectURITemplate = "http://127.0.0.1:%v/"
	}

	client := &http.Client{
		Transport: getTransport(cfg),
	}
	return &oauthClient{
		ctx:                 context.WithValue(ctx, oauth2.HTTPClient, client),
		cfg:                 cfg,
		port:                port,
		redirectURITemplate: redirectURITemplate,
		authorizationCodeProviderFactory: func() authorizationCodeProvider {
			return &browserBasedAuthorizationCodeProvider{}
		},
	}, nil
}

func (oauthClient *oauthClient) authenticateByOAuthAuthorizationCode() (string, error) {
	authCodeProvider := oauthClient.authorizationCodeProviderFactory()

	successChan := make(chan []byte)
	errChan := make(chan error)
	responseBodyChan := make(chan string, 2)
	closeListenerChan := make(chan bool, 2)

	defer func() {
		closeListenerChan <- true
		close(successChan)
		close(errChan)
		close(responseBodyChan)
		close(closeListenerChan)
	}()

	logger.Debug("setting up TCP listener for authorization code redirect")
	tcpListener, callbackPort, err := oauthClient.setupListener()
	logger.Debugf("opening socket on port %v", callbackPort)
	if err != nil {
		return "", err
	}
	defer func(tcpListener *net.TCPListener) {
		<-closeListenerChan
		logger.Debug("closing tcp listener")
		if err := tcpListener.Close(); err != nil {
			logger.Warnf("error while closing TCP listener. %v", err)
		}
	}(tcpListener)

	go handleOAuthSocket(tcpListener, successChan, errChan, responseBodyChan, closeListenerChan)

	oauth2cfg := oauthClient.buildAuthorizationCodeConfig(callbackPort)
	codeVerifier := authCodeProvider.createCodeVerifier()
	state := authCodeProvider.createState()
	authorizationURL := oauth2cfg.AuthCodeURL(state, oauth2.S256ChallengeOption(codeVerifier))
	if err = authCodeProvider.run(authorizationURL); err != nil {
		responseBodyChan <- err.Error()
		closeListenerChan <- true
		return "", err
	}

	err = <-errChan
	if err != nil {
		responseBodyChan <- err.Error()
		return "", err
	}
	codeReqBytes := <-successChan

	codeReq, err := http.ReadRequest(bufio.NewReader(bytes.NewReader(codeReqBytes)))
	if err != nil {
		responseBodyChan <- err.Error()
		return "", err
	}

	return oauthClient.exchangeAccessToken(codeReq, state, oauth2cfg, codeVerifier, responseBodyChan)
}

func (oauthClient *oauthClient) setupListener() (*net.TCPListener, int, error) {
	tcpListener, err := createLocalTCPListener(oauthClient.port)
	if err != nil {
		return nil, 0, err
	}
	callbackPort := tcpListener.Addr().(*net.TCPAddr).Port
	return tcpListener, callbackPort, nil
}

func (oauthClient *oauthClient) exchangeAccessToken(codeReq *http.Request, state string, oauth2cfg *oauth2.Config, codeVerifier string, responseBodyChan chan string) (string, error) {
	queryParams := codeReq.URL.Query()
	errorMsg := queryParams.Get("error")
	if errorMsg != "" {
		errorDesc := queryParams.Get("error_description")
		errMsg := fmt.Sprintf("error while getting authentication from oauth: %v. Details: %v", errorMsg, errorDesc)
		responseBodyChan <- errMsg
		return "", errors.New(errMsg)
	}

	receivedState := queryParams.Get("state")
	if state != receivedState {
		errMsg := "invalid oauth state received"
		responseBodyChan <- errMsg
		return "", errors.New(errMsg)
	}

	code := queryParams.Get("code")
	token, err := oauth2cfg.Exchange(oauthClient.ctx, code, oauth2.VerifierOption(codeVerifier))
	if err != nil {
		responseBodyChan <- err.Error()
		return "", err
	}
	responseBodyChan <- oauthSuccessHTML
	return token.AccessToken, nil
}

func (oauthClient *oauthClient) buildAuthorizationCodeConfig(callbackPort int) *oauth2.Config {
	return &oauth2.Config{
		ClientID:     oauthClient.cfg.OauthClientID,
		ClientSecret: oauthClient.cfg.OauthClientSecret,
		RedirectURL:  oauthClient.buildRedirectURI(callbackPort),
		Scopes:       oauthClient.buildScopes(),
		Endpoint: oauth2.Endpoint{
			AuthURL:  oauthClient.cfg.OauthAuthorizationURL,
			TokenURL: oauthClient.cfg.OauthTokenRequestURL,
		},
	}
}

func (oauthClient *oauthClient) buildRedirectURI(port int) string {
	if oauthClient.cfg.OauthRedirectURI != "" {
		return oauthClient.cfg.OauthRedirectURI
	}
	return fmt.Sprintf(oauthClient.redirectURITemplate, port)
}

func (oauthClient *oauthClient) buildScopes() []string {
	if oauthClient.cfg.OauthScope == "" {
		return []string{"session:role:" + oauthClient.cfg.Role}
	}
	scopes := strings.Split(oauthClient.cfg.OauthScope, ",")
	for i, scope := range scopes {
		scopes[i] = strings.TrimSpace(scope)
	}
	return scopes
}

func handleOAuthSocket(tcpListener *net.TCPListener, successChan chan []byte, errChan chan error, responseBodyChan chan string, closeListenerChan chan bool) {
	conn, err := tcpListener.AcceptTCP()
	if err != nil {
		logger.Warnf("error creating socket. %v", err)
		return
	}
	defer conn.Close()
	var buf [bufSize]byte
	codeResp := bytes.NewBuffer(nil)
	for {
		readBytes, err := conn.Read(buf[:])
		if err == io.EOF {
			break
		}
		if err != nil {
			errChan <- err
			return
		}
		codeResp.Write(buf[0:readBytes])
		if readBytes < bufSize {
			break
		}
	}

	errChan <- nil
	successChan <- codeResp.Bytes()

	responseBody := <-responseBodyChan
	respToBrowser, err := buildResponse(responseBody)
	if err != nil {
		logger.Warnf("cannot create response to browser. %v", err)
	}
	_, err = conn.Write(respToBrowser.Bytes())
	if err != nil {
		logger.Warnf("cannot write response to browser. %v", err)
	}
	closeListenerChan <- true
}

type authorizationCodeProvider interface {
	run(authorizationURL string) error
	createState() string
	createCodeVerifier() string
}

type browserBasedAuthorizationCodeProvider struct {
}

func (provider *browserBasedAuthorizationCodeProvider) run(authorizationURL string) error {
	return openBrowser(authorizationURL)
}

func (provider *browserBasedAuthorizationCodeProvider) createState() string {
	return NewUUID().String()
}

func (provider *browserBasedAuthorizationCodeProvider) createCodeVerifier() string {
	return oauth2.GenerateVerifier()
}

func (oauthClient *oauthClient) authenticateByOAuthClientCredentials() (string, error) {
	oauth2Cfg := oauthClient.buildClientCredentialsConfig()
	token, err := oauth2Cfg.Token(oauthClient.ctx)
	if err != nil {
		return "", err
	}
	return token.AccessToken, nil
}

func (oauthClient *oauthClient) buildClientCredentialsConfig() *clientcredentials.Config {
	return &clientcredentials.Config{
		ClientID:     oauthClient.cfg.OauthClientID,
		ClientSecret: oauthClient.cfg.OauthClientSecret,
		TokenURL:     oauthClient.cfg.OauthTokenRequestURL,
		Scopes:       oauthClient.buildScopes(),
	}
}
