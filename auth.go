// Copyright (c) 2017-2018 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/satori/go.uuid"
)

const (
	clientType = "Go"
)

const (
	externalBrowserAuthenticator = "EXTERNALBROWSER"
	oAuthAuthenticator           = "OAUTH"
)

// platform consists of compiler, OS and architecture type in string
var platform = fmt.Sprintf("%v-%v-%v", runtime.Compiler, runtime.GOOS, runtime.GOARCH)

// userAgent shows up in User-Agent HTTP header
var userAgent = fmt.Sprintf("%v/%v/%v/%v", clientType, SnowflakeGoDriverVersion, runtime.Version(), platform)

type authRequestClientEnvironment struct {
	Application string `json:"APPLICATION"`
	OsVersion   string `json:"OS_VERSION"`
}
type authRequestData struct {
	ClientAppID             string                       `json:"CLIENT_APP_ID"`
	ClientAppVersion        string                       `json:"CLIENT_APP_VERSION"`
	SvnRevision             string                       `json:"SVN_REVISION"`
	AccountName             string                       `json:"ACCOUNT_NAME"`
	LoginName               string                       `json:"LOGIN_NAME,omitempty"`
	Password                string                       `json:"PASSWORD,omitempty"`
	RawSAMLResponse         string                       `json:"RAW_SAML_RESPONSE,omitempty"`
	ExtAuthnDuoMethod       string                       `json:"EXT_AUTHN_DUO_METHOD,omitempty"`
	Passcode                string                       `json:"PASSCODE,omitempty"`
	Authenticator           string                       `json:"AUTHENTICATOR,omitempty"`
	SessionParameters       map[string]string            `json:"SESSION_PARAMETERS,omitempty"`
	ClientEnvironment       authRequestClientEnvironment `json:"CLIENT_ENVIRONMENT"`
	BrowserModeRedirectPort string                       `json:"BROWSER_MODE_REDIRECT_PORT,omitempty"`
	ProofKey                string                       `json:"PROOF_KEY,omitempty"`
	Token                   string                       `json:"TOKEN,omitempty"`
}
type authRequest struct {
	Data authRequestData `json:"data"`
}

type nameValueParameter struct {
	Name  string      `json:"name"`
	Value interface{} `json:"value"`
}

type authResponseSessionInfo struct {
	DatabaseName  string `json:"databaseName"`
	SchemaName    string `json:"schemaName"`
	WarehouseName string `json:"warehouseName"`
	RoleName      string `json:"roleName"`
}

type authResponseMain struct {
	Token                   string                  `json:"token,omitempty"`
	ValidityInSeconds       time.Duration           `json:"validityInSeconds,omitempty"`
	MasterToken             string                  `json:"masterToken,omitempty"`
	MasterValidityInSeconds time.Duration           `json:"masterValidityInSeconds"`
	DisplayUserName         string                  `json:"displayUserName"`
	ServerVersion           string                  `json:"serverVersion"`
	FirstLogin              bool                    `json:"firstLogin"`
	RemMeToken              string                  `json:"remMeToken"`
	RemMeValidityInSeconds  time.Duration           `json:"remMeValidityInSeconds"`
	HealthCheckInterval     time.Duration           `json:"healthCheckInterval"`
	NewClientForUpgrade     string                  `json:"newClientForUpgrade"`
	SessionID               int                     `json:"sessionId"`
	Parameters              []nameValueParameter    `json:"parameters"`
	SessionInfo             authResponseSessionInfo `json:"sessionInfo"`
	TokenURL                string                  `json:"tokenUrl,omitempty"`
	SSOURL                  string                  `json:"ssoUrl,omitempty"`
	ProofKey                string                  `json:"proofKey,omitempty"`
}
type authResponse struct {
	Data    authResponseMain `json:"data"`
	Message string           `json:"message"`
	Code    string           `json:"code"`
	Success bool             `json:"success"`
}

func postAuth(
	sr *snowflakeRestful,
	params *url.Values,
	headers map[string]string,
	body []byte,
	timeout time.Duration) (
	data *authResponse, err error) {
	params.Add("requestId", uuid.NewV4().String())
	fullURL := fmt.Sprintf(
		"%s://%s:%d%s", sr.Protocol, sr.Host, sr.Port,
		"/session/v1/login-request?"+params.Encode())
	glog.V(2).Infof("full URL: %v", fullURL)
	resp, err := sr.FuncPost(context.TODO(), sr, fullURL, headers, body, timeout, true)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		var respd authResponse
		err = json.NewDecoder(resp.Body).Decode(&respd)
		if err != nil {
			glog.V(1).Infof("failed to decode JSON. err: %v", err)
			glog.Flush()
			return nil, err
		}
		return &respd, nil
	}
	switch resp.StatusCode {
	case http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		// service availability or connectivity issue. Most likely server side issue.
		return nil, &SnowflakeError{
			Number:      ErrCodeServiceUnavailable,
			SQLState:    SQLStateConnectionWasNotEstablished,
			Message:     errMsgServiceUnavailable,
			MessageArgs: []interface{}{resp.StatusCode, fullURL},
		}
	case http.StatusUnauthorized, http.StatusForbidden:
		// failed to connect to db. account name may be wrong
		return nil, &SnowflakeError{
			Number:      ErrCodeFailedToConnect,
			SQLState:    SQLStateConnectionRejected,
			Message:     errMsgFailedToConnect,
			MessageArgs: []interface{}{resp.StatusCode, fullURL},
		}
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.V(1).Infof("failed to extract HTTP response body. err: %v", err)
		glog.Flush()
		return nil, err
	}
	glog.V(1).Infof("HTTP: %v, URL: %v, Body: %v", resp.StatusCode, fullURL, b)
	glog.V(1).Infof("Header: %v", resp.Header)
	glog.Flush()
	return nil, &SnowflakeError{
		Number:      ErrFailedToAuth,
		SQLState:    SQLStateConnectionRejected,
		Message:     errMsgFailedToAuth,
		MessageArgs: []interface{}{resp.StatusCode, fullURL},
	}
}

// authenticate is used to authenticate user to gain access to Snowflake database.
func authenticate(
	sr *snowflakeRestful,
	user string,
	password string,
	account string,
	database string,
	schema string,
	warehouse string,
	role string,
	passcode string,
	passcodeInPassword bool,
	application string,
	sessionParams map[string]*string,
	samlResponse []byte,
	mfaCallback string,
	passwordCallback string,
	proofKey []byte,
	token string) (resp *authResponseMain, err error) {
	glog.V(2).Info("authenticate")
	headers := make(map[string]string)
	headers["Content-Type"] = headerContentTypeApplicationJSON
	headers["accept"] = headerAcceptTypeApplicationSnowflake
	headers["User-Agent"] = userAgent

	clientEnvironment := authRequestClientEnvironment{
		Application: application,
		OsVersion:   platform,
	}

	sessionParameters := make(map[string]string)
	for k, v := range sessionParams {
		// upper casing to normalize keys
		sessionParameters[strings.ToUpper(k)] = *v
	}

	requestMain := authRequestData{
		ClientAppID:       clientType,
		ClientAppVersion:  SnowflakeGoDriverVersion,
		AccountName:       account,
		SessionParameters: sessionParameters,
		ClientEnvironment: clientEnvironment,
	}

	if !bytes.Equal(proofKey, []byte{}) {
		requestMain.ProofKey = string(proofKey)
		requestMain.Token = string(samlResponse)
		requestMain.LoginName = user
		requestMain.Authenticator = externalBrowserAuthenticator
	} else if !bytes.Equal(samlResponse, []byte{}) {
		requestMain.RawSAMLResponse = string(samlResponse)
	} else if token != "" {
		requestMain.LoginName = user
		requestMain.Authenticator = oAuthAuthenticator
		requestMain.Token = token
	} else {
		requestMain.LoginName = user
		requestMain.Password = password
		switch {
		case passcodeInPassword:
			requestMain.ExtAuthnDuoMethod = "passcode"
		case passcode != "":
			requestMain.Passcode = passcode
			requestMain.ExtAuthnDuoMethod = "passcode"
		}
	}

	authRequest := authRequest{
		Data: requestMain,
	}
	params := &url.Values{}
	if database != "" {
		params.Add("databaseName", database)
	}
	if schema != "" {
		params.Add("schemaName", schema)
	}
	if warehouse != "" {
		params.Add("warehouse", warehouse)
	}
	if role != "" {
		params.Add("roleName", role)
	}

	jsonBody, err := json.Marshal(authRequest)
	if err != nil {
		return
	}

	glog.V(2).Infof("PARAMS for Auth: %v, %v, %v, %v, %v, %v",
		params, sr.Protocol, sr.Host, sr.Port, sr.LoginTimeout, sr.Authenticator)

	respd, err := sr.FuncPostAuth(sr, params, headers, jsonBody, sr.LoginTimeout)
	if err != nil {
		return nil, err
	}
	if !respd.Success {
		glog.V(1).Infoln("Authentication FAILED")
		glog.Flush()
		sr.Token = ""
		sr.MasterToken = ""
		sr.SessionID = -1
		code, err := strconv.Atoi(respd.Code)
		if err != nil {
			code = -1
			return nil, err
		}
		return nil, &SnowflakeError{
			Number:   code,
			SQLState: SQLStateConnectionRejected,
			Message:  respd.Message,
		}
	}
	glog.V(2).Info("Authentication SUCCESS")
	sr.Token = respd.Data.Token
	sr.MasterToken = respd.Data.MasterToken
	sr.SessionID = respd.Data.SessionID
	return &respd.Data, nil
}
