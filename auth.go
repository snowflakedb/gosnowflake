// Go Snowflake Driver - Snowflake driver for Go's database/sql package
//
// Copyright (c) 2017 Snowflake Computing Inc. All right reserved.
//

package gosnowflake

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/url"
	"runtime"
	"strconv"
	"time"

	"github.com/golang/glog"
)

const (
	clientType = "Go"
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
	ClientAppID       string                       `json:"CLIENT_APP_ID"`
	ClientAppVersion  string                       `json:"CLIENT_APP_VERSION"`
	SvnRevision       string                       `json:"SVN_REVISION"`
	AccoutName        string                       `json:"ACCOUNT_NAME"`
	LoginName         string                       `json:"LOGIN_NAME,omitempty"`
	Password          string                       `json:"PASSWORD,omitempty"`
	RawSAMLResponse   string                       `json:"RAW_SAML_RESPONSE,omitempty"`
	ExtAuthnDuoMethod string                       `json:"EXT_AUTHN_DUO_METHOD,omitempty"`
	Passcode          string                       `json:"PASSCODE,omitempty"`
	Authenticator     string                       `json:"AUTHENTICATOR,omitempty"`
	ClientEnvironment authRequestClientEnvironment `json:"CLIENT_ENVIRONMENT"`
}
type authRequest struct {
	Data authRequestData `json:"data"`
}

type authResponseParameter struct {
	Name  string          `json:"name"`
	Value json.RawMessage `json:"value"`
}

// AuthResponseSessionInfo includes the current database, schema, warehouse and role in the session.
type AuthResponseSessionInfo struct {
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
	NewClientForUpgrade     string                  `json:"newClientForUpgrade"` // TODO: what is datatype?
	SessionID               int                     `json:"sessionId"`
	Parameters              []authResponseParameter `json:"parameters"`
	SessionInfo             AuthResponseSessionInfo `json:"sessionInfo"`
	TokenURL                string                  `json:"tokenUrl,omitempty"`
	SSOURL                  string                  `json:"ssoUrl,omitempty"`
}
type authResponse struct {
	Data    authResponseMain `json:"data"`
	Message string           `json:"message"`
	Code    string           `json:"code"`
	Success bool             `json:"success"`
}

type authOKTARequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type authOKTAResponse struct {
	CookieToken string `json:"cookieToken"`
}

// authenticate is used to authenticate user to gain accesss to Snowflake database.
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
	samlResponse []byte,
	mfaCallback string,
	passwordCallback string) (resp *AuthResponseSessionInfo, err error) {
	glog.V(2).Info("authenticate")

	if sr.Token != "" && sr.MasterToken != "" {
		glog.V(2).Infoln("Tokens are already available.")
		return nil, nil
	}

	headers := make(map[string]string)
	headers["Content-Type"] = headerContentTypeApplicationJSON
	headers["accept"] = headerAcceptTypeApplicationSnowflake
	headers["User-Agent"] = userAgent

	clientEnvironment := authRequestClientEnvironment{
		Application: application,
		OsVersion:   platform,
	}

	requestMain := authRequestData{
		ClientAppID:       clientType,
		ClientAppVersion:  SnowflakeGoDriverVersion,
		AccoutName:        account,
		ClientEnvironment: clientEnvironment,
	}
	if bytes.Compare(samlResponse, []byte{}) != 0 {
		requestMain.RawSAMLResponse = string(samlResponse)
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

	glog.V(2).Infof("PARAMS for Auth: %v, %v", params, sr)
	respd, err := sr.PostAuth(params, headers, jsonBody, sr.LoginTimeout)
	if err != nil {
		// TODO: error handing, Forbidden 403, BadGateway 504, ServiceUnavailable 503
		return nil, err
	}
	if !respd.Success {
		glog.V(1).Infoln("Authentication FAILED")
		sr.Token = ""
		sr.MasterToken = ""
		sr.SessionID = -1
		code, err := strconv.Atoi(respd.Code)
		if err != nil {
			code = -1
			return nil, err
		}
		return nil, &SnowflakeError{
			Number:  code,
			Message: respd.Message,
		}
	}
	glog.V(2).Info("Authentication SUCCES")
	sr.Token = respd.Data.Token
	sr.MasterToken = respd.Data.MasterToken
	sr.SessionID = respd.Data.SessionID
	return &respd.Data.SessionInfo, nil
}

/*
authenticateBySAML authenticates a user by SAML
SAML Authentication
1.  query GS to obtain IDP token and SSO url
2.  IMPORTANT Client side validation:
	validate both token url and sso url contains same prefix
	(protocol + host + port) as the given authenticator url.
	Explanation:
	This provides a way for the user to 'authenticate' the IDP it is
	sending his/her credentials to.  Without such a check, the user could
	be coerced to provide credentials to an IDP impersonator.
3.  query IDP token url to authenticate and retrieve access token
4.  given access token, query IDP URL snowflake app to get SAML response
5.  IMPORTANT Client side validation:
	validate the post back url come back with the SAML response
	contains the same prefix as the Snowflake's server url, which is the
	intended destination url to Snowflake.
Explanation:
	This emulates the behavior of IDP initiated login flow in the user
	browser where the IDP instructs the browser to POST the SAML
	assertion to the specific SP endpoint.  This is critical in
	preventing a SAML assertion issued to one SP from being sent to
	another SP.
*/
func authenticateBySAML(
	sr *snowflakeRestful,
	authenticator string,
	application string,
	account string,
	user string,
	password string,
) (samlResponse []byte, err error) {
	glog.V(2).Info("step 1: query GS to obtain IDP token and SSO url")
	headers := make(map[string]string)
	headers["Content-Type"] = headerContentTypeApplicationJSON
	headers["accept"] = headerContentTypeApplicationJSON
	headers["User-Agent"] = userAgent

	clientEnvironment := authRequestClientEnvironment{
		Application: application,
		OsVersion:   platform,
	}
	requestMain := authRequestData{
		ClientAppID:       clientType,
		ClientAppVersion:  SnowflakeGoDriverVersion,
		AccoutName:        account,
		ClientEnvironment: clientEnvironment,
		Authenticator:     authenticator,
	}
	authRequest := authRequest{
		Data: requestMain,
	}
	params := &url.Values{}
	jsonBody, err := json.Marshal(authRequest)
	if err != nil {
		return
	}
	glog.V(2).Infof("PARAMS for Auth: %v, %v", params, sr)
	respd, err := sr.PostAuthSAML(headers, jsonBody, sr.LoginTimeout)
	if err != nil {
		return nil, err
	}
	if !respd.Success {
		glog.V(1).Infoln("Authentication FAILED")
		sr.Token = ""
		sr.MasterToken = ""
		sr.SessionID = -1
		code, err := strconv.Atoi(respd.Code)
		if err != nil {
			code = -1
			return nil, err
		}
		return nil, &SnowflakeError{
			Number:  code,
			Message: respd.Message,
		}
	}
	glog.V(2).Info("step 2: validate Token and SSO URL has the same prefix as authenticator")
	var b1, b2 bool
	if b1, err = isPrefixEqual(authenticator, respd.Data.TokenURL); err != nil {
		return nil, err
	}
	if b2, err = isPrefixEqual(authenticator, respd.Data.SSOURL); err != nil {
		return nil, err
	}
	if !b1 || !b2 {
		return nil, &SnowflakeError{
			Number:      ErrCodeIdpConnectionError,
			Message:     errMsgIdpConnectionError,
			MessageArgs: []interface{}{authenticator, respd.Data.TokenURL, respd.Data.SSOURL},
		}
	}
	glog.V(2).Info("step 3: query IDP token url to authenticate and retrieve access token")
	jsonBody, err = json.Marshal(authOKTARequest{
		Username: user,
		Password: password,
	})
	respa, err := sr.PostAuthOKTA(headers, jsonBody, respd.Data.TokenURL, sr.LoginTimeout)
	if err != nil {
		return nil, err
	}

	glog.V(2).Info("step 4: query IDP URL snowflake app to get SAML response")
	params = &url.Values{}
	params.Add("RelayState", "/some/deep/link")
	params.Add("onetimetoken", respa.CookieToken)

	headers = make(map[string]string)
	headers["accept"] = "*/*"
	bd, err := sr.GetSSO(params, headers, respd.Data.SSOURL, sr.LoginTimeout)
	if err != nil {
		return nil, err
	}
	glog.V(2).Info("step 5: validate post_back_url matches Snowflake URL")
	tgtURL, err := postBackURL(bd)
	if err != nil {
		return nil, err
	}
	fullURL := fmt.Sprintf("%s://%s:%d", sr.Protocol, sr.Host, sr.Port)
	glog.V(2).Infof("tgtURL: %v, origURL: %v", tgtURL, fullURL)
	if b2, err = isPrefixEqual(tgtURL, fullURL); err != nil {
		return nil, err
	}
	return bd, nil
}

func postBackURL(htmlData []byte) (string, error) {
	idx0 := bytes.Index(htmlData, []byte("<form"))
	if idx0 < 0 {
		return "", fmt.Errorf("failed to find a form tag in HTML response: %v", htmlData)
	}
	idx := bytes.Index(htmlData[idx0:], []byte("action=\""))
	if idx < 0 {
		return "", fmt.Errorf("failed to find action field in HTML response: %v", htmlData[idx0:])
	}
	idx += idx0
	endIdx := bytes.Index(htmlData[idx+8:], []byte("\""))
	if endIdx < 0 {
		return "", fmt.Errorf("failed to find the end of action field: %v", htmlData[idx+8:])
	}
	// fmt.Printf("%v", string(htmlData[idx+8:]))
	urlp := url.QueryEscape(string(htmlData[idx+8 : idx+8+endIdx]))
	return urlp, nil
}
func isPrefixEqual(url1 string, url2 string) (bool, error) {
	var err error
	var u1, u2 *url.URL
	u1, err = url.Parse(url1)
	if err != nil {
		return false, fmt.Errorf("failed to parse URL. %v", url1)
	}
	u2, err = url.Parse(url2)
	if err != nil {
		return false, fmt.Errorf("failed to parse URL. %v", url2)
	}
	p1 := u1.Port()
	if p1 == "" && u1.Scheme == "https" {
		p1 = "443"
	}
	p2 := u1.Port()
	if p2 == "" && u1.Scheme == "https" {
		p2 = "443"
	}

	return u1.Hostname() == u2.Hostname() && p1 == p2 && u1.Scheme == u2.Scheme, nil
}
