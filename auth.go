// Package gosnowflake is a Go Snowflake Driver for Go's database/sql
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
	respd, err := sr.postAuth(params, headers, jsonBody, sr.LoginTimeout)
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
