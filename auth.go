// Go Snowflake Driver - Snowflake driver for Go's database/sql package
//
// Copyright (c) 2017 Snowflake Computing Inc. All right reserved.
//

package gosnowflake

import (
	"encoding/json"
	"net/url"
	"strconv"
	"time"

	"github.com/golang/glog"
)

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
}
type authResponse struct {
	Data    authResponseMain `json:"data"`
	Message string           `json:"message"`
	Code    string           `json:"code"`
	Success bool             `json:"success"`
}

// Authenticate is used to authenticate user to gain accesss to Snowflake database.
func Authenticate(
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
	samlResponse string,
	mfaCallback string,
	passwordCallback string,
	sessionParameters map[string]string) (resp *AuthResponseSessionInfo, err error) {
	glog.V(2).Info("Authenticate")

	if sr.Token != "" && sr.MasterToken != "" {
		glog.V(2).Infoln("Tokens are already available.")
		return nil, nil
	}

	headers := make(map[string]string)
	headers["Content-Type"] = headerContentTypeApplicationJSON
	headers["accept"] = headerAcceptTypeAppliationSnowflake
	headers["User-Agent"] = UserAgent

	clientEnvironment := authRequestClientEnvironment{
		Application: clientType,
		OsVersion:   osVersion,
	}

	requestMain := authRequestData{
		ClientAppID:       clientType,
		ClientAppVersion:  clientVersion,
		SvnRevision:       "",
		AccoutName:        account,
		ClientEnvironment: clientEnvironment,
	}
	if samlResponse != "" {
		requestMain.RawSAMLResponse = samlResponse
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
