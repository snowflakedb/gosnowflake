package gosnowflake

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

const AuthorizationKey string = "Authorization"
const ContentTypeApplicationJson string = "application/json"
const AcceptTypeAppliationSnowflake string = "application/snowflake"
const ClientType string = "Go"
const ClientVersion string = "0.1"
const OSVersion string = "0.11"

var UserAgent string = fmt.Sprintf("%s %s", ClientType, ClientVersion)

type AuthRequestClientEnvironment struct {
	Application string `json:"APPLICATION"`
	OsVersion   string `json:"OS_VERSION"`
}
type AuthRequestMain struct {
	ClientAppId       string `json:"CLIENT_APP_ID"`
	ClientAppVersion  string `json:"CLIENT_APP_VERSION"`
	SvnRevision       string `json:"SVN_REVISION"`
	AccoutName        string `json:"ACCOUNT_NAME"`
	LoginName         string `json:"LOGIN_NAME,omitempty"`
	Password          string `json:"PASSWORD,omitempty"`
	RawSAMLResponse   string `json:"RAW_SAML_RESPONSE,omitempty"`
	ClientEnvironment AuthRequestClientEnvironment `json:"CLIENT_ENVIRONMENT"`
}
type AuthRequest struct {
	Data AuthRequestMain `json:"data"`
}

type AuthResponseParameter struct {
	Name  string  `json:"name"`
	Value json.RawMessage `json:"value"`
}

type AuthResponseSessionInfo struct {
	DatabaseName  string `json:"databaseName"`
	SchemaName    string `json:"schemaName"`
	WarehouseName string `json:"warehouseName"`
	RoleName      string `json:"roleName"`
}

type AuthResponseMain struct {
	Token                   string `json:"token,omitempty"`
	ValidityInSeconds       time.Duration `json:"validityInSeconds,omitempty"`
	MasterToken             string `json:"maxterToken,omitempty"`
	MasterValidityInSeconds time.Duration `json:"masterValidityInSeconds"`
	DisplayUserName         string`json:"displayUserName"`
	ServerVersion           string`json:"serverVersion"`
	FirstLogin              bool`json:"firstLogin"`
	RemMeToken              string`json:"remMeToken"`
	RemMeValidityInSeconds  time.Duration`json:"remMeValidityInSeconds"`
	HealthCheckInterval     time.Duration`json:"healthCheckInterval"`
	NewClientForUpgrade     string `json:"newClientForUpgrade"` // TODO: what is datatype?
	SessionId               int`json:"sessionId"`
	Parameters              []AuthResponseParameter`json:"parameters"`
	SessionInfo             AuthResponseSessionInfo`json:"sessionInfo"`
}
type AuthResponse struct {
	Data    AuthResponseMain `json:"data"`
	Message string `json:"message"`
	Code    string `json:"code"`
	Success bool`json:"success"`
}

type SnowflakeRestful struct {
	Host           string
	Port           int
	ProxyHost      string
	ProxyPort      int
	ProxyUser      string
	ProxyPass      string
	Protocol       string
	ConnectTimeout time.Duration // Dial timeout
	RequestTimeout time.Duration // Request read time
	LoginTimeout   time.Duration // Login timeout
}

func (sf *SnowflakeRestful) Authenticate(
  client *http.Client, user string, password string, account string) (err error) {
	log.Println("Authenticate")
	headers := make(map[string]string)
	headers["Content-Type"] = ContentTypeApplicationJson
	headers["accept"] = AcceptTypeAppliationSnowflake
	headers["User-Agent"] = UserAgent

	body := AuthRequest{
		Data: AuthRequestMain{
			ClientAppId:      ClientType,
			ClientAppVersion: ClientVersion,
			SvnRevision:      "",
			AccoutName:       account,
			LoginName:        user,
			Password:         password,
			ClientEnvironment: AuthRequestClientEnvironment{
				Application: ClientType,
				OsVersion:   OSVersion,
			},
		},
	}
	var json_body []byte
	json_body, err = json.Marshal(body)
	if err != nil {
		return
	}

	url := "/session/v1/login-request"
	respd, err := sf.Post(client, url, headers, json_body, "", sf.LoginTimeout)
	if err != nil {
		return nil
	}
	return nil
}

func (sf *SnowflakeRestful) Post(
  client *http.Client,
  url string, headers map[string]string, body []byte, token string, timeout time.Duration) (
  data AuthResponse, err error) {
	fullUrl := fmt.Sprintf("%s://%s:%d%s", sf.Protocol, sf.Host, sf.Port, url)
	log.Println(fullUrl)
	req, err := http.NewRequest("POST", fullUrl, bytes.NewReader(body))
	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	var resp *http.Response
	resp, err = client.Do(req)
	defer resp.Body.Close()
	var respd AuthResponse
	err = json.NewDecoder(resp.Body).Decode(&respd)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	log.Printf("RES: %s", respd)
	return respd, nil
}
