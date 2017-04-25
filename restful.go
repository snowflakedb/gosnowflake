// Package gosnowflake is a Go Snowflake Driver for Go's database/sql
//
// Copyright (c) 2017 Snowflake Computing Inc. All right reserved.
//
package gosnowflake

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	"github.com/golang/glog"
	"github.com/satori/go.uuid"
)

var snowflakeTransport = &http.Transport{
	// TODO: Proxy
	MaxIdleConns:    10,
	IdleConnTimeout: 30 * time.Minute,
	// TODO: Timeout
}

type snowflakeRestful struct {
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

	Client      *http.Client
	Token       string
	MasterToken string
	SessionID   int

	Connection *snowflakeConn
}

type renewSessionResponse struct {
	Data    renewSessionResponseMain `json:"data"`
	Message string                   `json:"message"`
	Code    string                   `json:"code"`
	Success bool                     `json:"success"`
}

type renewSessionResponseMain struct {
	SessionToken        string        `json:"sessionToken"`
	ValidityInSecondsST time.Duration `json:"validityInSecondsST"`
	MasterToken         string        `json:"masterToken"`
	ValidityInSecondsMT time.Duration `json:"validityInSecondsMT"`
	SessionID           int           `json:"sessionId"`
}

func (sr *snowflakeRestful) post(
	fullURL string,
	headers map[string]string,
	body []byte) (
	*http.Response, error) {
	req, err := http.NewRequest("POST", fullURL, bytes.NewReader(body))
	if err != nil {
		// TODO: error handling
		glog.V(1).Infof("%v", err)
		return nil, err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	return sr.Client.Do(req)
}

func (sr *snowflakeRestful) get(
	path string,
	headers map[string]string) (
	*http.Response, error) {
	fullURL := fmt.Sprintf(
		"%s://%s:%d%s", sr.Protocol, sr.Host, sr.Port, path)

	req, err := http.NewRequest("GET", fullURL, nil)
	if err != nil {
		glog.V(1).Infof("%v", err)
		return nil, err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	return sr.Client.Do(req)
}

func (sr *snowflakeRestful) PostQuery(
	params *url.Values,
	headers map[string]string,
	body []byte,
	timeout time.Duration) (
	data *execResponse, err error) {
	glog.V(2).Infof("PARAMS: %v, BODY: %v", params, body)
	uuid := fmt.Sprintf("requestId=%v", uuid.NewV4().String())
	fullURL := fmt.Sprintf(
		"%s://%s:%d%s", sr.Protocol, sr.Host, sr.Port,
		"/queries/v1/query-request?"+uuid+"&"+params.Encode())
	if sr.Token != "" {
		headers[headerAuthorizationKey] = fmt.Sprintf(headerSnowflakeToken, sr.Token)
	}
	resp, err := sr.post(fullURL, headers, body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		glog.V(2).Infof("PostQuery: resp: %v", resp)
		var respd execResponse
		err = json.NewDecoder(resp.Body).Decode(&respd)
		if err != nil {
			glog.V(1).Infof("%v", err)
			return nil, err
		}

		if respd.Code == sessionExpiredCode {
			err = sr.renewSession()
			if err != nil {
				return nil, err
			}
			return sr.PostQuery(params, headers, body, timeout)
		}

		var resultURL string
		isSessionRenewed := false

		for isSessionRenewed || respd.Code == queryInProgressCode ||
			respd.Code == queryInProgressAsyncCode {
			if !isSessionRenewed {
				resultURL = respd.Data.GetResultURL
			}

			glog.V(2).Info("START PING PONG")
			headers[headerAuthorizationKey] = fmt.Sprintf(headerSnowflakeToken, sr.Token)
			resp, err = sr.get(resultURL, headers)
			respd = execResponse{}

			err = json.NewDecoder(resp.Body).Decode(&respd)
			resp.Body.Close()
			if err != nil {
				return nil, err
			}

			if respd.Code == sessionExpiredCode {
				err = sr.renewSession()
				if err != nil {
					return nil, err
				}
				isSessionRenewed = true
			} else {
				isSessionRenewed = false
			}
		}
		return &respd, nil
	}
	// TODO: better error handing and retry
	glog.V(2).Infof("PostQuery: resp: %v", resp)
	b, err := ioutil.ReadAll(resp.Body)
	glog.V(2).Infof("b RESPONSE: %s", b)
	if err != nil {
		glog.V(1).Infof("%v", err)
		return nil, err
	}
	glog.V(2).Infof("ERROR RESPONSE: %v", b)
	return nil, err
}

func (sr *snowflakeRestful) PostAuth(
	params *url.Values,
	headers map[string]string,
	body []byte,
	timeout time.Duration) (
	data *authResponse, err error) {
	uuid := fmt.Sprintf("requestId=%v", uuid.NewV4().String())
	fullURL := fmt.Sprintf(
		"%s://%s:%d%s", sr.Protocol, sr.Host, sr.Port,
		"/session/v1/login-request?"+uuid+"&"+params.Encode())
	glog.V(2).Infof("fullURL: %v", fullURL)
	resp, err := sr.post(fullURL, headers, body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		glog.V(2).Infof("PostAuth: resp: %v", resp)
		var respd authResponse
		err = json.NewDecoder(resp.Body).Decode(&respd)
		if err != nil {
			glog.V(1).Infof("%v", err)
			return nil, err
		}
		return &respd, nil
	}
	// TODO: better error handing and retry
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.V(1).Infof("%v", err)
		return nil, err
	}
	glog.V(2).Infof("ERROR RESPONSE: %v", b)
	return nil, err
}

func (sr *snowflakeRestful) renewSession() error {
	glog.V(2).Info("START RENEW SESSION")
	params := &url.Values{}
	params.Add("requestId", uuid.NewV4().String())
	fullURL := fmt.Sprintf(
		"%s://%s:%d%s", sr.Protocol, sr.Host, sr.Port, "/session/token-request?"+params.Encode())

	headers := make(map[string]string)
	headers["Content-Type"] = headerContentTypeApplicationJSON
	headers["accept"] = headerAcceptTypeAppliationSnowflake
	headers["User-Agent"] = UserAgent
	headers[headerAuthorizationKey] = fmt.Sprintf(headerSnowflakeToken, sr.MasterToken)

	body := make(map[string]string)
	body["oldSessionToken"] = sr.Token
	body["requestType"] = "RENEW"

	var reqBody []byte
	reqBody, err := json.Marshal(body)
	if err != nil {
		return err
	}

	resp, err := sr.post(fullURL, headers, reqBody)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		var respd renewSessionResponse
		err = json.NewDecoder(resp.Body).Decode(&respd)
		if err != nil {
			return err
		}

		if respd.Success == false {
			return &SnowflakeError{Message: respd.Message, SQLState: respd.Code}
		}
		sr.Token = respd.Data.SessionToken
		sr.MasterToken = respd.Data.MasterToken
		return nil
	}

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.V(1).Infof("%v", err)
		return err
	}
	glog.V(2).Infof("ERROR RESPONSE: %v", b)
	return err
}
