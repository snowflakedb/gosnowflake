// Package gosnowflake is a Go Snowflake Driver for Go's database/sql
//
// Copyright (c) 2017 Snowflake Computing Inc. All right reserved.
//
package gosnowflake

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/golang/glog"
	"github.com/satori/go.uuid"
)

const (
	headerSnowflakeToken   = "Snowflake Token=\"%v\""
	headerAuthorizationKey = "Authorization"

	headerContentTypeApplicationJSON     = "application/json"
	headerAcceptTypeApplicationSnowflake = "application/snowflake"

	sessionExpiredCode       = "390112"
	queryInProgressCode      = "333333"
	queryInProgressAsyncCode = "333334"
)

var (
	proxyHost     string
	proxyPort     int
	proxyUser     string
	proxyPassword string
)

type snowflakeRestful struct {
	Host           string
	Port           int
	Protocol       string
	LoginTimeout   time.Duration // Login timeout
	RequestTimeout time.Duration // request timeout
	Authenticator  string

	Client      *http.Client
	Token       string
	MasterToken string
	SessionID   int

	Connection          *snowflakeConn
	FuncPostQuery       func(context.Context, *snowflakeRestful, *url.Values, map[string]string, []byte, time.Duration) (*execResponse, error)
	FuncPostQueryHelper func(context.Context, *snowflakeRestful, *url.Values, map[string]string, []byte, time.Duration, string) (*execResponse, error)
	FuncPost            func(context.Context, *snowflakeRestful, string, map[string]string, []byte, time.Duration) (*http.Response, error)
	FuncGet             func(context.Context, *snowflakeRestful, string, map[string]string, time.Duration) (*http.Response, error)
	FuncRenewSession    func(context.Context, *snowflakeRestful) error
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

type cancelQueryResponse struct {
	Data    interface{} `json:"data"`
	Message string      `json:"message"`
	Code    string      `json:"code"`
	Success bool        `json:"success"`
}

func postRestful(
	ctx context.Context,
	sr *snowflakeRestful,
	fullURL string,
	headers map[string]string,
	body []byte,
	timeout time.Duration) (
	*http.Response, error) {
	return retryHTTP(ctx, sr.Client, http.NewRequest, "POST", fullURL, headers, body, timeout)
}

func getRestful(
	ctx context.Context,
	sr *snowflakeRestful,
	fullURL string,
	headers map[string]string,
	timeout time.Duration) (
	*http.Response, error) {
	return retryHTTP(ctx, sr.Client, http.NewRequest, "GET", fullURL, headers, nil, timeout)
}

type execResponseAndErr struct {
	resp *execResponse
	err  error
}

func postRestfulQuery(
	ctx context.Context,
	sr *snowflakeRestful,
	params *url.Values,
	headers map[string]string,
	body []byte,
	timeout time.Duration) (
	data *execResponse, err error) {

	requestID := uuid.NewV4().String()
	execResponseChan := make(chan execResponseAndErr)

	go func() {
		data, err := sr.FuncPostQueryHelper(ctx, sr, params, headers, body, timeout, requestID)
		execResp := execResponseAndErr{data, err}
		execResponseChan <- execResp
		close(execResponseChan)
	}()

	select {
	case <-ctx.Done():
		err := sr.cancelQuery(requestID)
		if err != nil {
			return nil, err
		}
		return nil, ctx.Err()
	case respAndErr := <-execResponseChan:
		return respAndErr.resp, respAndErr.err
	}
}

func postRestfulQueryHelper(
	ctx context.Context,
	sr *snowflakeRestful,
	params *url.Values,
	headers map[string]string,
	body []byte,
	timeout time.Duration,
	requestID string) (
	data *execResponse, err error) {
	glog.V(2).Infof("PARAMS: %v", params)
	params.Add("requestId", requestID)
	if sr.Token != "" {
		headers[headerAuthorizationKey] = fmt.Sprintf(headerSnowflakeToken, sr.Token)
	}
	fullURL := fmt.Sprintf(
		"%s://%s:%d%s", sr.Protocol, sr.Host, sr.Port,
		"/queries/v1/query-request?"+params.Encode())
	resp, err := sr.FuncPost(ctx, sr, fullURL, headers, body, timeout)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		glog.V(2).Infof("postQuery: resp: %v", resp)
		var respd execResponse
		err = json.NewDecoder(resp.Body).Decode(&respd)
		if err != nil {
			glog.V(1).Infof("failed to decode JSON output. err: %v", err)
			b, err2 := ioutil.ReadAll(resp.Body)
			if err2 != nil {
				glog.V(1).Infof("failed to extract HTTP response body. err: %v", err2)
				return nil, err2
			}
			glog.V(1).Infof("HTTP Response body: %v", b)
			return nil, err
		}
		if respd.Code == sessionExpiredCode {
			err = sr.FuncRenewSession(ctx, sr)
			if err != nil {
				return nil, err
			}
			return sr.FuncPostQuery(ctx, sr, params, headers, body, timeout)
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
			fullURL := fmt.Sprintf(
				"%s://%s:%d%s", sr.Protocol, sr.Host, sr.Port, resultURL)

			resp, err = sr.FuncGet(ctx, sr, fullURL, headers, 0)
			respd = execResponse{}

			err = json.NewDecoder(resp.Body).Decode(&respd)
			resp.Body.Close()
			if err != nil {
				return nil, err
			}

			if respd.Code == sessionExpiredCode {
				err = sr.FuncRenewSession(ctx, sr)
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
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.V(1).Infof("failed to extract HTTP response body. err: %v", err)
		return nil, err
	}
	glog.V(1).Infof("HTTP: %v, URL: %v, Body: %v", resp.StatusCode, fullURL, b)
	glog.V(1).Infof("Header: %v", resp.Header)
	return nil, &SnowflakeError{
		Number:      ErrFailedToPostQuery,
		Message:     errMsgFailedToPostQuery,
		MessageArgs: []interface{}{resp.StatusCode, fullURL},
	}
}

func (sr *snowflakeRestful) postAuth(
	params *url.Values,
	headers map[string]string,
	body []byte,
	timeout time.Duration) (
	data *authResponse, err error) {
	requestID := fmt.Sprintf("requestId=%v", uuid.NewV4().String())
	params.Add("requestId", requestID)
	fullURL := fmt.Sprintf(
		"%s://%s:%d%s", sr.Protocol, sr.Host, sr.Port,
		"/session/v1/login-request?"+params.Encode())
	glog.V(2).Infof("fullURL: %v", fullURL)
	resp, err := sr.FuncPost(context.TODO(), sr, fullURL, headers, body, timeout)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		glog.V(2).Infof("postAuth: resp: %v", resp)
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

func (sr *snowflakeRestful) closeSession() error {
	glog.V(2).Info("CLOSE SESSION")
	params := &url.Values{}
	params.Add("delete", "true")
	params.Add("requestId", uuid.NewV4().String())
	fullURL := fmt.Sprintf(
		"%s://%s:%d%s", sr.Protocol, sr.Host, sr.Port, "/session?"+params.Encode())

	headers := make(map[string]string)
	headers["Content-Type"] = headerContentTypeApplicationJSON
	headers["accept"] = headerAcceptTypeApplicationSnowflake
	headers["User-Agent"] = userAgent
	headers[headerAuthorizationKey] = fmt.Sprintf(headerSnowflakeToken, sr.Token)

	resp, err := sr.FuncPost(context.TODO(), sr, fullURL, headers, nil, 5*time.Second)
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
		if respd.Success == false && respd.Code != sessionExpiredCode {
			c, err := strconv.Atoi(respd.Code)
			if err != nil {
				return err
			}
			return &SnowflakeError{
				Number:   c,
				Message:  respd.Message,
				SQLState: respd.Code,
			}
		}
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

func renewRestfulSession(ctx context.Context, sr *snowflakeRestful) error {
	glog.V(2).Info("START RENEW SESSION")
	params := &url.Values{}
	params.Add("requestId", uuid.NewV4().String())
	fullURL := fmt.Sprintf(
		"%s://%s:%d%s", sr.Protocol, sr.Host, sr.Port, "/session/token-request?"+params.Encode())

	headers := make(map[string]string)
	headers["Content-Type"] = headerContentTypeApplicationJSON
	headers["accept"] = headerAcceptTypeApplicationSnowflake
	headers["User-Agent"] = userAgent
	headers[headerAuthorizationKey] = fmt.Sprintf(headerSnowflakeToken, sr.MasterToken)

	body := make(map[string]string)
	body["oldSessionToken"] = sr.Token
	body["requestType"] = "RENEW"

	var reqBody []byte
	reqBody, err := json.Marshal(body)
	if err != nil {
		return err
	}

	resp, err := sr.FuncPost(ctx, sr, fullURL, headers, reqBody, sr.RequestTimeout)
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
			c, err := strconv.Atoi(respd.Code)
			if err != nil {
				return err
			}
			return &SnowflakeError{
				Number:   c,
				Message:  respd.Message,
				SQLState: respd.Code,
			}
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

func (sr *snowflakeRestful) cancelQuery(requestID string) error {
	glog.V(2).Info("CANCEL QUERY")
	params := &url.Values{}
	params.Add("requestId", uuid.NewV4().String())
	fullURL := fmt.Sprintf(
		"%s://%s:%d%s", sr.Protocol, sr.Host, sr.Port, "/queries/v1/abort-request?"+params.Encode())

	headers := make(map[string]string)
	headers["Content-Type"] = headerContentTypeApplicationJSON
	headers["accept"] = headerAcceptTypeApplicationSnowflake
	headers["User-Agent"] = userAgent
	headers[headerAuthorizationKey] = fmt.Sprintf(headerSnowflakeToken, sr.Token)

	req := make(map[string]string)
	req["requestId"] = requestID

	reqByte, err := json.Marshal(req)
	if err != nil {
		return err
	}

	resp, err := sr.FuncPost(context.TODO(), sr, fullURL, headers, reqByte, 0)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		var respd cancelQueryResponse
		err = json.NewDecoder(resp.Body).Decode(&respd)
		if err != nil {
			return err
		}
		if respd.Success == false && respd.Code == sessionExpiredCode {
			err := sr.FuncRenewSession(context.TODO(), sr)
			if err != nil {
				return err
			}
			return sr.cancelQuery(requestID)
		} else if respd.Success == true {
			return nil
		} else {
			return &SnowflakeError{Message: respd.Message, SQLState: respd.Code}
		}
	}

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.V(1).Infof("%v", err)
		return err
	}
	glog.V(2).Infof("ERROR RESPONSE: %v", b)
	return err
}
