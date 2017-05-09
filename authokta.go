// Package gosnowflake is a Go Snowflake Driver for Go's database/sql
//
// Copyright (c) 2017 Snowflake Computing Inc. All right reserved.
//
package gosnowflake

import (
	"bytes"
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

type authOKTARequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type authOKTAResponse struct {
	CookieToken string `json:"cookieToken"`
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

func (sr *snowflakeRestful) PostAuthSAML(
	headers map[string]string,
	body []byte,
	timeout time.Duration) (
	data *authResponse, err error) {
	requestID := fmt.Sprintf("requestId=%v", uuid.NewV4().String())
	fullURL := fmt.Sprintf(
		"%s://%s:%d%s", sr.Protocol, sr.Host, sr.Port,
		"/session/authenticator-request?"+requestID)
	glog.V(2).Infof("fullURL: %v", fullURL)
	resp, err := sr.post(context.TODO(), fullURL, headers, body, timeout)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		glog.V(2).Infof("PostAuthSAML: resp: %v", resp)
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

func (sr *snowflakeRestful) PostAuthOKTA(
	headers map[string]string,
	body []byte,
	fullURL string,
	timeout time.Duration) (
	data *authOKTAResponse, err error) {
	glog.V(2).Infof("fullURL: %v", fullURL)
	resp, err := sr.post(context.TODO(), fullURL, headers, body, timeout)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		glog.V(2).Infof("PostAuthOKTA: resp: %v", resp)
		var respd authOKTAResponse
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

func (sr *snowflakeRestful) GetSSO(
	params *url.Values,
	headers map[string]string,
	url string,
	timeout time.Duration) (
	bd []byte, err error) {
	fullURL := fmt.Sprintf("%s?%s", url, params.Encode())
	glog.V(2).Infof("fullURL: %v", fullURL)
	resp, err := sr.get(context.TODO(), fullURL, headers, timeout)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.V(1).Infof("%v", err)
		return nil, err
	}
	if resp.StatusCode == http.StatusOK {
		glog.V(2).Infof("GetSSO: resp: %v", resp)
		return b, nil
	}
	return nil, fmt.Errorf("failed to get SSO response. HTTP code: %v", resp.StatusCode)
}
