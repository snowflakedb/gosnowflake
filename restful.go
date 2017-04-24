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
	"log"
	"net/http"
	"net/url"
	"time"

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

func (sr *snowflakeRestful) post(
	fullURL string,
	headers map[string]string,
	body []byte) (
	*http.Response, error) {
	req, err := http.NewRequest("POST", fullURL, bytes.NewReader(body))
	if err != nil {
		log.Fatal(err)
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
	log.Printf("PARAMS: %v, BODY: %v", params, body)
	uuid := fmt.Sprintf("requestId=%v", uuid.NewV4().String())
	fullURL := fmt.Sprintf(
		"%s://%s:%d%s", sr.Protocol, sr.Host, sr.Port,
		"/queries/v1/query-request?"+uuid+"&"+params.Encode())
	resp, err := sr.post(fullURL, headers, body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		log.Printf("PostQuery: resp: %v", resp)
		var respd execResponse
		err = json.NewDecoder(resp.Body).Decode(&respd)
		if err != nil {
			log.Fatal(err)
			return nil, err
		}
		return &respd, nil
	}
	// TODO: better error handing and retry
	log.Printf("PostQuery: resp: %v", resp)
	b, err := ioutil.ReadAll(resp.Body)
	log.Printf("b RESPONSE: %s", b)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	log.Printf("ERROR RESPONSE: %v", b)
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
	log.Printf("fullURL: %v", fullURL)
	resp, err := sr.post(fullURL, headers, body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		log.Printf("PostAuth: resp: %v", resp)
		var respd authResponse
		err = json.NewDecoder(resp.Body).Decode(&respd)
		if err != nil {
			log.Fatal(err)
			return nil, err
		}
		return &respd, nil
	}
	// TODO: better error handing and retry
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	log.Printf("ERROR RESPONSE: %v", b)
	return nil, err
}
