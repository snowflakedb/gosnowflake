// Go Snowflake Driver - Snowflake driver for Go's database/sql package
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
)

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
	SessionId   int

	Connection *snowflakeConn
}

func (sr *snowflakeRestful) post(
  fullUrl string,
  headers map[string]string,
  body []byte) (
  *http.Response, error) {
	req, err := http.NewRequest("POST", fullUrl, bytes.NewReader(body))
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
  data *ExecResponse, err error) {
	log.Printf("PARAMS: %s", params)
	log.Printf("BODY: %s", body)
	fullUrl := fmt.Sprintf(
		"%s://%s:%d%s", sr.Protocol, sr.Host, sr.Port, "/queries/v1/query-request?"+params.Encode())
	resp, err := sr.post(fullUrl, headers, body)
	defer resp.Body.Close()
	if resp.StatusCode == 200 {
		log.Printf("resp: %s", resp)
		var respd ExecResponse
		err = json.NewDecoder(resp.Body).Decode(&respd)
		if err != nil {
			log.Fatal(err)
			return nil, err
		}
		return &respd, nil
	} else {
		// TODO: better error handing
		log.Printf("resp: %s", resp)
		b, err := ioutil.ReadAll(resp.Body)
		log.Printf("b RESPONSE: %s", b)
		if err != nil {
			log.Fatal(err)
			return nil, err
		}
		log.Printf("ERROR RESPONSE: %s", b)
		return nil, err
	}
}

func (sr *snowflakeRestful) PostAuth(
  params *url.Values,
  headers map[string]string,
  body []byte,
  timeout time.Duration) (
  data *AuthResponse, err error) {
	fullUrl := fmt.Sprintf(
		"%s://%s:%d%s", sr.Protocol, sr.Host, sr.Port, "/session/v1/login-request?"+params.Encode())
	resp, err := sr.post(fullUrl, headers, body)
	defer resp.Body.Close()
	if resp.StatusCode == 200 {
		log.Printf("resp: %s", resp)
		var respd AuthResponse
		err = json.NewDecoder(resp.Body).Decode(&respd)
		if err != nil {
			log.Fatal(err)
			return nil, err
		}
		return &respd, nil
	} else {
		// TODO: better error handing
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Fatal(err)
			return nil, err
		}
		log.Printf("ERROR RESPONSE: %s", b)
		return nil, err

	}
}
