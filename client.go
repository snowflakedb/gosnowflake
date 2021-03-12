// Copyright (c) 2021 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
	"sync/atomic"
	"time"
)

// Client holds an InternalClient
type Client struct {
	client InternalClient
}

// NewClient returns a Client
func NewClient() *Client {
	cli := &HTTPClient{}
	return &Client{cli}
}

// InternalClient is implemented by HTTPClient
type InternalClient interface {
	Get(string, map[string]string, time.Duration) (*http.Response, error)
	Post(string, map[string]string, []byte, time.Duration) (*http.Response, error)
}

// HTTPClient implements InternalClient
type HTTPClient struct {
	cfg *Config
	client clientInterface
}

// SetConfig sets config
func (cli *HTTPClient) SetConfig(config *Config) {
	cli.cfg = config
	cli.cfg.Transporter = SnowflakeTransport
	if config.Transporter == nil {
		if config.InsecureMode {
			cli.cfg.Transporter = snowflakeInsecureTransport
		} else {
			ocspResponseCacheLock.Lock()
			atomic.StoreUint32((*uint32)(&ocspFailOpen), uint32(config.OCSPFailOpen))
			ocspResponseCacheLock.Unlock()
		}
	} else {
		cli.cfg.Transporter = config.Transporter
	}

	if cli.cfg.TokenAccessor == nil {
		cli.cfg.TokenAccessor = getSimpleTokenAccessor()
	}

	cli.client = &http.Client{
		Timeout: defaultClientTimeout,
		Transport: cli.cfg.Transporter,
	}
}

// Get implements InternalClient
func (cli *HTTPClient) Get(path string, headers map[string]string, timeout time.Duration) (*http.Response, error) {
	return newRetryHTTP(
		context.Background(),
		cli.client,
		http.NewRequest,
		&url.URL{Scheme: cli.cfg.Protocol,
			Host: cli.cfg.Host + ":" + strconv.Itoa(cli.cfg.Port),
			Path: path,
		},
		headers,
		timeout,
	).execute()
}

// Post implements InternalClient
func (cli *HTTPClient) Post(path string, headers map[string]string, body []byte, timeout time.Duration) (*http.Response, error) {
	return newRetryHTTP(context.Background(),
		cli.client,
		http.NewRequest,
		&url.URL{Scheme: cli.cfg.Protocol,
			Host: cli.cfg.Host + ":" + strconv.Itoa(cli.cfg.Port),
			Path: path,
		},
		headers,
		timeout,
	).doPost().setBody(body).doRaise4XX(true).execute()
}

func (cli *HTTPClient) setClient(client clientInterface) {
	cli.client = client
}
