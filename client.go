// Copyright (c) 2021 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
	"context"
	"net/http"
	"net/url"
	"time"
)

// InternalClient is implemented by HTTPClient
type InternalClient interface {
	Get(context.Context, *url.URL, map[string]string, time.Duration) (*http.Response, error)
	Post(context.Context, *url.URL, map[string]string, []byte, time.Duration, bool) (*http.Response, error)
}

type hTTPClient struct {
	sr *snowflakeRestful
}

func (cli *hTTPClient) Get(
	ctx context.Context,
	url *url.URL,
	headers map[string]string,
	timeout time.Duration) (*http.Response, error) {
	return cli.sr.FuncGet(ctx, cli.sr, url, headers, timeout)
}

func (cli *hTTPClient) Post(
	ctx context.Context,
	url *url.URL,
	headers map[string]string,
	body []byte,
	timeout time.Duration,
	raise4xx bool) (*http.Response, error) {
	return cli.sr.FuncPost(ctx, cli.sr, url, headers, body, timeout, raise4xx)
}
