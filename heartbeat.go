package gosnowflake

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

const (
	// One hour interval should be good enough to renew tokens for four hours master token validity
	heartBeatInterval = 3600 * time.Second
)

type heartbeat struct {
	restful      *snowflakeRestful
	shutdownChan chan bool
}

func (hc *heartbeat) run() {
	_, _, sessionID := hc.restful.TokenAccessor.GetTokens()
	ctx := context.WithValue(context.Background(), SFSessionIDKey, sessionID)
	hbTicker := time.NewTicker(heartBeatInterval)
	defer hbTicker.Stop()
	for {
		select {
		case <-hbTicker.C:
			err := hc.heartbeatMain()
			if err != nil {
				logger.WithContext(ctx).Error("failed to heartbeat")
			}
		case <-hc.shutdownChan:
			logger.WithContext(ctx).Info("stopping heartbeat")
			return
		}
	}
}

func (hc *heartbeat) start() {
	_, _, sessionID := hc.restful.TokenAccessor.GetTokens()
	ctx := context.WithValue(context.Background(), SFSessionIDKey, sessionID)
	hc.shutdownChan = make(chan bool)
	go hc.run()
	logger.WithContext(ctx).Info("heartbeat started")
}

func (hc *heartbeat) stop() {
	_, _, sessionID := hc.restful.TokenAccessor.GetTokens()
	ctx := context.WithValue(context.Background(), SFSessionIDKey, sessionID)
	hc.shutdownChan <- true
	close(hc.shutdownChan)
	logger.WithContext(ctx).Info("heartbeat stopped")
}

func (hc *heartbeat) heartbeatMain() error {
	params := &url.Values{}
	params.Set(requestIDKey, NewUUID().String())
	params.Set(requestGUIDKey, NewUUID().String())
	headers := getHeaders()
	token, _, sessionID := hc.restful.TokenAccessor.GetTokens()
	ctx := context.WithValue(context.Background(), SFSessionIDKey, sessionID)
	logger.WithContext(ctx).Info("Heartbeating!")
	headers[headerAuthorizationKey] = fmt.Sprintf(headerSnowflakeToken, token)

	fullURL := hc.restful.getFullURL(heartBeatPath, params)
	timeout := hc.restful.RequestTimeout
	resp, err := hc.restful.FuncPost(context.Background(), hc.restful, fullURL, headers, nil, timeout, defaultTimeProvider, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		logger.WithContext(ctx).Infof("heartbeatMain: resp: %v", resp)
		var respd execResponse
		err = json.NewDecoder(resp.Body).Decode(&respd)
		if err != nil {
			logger.WithContext(ctx).Infof("failed to decode JSON. err: %v", err)
			return err
		}
		if respd.Code == sessionExpiredCode {
			logger.WithContext(ctx).Info("Snowflake returned error 390112 (session expired), trying to renew expired token.")
			err = hc.restful.renewExpiredSessionToken(context.Background(), timeout, token)
			if err != nil {
				return err
			}
		}
		return nil
	}
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		logger.WithContext(ctx).Errorf("failed to extract HTTP response body. err: %v", err)
		return err
	}
	logger.WithContext(ctx).Infof("HTTP: %v, URL: %v, Body: %v", resp.StatusCode, fullURL, b)
	logger.WithContext(ctx).Infof("Header: %v", resp.Header)
	return &SnowflakeError{
		Number:   ErrFailedToHeartbeat,
		SQLState: SQLStateConnectionFailure,
		Message:  "Failed to heartbeat.",
	}
}
