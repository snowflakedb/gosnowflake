// Copyright (c) 2018 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
	"fmt"
	"net/url"
	"time"

	"context"

	"io/ioutil"
	"net/http"

	"encoding/json"

	"github.com/satori/go.uuid"
)

const (
	// One hour interval should be good enough to renew tokens for four hours master token validity
	heartBeatInterval = 60 * time.Minute
)

type heartbeat struct {
	restful      *snowflakeRestful
	ShutdownChan chan int
}

func (hc *heartbeat) run() {
	hbTimer := time.NewTimer(heartBeatInterval)
	for {
		go func() {
			<-hbTimer.C
			err := hc.heartbeatMain()
			if err != nil {
				glog.V(2).Info("failed to heartbeat")
				return
			}
			hbTimer = time.NewTimer(heartBeatInterval)
		}()
		select {
		case <-hc.ShutdownChan:
			// stop
			hbTimer.Stop()
			glog.V(2).Info("stopping")
			return
		default:
			// no more download
			glog.V(2).Info("no shutdown signal")
		}
		time.Sleep(1 * time.Second)
	}
}

func (hc *heartbeat) start() {
	hc.ShutdownChan = make(chan int)
	go hc.run()
	glog.V(2).Info("heartbeat started")
}

func (hc *heartbeat) stop() {
	hc.ShutdownChan <- 1
	close(hc.ShutdownChan)
	glog.V(2).Info("heartbeat stopped")
}

func (hc *heartbeat) heartbeatMain() error {
	glog.V(2).Info("Heartbeating!")
	params := &url.Values{}
	params.Add("requestId", uuid.NewV4().String())
	fullURL := fmt.Sprintf(
		"%s://%s:%d%s", hc.restful.Protocol, hc.restful.Host, hc.restful.Port, "/session/heartbeat?"+params.Encode())
	headers := make(map[string]string)
	headers["Content-Type"] = headerContentTypeApplicationJSON
	headers["accept"] = headerAcceptTypeApplicationSnowflake
	headers["User-Agent"] = userAgent
	headers[headerAuthorizationKey] = fmt.Sprintf(headerSnowflakeToken, hc.restful.Token)

	resp, err := hc.restful.FuncPost(context.TODO(), hc.restful, fullURL, headers, nil, hc.restful.RequestTimeout, false)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		glog.V(2).Infof("heartbeatMain: resp: %v", resp)
		var respd execResponse
		err = json.NewDecoder(resp.Body).Decode(&respd)
		if err != nil {
			glog.V(1).Infof("failed to decode JSON. err: %v", err)
			glog.Flush()
			return err
		}
		if respd.Code == sessionExpiredCode {
			err = hc.restful.FuncRenewSession(context.TODO(), hc.restful)
			if err != nil {
				return err
			}
		}
		return nil
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.V(1).Infof("failed to extract HTTP response body. err: %v", err)
		return err
	}
	glog.V(1).Infof("HTTP: %v, URL: %v, Body: %v", resp.StatusCode, fullURL, b)
	glog.V(1).Infof("Header: %v", resp.Header)
	glog.Flush()
	return &SnowflakeError{
		Number:   ErrFailedToHeartbeat,
		SQLState: SQLStateConnectionFailure,
		Message:  "Failed to heartbeat.",
	}
}
