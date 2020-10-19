package gosnowflake

import (
	"context"
	"net/url"
	"testing"
	"time"
)

const serviceNameStub = "SV"
const serviceNameAppend = "a"

// postQueryMock would generate a response based on the X-Snowflake-Service header, to generate a response
// with the SERVICE_NAME field appending a character at the end of the header
// This way it could test both the send and receive logic
func postQueryMock(_ context.Context, _ *snowflakeRestful, _ *url.Values, headers map[string]string, _ []byte, _ time.Duration, _ string) (*execResponse, error) {
	var serviceName string
	if serviceHeader, ok := headers["X-Snowflake-Service"]; ok {
		serviceName = serviceHeader + serviceNameAppend
	} else {
		serviceName = serviceNameStub
	}

	dd := &execResponseData{
		Parameters: []nameValueParameter{{"SERVICE_NAME", serviceName}},
	}
	return &execResponse{
		Data:    *dd,
		Message: "",
		Code:    "0",
		Success: true,
	}, nil
}

func TestUnitPostQueryWithSpecificRequestID(t *testing.T) {
	origRequestID := "specific-snowflake-request-id"
	ctx := context.WithValue(context.Background(), SnowflakeRequestIDKey, origRequestID)
	postQueryMock := func(_ context.Context, _ *snowflakeRestful, _ *url.Values, _ map[string]string, _ []byte, _ time.Duration, requestID string) (*execResponse, error) {
		// ensure the same requestID is used after the session token is renewed.
		if requestID != origRequestID {
			t.Fatal("requestID doesn't match")
		}
		dd := &execResponseData{}
		return &execResponse{
			Data:    *dd,
			Message: "",
			Code:    "0",
			Success: true,
		}, nil
	}

	sr := &snowflakeRestful{
		FuncPostQuery: postQueryMock,
	}

	sc := &snowflakeConn{
		cfg:  &Config{Params: map[string]*string{}},
		rest: sr,
	}
	_, err := sc.exec(ctx, "", false, false, nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
}

// TestServiceName tests two things:
// 1. request header would contain X-Snowflake-Service if the cfg parameters contains SERVICE_NAME
// 2. SERVICE_NAME would be update by response payload
// It is achieved through an interactive postQueryMock that would generate response based on header
func TestServiceName(t *testing.T) {
	sr := &snowflakeRestful{
		FuncPostQuery: postQueryMock,
	}

	sc := &snowflakeConn{
		cfg:  &Config{Params: map[string]*string{}},
		rest: sr,
	}

	expectServiceName := serviceNameStub
	for i := 0; i < 5; i++ {
		sc.exec(context.TODO(), "", false, false, nil)
		if actualServiceName, ok := sc.cfg.Params[serviceName]; ok {
			if *actualServiceName != expectServiceName {
				t.Errorf("service name mis-match. expected %v, actual %v", expectServiceName, actualServiceName)
			}
		} else {
			t.Error("No service name in the response")
		}

		expectServiceName += serviceNameAppend
	}
}

func closeSessionMock(_ context.Context, _ *snowflakeRestful, _ time.Duration) error {
	return &SnowflakeError{
		Number: ErrSessionGone,
	}
}

func TestCloseIgnoreSessionGone(t *testing.T) {
	sr := &snowflakeRestful{
		FuncCloseSession: closeSessionMock,
	}
	sc := &snowflakeConn{
		cfg:  &Config{Params: map[string]*string{}},
		rest: sr,
	}

	if sc.Close() != nil {
		t.Error("Close should let go session gone error")
	}
}
