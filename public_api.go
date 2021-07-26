package gosnowflake

import "context"

// SnowflakeConnectionAPI exposes methods publicly not part of the Golang SQL API
type SnowflakeConnectionAPI interface {
	GetQueryStatus(ctx context.Context, qid string) error
}

