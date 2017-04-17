package gosnowflake

import (
	"database/sql/driver"
	"log"
)

type snowflakeConn struct {
	cfg *Config
}

func (sc *snowflakeConn) handleParams() (err error) {
	return
}

func (sc *snowflakeConn) Begin() (driver.Tx, error) {
	log.Println("Begin")
	return nil, nil
}
func (sc *snowflakeConn) Close() (err error) {
	log.Println("Close")
	return nil
}
func (sc *snowflakeConn) cleanup() {
}
func (sc *snowflakeConn) Prepare(query string) (driver.Stmt, error) {
	log.Println("Prepare")
	return nil, nil
}
func (sc *snowflakeConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	log.Println("Exec")
	return nil, nil
}

func (sc *snowflakeConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	log.Println("Query")
	return nil, nil
}
