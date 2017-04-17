package gosnowflake

import (
	"database/sql"
	"database/sql/driver"
	"log"
	"net"
	"net/http"
)

type SnowflakeDriver struct {
	rest *SnowflakeRestful
}

type DialFunc func(addr string) (net.Conn, error)

var dials map[string]DialFunc

func RegisterDial(net string, dial DialFunc) {
	if dials == nil {
		dials = make(map[string]DialFunc)
	}
	dials[net] = dial
}

func (d SnowflakeDriver) Open(dsn string) (driver.Conn, error) {
	log.Println("Open: " + dsn) // TODO: Hide credential
	var err error
	sc := &snowflakeConn{}
	sc.cfg, err = ParseDSN(dsn)
	if err != nil {
		return nil, err
	}

	// Authenticate
	d.rest = &SnowflakeRestful{
		Host:     sc.cfg.Host,
		Port:     sc.cfg.Port,
		Protocol: sc.cfg.Protocol,
	}
	client := &http.Client{}
	d.rest.Authenticate(client, sc.cfg.User, sc.cfg.Password, sc.cfg.Account)

	return sc, nil
}

func init() {
	sql.Register("snowflake", &SnowflakeDriver{})
}
