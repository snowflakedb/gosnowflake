package gosnowflake

import (
	"context"
	"database/sql"
	"testing"
)

// TODO move this test to config package when we have wiremock support in an internal package
func TestShouldSetUpTlsConfig(t *testing.T) {
	tlsConfig := wiremockHTTPS.tlsConfig(t)
	err := RegisterTLSConfig("wiremock", tlsConfig)
	assertNilF(t, err)
	wiremockHTTPS.registerMappings(t, newWiremockMapping("auth/password/successful_flow.json"))

	for _, dbFunc := range []func() *sql.DB{
		func() *sql.DB {
			cfg := wiremockHTTPS.connectionConfig(t)
			cfg.TLSConfigName = "wiremock"
			cfg.Transporter = nil
			return sql.OpenDB(NewConnector(SnowflakeDriver{}, *cfg))
		},
		func() *sql.DB {
			cfg := wiremockHTTPS.connectionConfig(t)
			cfg.TLSConfigName = "wiremock"
			cfg.Transporter = nil
			dsn, err := DSN(cfg)
			assertNilF(t, err)
			db, err := sql.Open("snowflake", dsn)
			assertNilF(t, err)
			return db
		},
	} {
		t.Run("", func(t *testing.T) {
			db := dbFunc()
			defer db.Close()
			// mock connection, no need to close
			_, err := db.Conn(context.Background())
			assertNilF(t, err)
		})
	}
}
