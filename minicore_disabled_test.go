//go:build minicore_disabled

package gosnowflake

import (
	"database/sql"
	"testing"

	"github.com/snowflakedb/gosnowflake/v2/internal/compilation"
)

func TestMiniCoreDisabledAtCompileTime(t *testing.T) {
	assertFalseF(t, compilation.MinicoreEnabled, "MinicoreEnabled should be false when built with -tags minicore_disabled")
}

func TestMiniCoreDisabledE2E(t *testing.T) {
	wiremock.registerMappings(t, newWiremockMapping("minicore/auth/disabled_flow.json"), newWiremockMapping("select1.json"))
	cfg := wiremock.connectionConfig()
	connector := NewConnector(SnowflakeDriver{}, *cfg)
	db := sql.OpenDB(connector)
	runSmokeQuery(t, db)
}
