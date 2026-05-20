package gosnowflake

import (
	"context"
	"sync"
	"testing"

	sfconfig "github.com/snowflakedb/gosnowflake/v2/internal/config"
)

// newTestShapeConn returns a snowflakeConn with an enabled in-memory telemetry
// client suitable for unit-testing connectionIdentifierShapeTelemetry without
// requiring a real Snowflake account or HTTP plumbing. The returned conn's
// telemetry.sr is nil, which is safe because the tests only exercise addLog
// (which appends to st.logs without touching st.sr).
func newTestShapeConn() *snowflakeConn {
	return &snowflakeConn{
		ctx: context.Background(),
		telemetry: &snowflakeTelemetry{
			mutex:     &sync.Mutex{},
			enabled:   true,
			flushSize: defaultFlushSize,
		},
	}
}

func TestConnectionIdentifierShapeTelemetryEmitsExpectedPayload(t *testing.T) {
	t.Setenv(disableConnectionShapeEnv, "")
	cfg := &Config{}
	sfconfig.SetInputShape(cfg, &sfconfig.ConnectionIdentifierShape{
		AccountProvided:    true,
		AccountWithRegion:  true,
		AccountOrgProvided: true,
		RegionProvided:     false,
		HostProvided:       false,
	})
	sc := newTestShapeConn()
	sc.connectionIdentifierShapeTelemetry(cfg)

	assertEqualF(t, len(sc.telemetry.logs), 1, "expected exactly one telemetry record")
	msg := sc.telemetry.logs[0].Message
	assertEqualE(t, msg[typeKey], connectionIdentifierShape)
	assertEqualE(t, msg[sourceKey], telemetrySource)
	assertEqualE(t, msg[driverTypeKey], "Go")
	assertEqualE(t, msg[accountProvidedKey], "true")
	assertEqualE(t, msg[accountWithRegionKey], "true")
	assertEqualE(t, msg[accountOrgProvidedKey], "true")
	assertEqualE(t, msg[regionProvidedKey], "false")
	assertEqualE(t, msg[hostProvidedKey], "false")
	assertNotEqualE(t, msg[driverVersionKey], "",
		"driver version field should carry SnowflakeGoDriverVersion")
	assertNotEqualE(t, msg[golangVersionKey], "",
		"golang version field should carry runtime.Version()")
}

func TestConnectionIdentifierShapeTelemetrySkipsWhenInputShapeNil(t *testing.T) {
	t.Setenv(disableConnectionShapeEnv, "")
	sc := newTestShapeConn()
	// A freshly-constructed Config has no shape set (SetInputShape not called).
	sc.connectionIdentifierShapeTelemetry(&Config{})
	assertEmptyE(t, sc.telemetry.logs,
		"expected no telemetry record when shape is nil")
}

func TestConnectionIdentifierShapeTelemetrySkipsWhenCfgNil(t *testing.T) {
	t.Setenv(disableConnectionShapeEnv, "")
	sc := newTestShapeConn()
	sc.connectionIdentifierShapeTelemetry(nil)
	assertEmptyE(t, sc.telemetry.logs,
		"expected no telemetry record when cfg is nil")
}

func TestConnectionIdentifierShapeTelemetryHonorsEnvKillSwitch(t *testing.T) {
	// Only case-insensitive "true" disables, matching SF_DISABLE_MINICORE
	// and the CHANGELOG documentation.
	for _, v := range []string{"true", "True", "TRUE"} {
		t.Run("disabled_by_"+v, func(t *testing.T) {
			t.Setenv(disableConnectionShapeEnv, v)
			cfg := &Config{}
			sfconfig.SetInputShape(cfg, &sfconfig.ConnectionIdentifierShape{AccountProvided: true})
			sc := newTestShapeConn()
			sc.connectionIdentifierShapeTelemetry(cfg)
			assertEmptyE(t, sc.telemetry.logs,
				"expected no telemetry record when "+disableConnectionShapeEnv+"="+v)
		})
	}
}

func TestConnectionIdentifierShapeTelemetryEnvVarOffByDefault(t *testing.T) {
	// Anything other than case-insensitive "true" (including former truthy
	// aliases like "1" / "yes") must leave emission enabled.
	for _, v := range []string{"", "0", "1", "yes", "Yes", "false", "no", "anything-else"} {
		t.Run("not_disabled_by_"+v, func(t *testing.T) {
			t.Setenv(disableConnectionShapeEnv, v)
			cfg := &Config{}
			sfconfig.SetInputShape(cfg, &sfconfig.ConnectionIdentifierShape{AccountProvided: true})
			sc := newTestShapeConn()
			sc.connectionIdentifierShapeTelemetry(cfg)
			assertEqualF(t, len(sc.telemetry.logs), 1,
				"expected exactly one telemetry record when "+disableConnectionShapeEnv+"="+v)
		})
	}
}

func TestConnectionIdentifierShapeTelemetryRespectsTelemetryDisabled(t *testing.T) {
	t.Setenv(disableConnectionShapeEnv, "")
	cfg := &Config{}
	sfconfig.SetInputShape(cfg, &sfconfig.ConnectionIdentifierShape{AccountProvided: true})
	sc := newTestShapeConn()
	sc.telemetry.enabled = false // server said CLIENT_TELEMETRY_ENABLED=false
	sc.connectionIdentifierShapeTelemetry(cfg)
	assertEmptyE(t, sc.telemetry.logs,
		"expected no telemetry record when sc.telemetry.enabled=false")
}
