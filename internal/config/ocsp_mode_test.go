package config

import (
	"os"
	"strings"
	"testing"
)

func unsetOCSPEnv(t *testing.T, key string) {
	t.Helper()
	prev, existed := os.LookupEnv(key)
	assertNilF(t, os.Unsetenv(key), "unset "+key)
	t.Cleanup(func() {
		if existed {
			assertNilF(t, os.Setenv(key, prev), "restore "+key)
			return
		}
		assertNilF(t, os.Unsetenv(key), "keep unset "+key)
	})
}

func TestOCSPResolver(t *testing.T) {
	unsetOCSPEnv(t, EnvVarDisableOCSPChecks)

	type row struct {
		name        string
		disable     bool
		env         *string
		failOpen    OCSPFailOpenMode
		wantEnabled bool
		wantMode    string
	}
	falseS := "false"
	trueS := "true"
	upperFalse := "FALSE"
	zeroS := "0"
	fS := "f"
	foo := "foo"
	yes := "yes"

	rows := []row{
		{name: "default off", wantMode: ocspModeInsecure},
		{name: "env false", env: &falseS, wantEnabled: true, wantMode: ocspModeFailOpen},
		{name: "env FALSE", env: &upperFalse, wantEnabled: true, wantMode: ocspModeFailOpen},
		{name: "env 0", env: &zeroS, wantEnabled: true, wantMode: ocspModeFailOpen},
		{name: "env f", env: &fS, wantEnabled: true, wantMode: ocspModeFailOpen},
		{name: "env true", env: &trueS, wantMode: ocspModeInsecure},
		{name: "explicit fail-open", failOpen: OCSPFailOpenTrue, wantEnabled: true, wantMode: ocspModeFailOpen},
		{name: "explicit fail-closed", failOpen: OCSPFailOpenFalse, wantEnabled: true, wantMode: ocspModeFailClosed},
		{name: "disable bool beats fail-open", disable: true, failOpen: OCSPFailOpenTrue, wantMode: ocspModeInsecure},
		{name: "disable bool beats fail-closed", disable: true, failOpen: OCSPFailOpenFalse, wantMode: ocspModeInsecure},
		{name: "hard disable beats env false", disable: true, env: &falseS, wantMode: ocspModeInsecure},
		{name: "disable bool beats fail-open and env false", disable: true, env: &falseS, failOpen: OCSPFailOpenTrue, wantMode: ocspModeInsecure},
		{name: "fail-closed beats env true", env: &trueS, failOpen: OCSPFailOpenFalse, wantEnabled: true, wantMode: ocspModeFailClosed},
		{name: "env true beats fail-open", env: &trueS, failOpen: OCSPFailOpenTrue, wantMode: ocspModeInsecure},
		{name: "invalid env foo", env: &foo, wantMode: ocspModeInsecure},
		{name: "invalid env yes still enables via fail-open", env: &yes, failOpen: OCSPFailOpenTrue, wantEnabled: true, wantMode: ocspModeFailOpen},
	}

	for _, tc := range rows {
		t.Run(tc.name, func(t *testing.T) {
			if tc.env != nil {
				t.Setenv(EnvVarDisableOCSPChecks, *tc.env)
			} else {
				unsetOCSPEnv(t, EnvVarDisableOCSPChecks)
			}
			cfg := &Config{DisableOCSPChecks: tc.disable, OCSPFailOpen: tc.failOpen}
			assertEqualF(t, OCSPEnabled(cfg), tc.wantEnabled, "OCSPEnabled")
			assertEqualF(t, OcspMode(cfg), tc.wantMode, "OcspMode")
		})
	}

	t.Run("empty env is unset", func(t *testing.T) {
		unsetOCSPEnv(t, EnvVarDisableOCSPChecks)
		cfg := &Config{}
		assertEqualF(t, OCSPEnabled(cfg), false, "unset env")
		assertEqualF(t, OcspMode(cfg), ocspModeInsecure, "unset env mode")
	})

	t.Run("resolver re-reads env live", func(t *testing.T) {
		cfg := &Config{}
		t.Setenv(EnvVarDisableOCSPChecks, "true")
		assertEqualF(t, OCSPEnabled(cfg), false, "env true")
		assertEqualF(t, OcspMode(cfg), ocspModeInsecure, "env true mode")
		t.Setenv(EnvVarDisableOCSPChecks, "false")
		assertEqualF(t, OCSPEnabled(cfg), true, "env flipped to false")
		assertEqualF(t, OcspMode(cfg), ocspModeFailOpen, "env flipped mode")
	})
}

func TestFillMissingDoesNotMutateOCSPFields(t *testing.T) {
	unsetOCSPEnv(t, EnvVarDisableOCSPChecks)
	cfg := &Config{Account: "ac", User: "u", Password: "p"}
	assertNilF(t, FillMissingConfigParameters(cfg), "fill")
	assertEqualF(t, cfg.DisableOCSPChecks, false, "bool stays unset")
	assertEqualF(t, cfg.OCSPFailOpen, OCSPFailOpenNotSet, "fail-mode stays NotSet")
	assertEqualF(t, OCSPEnabled(cfg), false, "still off")
	assertNilF(t, FillMissingConfigParameters(cfg), "second fill")
	assertEqualF(t, cfg.OCSPFailOpen, OCSPFailOpenNotSet, "idempotent")
}

func TestDSNOmitsUnsetOCSPFailOpen(t *testing.T) {
	unsetOCSPEnv(t, EnvVarDisableOCSPChecks)
	dsn, err := DSN(&Config{User: "u", Password: "p", Account: "a"})
	assertNilF(t, err, "DSN")
	assertFalseE(t, strings.Contains(dsn, "ocspFailOpen"), "omit when NotSet")
	assertFalseE(t, strings.Contains(dsn, "disableOCSPChecks"), "omit when bool unset")

	dsn, err = DSN(&Config{User: "u", Password: "p", Account: "a", OCSPFailOpen: OCSPFailOpenTrue})
	assertNilF(t, err, "DSN fail-open")
	assertTrueE(t, strings.Contains(dsn, "ocspFailOpen=true"), "emit explicit true")

	dsn, err = DSN(&Config{User: "u", Password: "p", Account: "a", OCSPFailOpen: OCSPFailOpenFalse})
	assertNilF(t, err, "DSN fail-closed")
	assertTrueE(t, strings.Contains(dsn, "ocspFailOpen=false"), "emit explicit false")

	dsn, err = DSN(&Config{User: "u", Password: "p", Account: "a", DisableOCSPChecks: true})
	assertNilF(t, err, "DSN disable")
	assertTrueE(t, strings.Contains(dsn, "disableOCSPChecks=true"), "emit explicit disable")
	assertFalseE(t, strings.Contains(dsn, "ocspFailOpen"), "still omit fail-mode when NotSet")
}
