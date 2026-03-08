package gosnowflake

import (
	"database/sql"
	toml "github.com/BurntSushi/toml"
	"os"
	"strconv"
	"testing"
)

// TODO move this test to config package when we have wiremock support in an internal package
func TestTomlConnection(t *testing.T) {
	os.Setenv("SNOWFLAKE_HOME", "./test_data/")                       // TODO replace with snowflakeHome const
	os.Setenv("SNOWFLAKE_DEFAULT_CONNECTION_NAME", "toml-connection") // TODO replace with snowflakeConnectionName const

	defer os.Unsetenv("SNOWFLAKE_HOME")                    // TODO replace with snowflakeHome const
	defer os.Unsetenv("SNOWFLAKE_DEFAULT_CONNECTION_NAME") // TODO replace with snowflakeHome const
	wiremock.registerMappings(t,
		wiremockMapping{filePath: "auth/password/successful_flow.json"},
		wiremockMapping{filePath: "select1.json", params: map[string]string{
			"%AUTHORIZATION_HEADER%": "session token",
		}},
	)
	type Connection struct {
		Account  string `toml:"account"`
		User     string `toml:"user"`
		Password string `toml:"password"`
		Host     string `toml:"host"`
		Port     string `toml:"port"`
		Protocol string `toml:"protocol"`
	}

	type TomlStruct struct {
		Connection Connection `toml:"toml-connection"`
	}

	cfg := wiremock.connectionConfig()
	connection := &TomlStruct{
		Connection: Connection{
			Account:  cfg.Account,
			User:     cfg.User,
			Password: cfg.Password,
			Host:     cfg.Host,
			Port:     strconv.Itoa(cfg.Port),
			Protocol: cfg.Protocol,
		},
	}

	f, err := os.OpenFile("./test_data/connections.toml", os.O_APPEND|os.O_WRONLY, 0600)
	assertNilF(t, err, "Failed to create connections.toml file")
	defer f.Close()

	encoder := toml.NewEncoder(f)
	err = encoder.Encode(connection)
	assertNilF(t, err, "Failed to parse the config to toml structure")

	if !isWindows {
		err = os.Chmod("./test_data/connections.toml", 0600)
		assertNilF(t, err, "The error occurred because you cannot change the file permission")
	}

	db, err := sql.Open("snowflake", "autoConfig")
	assertNilF(t, err, "The error occurred because the db cannot be established")
	runSmokeQuery(t, db)
}
