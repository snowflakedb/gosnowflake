package gosnowflake

import (
	"errors"
	"log"
	"strconv"
	"strings"
	"time"
)

var (
	errInvalidDSNNoSlash         = errors.New("invalid DSN: missing the slash separating the database name")
)

// Config is a configuration parsed from a DSN string
type Config struct {
	Account   string // Account name
	User      string // Username
	Password  string // Password (requires User)
	Database  string // Database name
	Schema    string // Schema
	Warehouse string // Warehouse
	Role      string // Role

	Protocol       string        // http or https (optional)
	Host           string        // hostname (optional)
	Port           int           // port (optional)
	ConnectTimeout time.Duration // Dial timeout
	RequestTimeout time.Duration // Request read time
	LoginTimeout   time.Duration // Login timeout
}

// ParseDSN parses the DSN string to a Config
func ParseDSN(dsn string) (cfg *Config, err error) {
	// New config with some default values
	cfg = &Config{}

	// user[:password]@account/database/schema[?param1=value1&paramN=valueN]
	// or
	// user[:password]@account/database[?param1=value1&paramN=valueN]
	// or
	// user[:password]@host:port/database/schema?account=user_account[?param1=value1&paramN=valueN]

	foundSlash := false
	secondSlash := false
	for i := len(dsn) - 1; i >= 0; i-- {
		if dsn[i] == '/' {
			foundSlash = true

			// left part is empty if i <= 0
			var j, k int
			posSecondSlash := i
			if i > 0 {
				for j = i; j >= 0; j-- {
					// username[:password]@...
					// Find the last '@' in dsn[:i]
					switch {
					case dsn[j] == '/':
						// second slash
						secondSlash = true
						posSecondSlash = j
					case dsn[j] == '@':
						// username[:password]
						// Find the first ':' in dsn[:j]
						for k = 0; k < j; k++ {
							if dsn[k] == ':' {
								cfg.Password = dsn[k+1: j]
								break
							}
						}
						cfg.User = dsn[:k]
					}
					if dsn[j] == '@' {
						break
					}
				}

				// account or host:port
				for k = j + 1; k < posSecondSlash; k++ {
					if dsn[k] == ':' {
						cfg.Port, err = strconv.Atoi(dsn[k+1: posSecondSlash])
						if err != nil {
							return
						}
						break
					}
				}
				cfg.Host = dsn[j+1: k]

			}
			// [?param1=value1&...&paramN=valueN]
			// Find the first '?' in dsn[i+1:]
			for j = i + 1; j < len(dsn); j++ {
				if dsn[j] == '?' {
					if err = parseDSNParams(cfg, dsn[j+1:]); err != nil {
						return
					}
					break
				}
			}
			if secondSlash {
				cfg.Database = dsn[posSecondSlash+1: i]
				cfg.Schema = dsn[i+1: j]
			} else {
				cfg.Database = dsn[i+1: j]
				cfg.Schema = "public"
			}

			break

		}
	}

	if !foundSlash && len(dsn) > 0 {
		return nil, errInvalidDSNNoSlash
	}
	if cfg.Protocol == "" {
		cfg.Protocol = "https"
	}
	if cfg.Port == 0 {
		cfg.Port = 443
	}
	if cfg.ConnectTimeout == 0 {
		cfg.ConnectTimeout = 60
	}
	if cfg.LoginTimeout == 0 {
		cfg.LoginTimeout = 120
	}
	log.Printf("ParseDSN: %s\n", cfg)
	return cfg, nil
}

// parseDSNParams parses the DSN "query string"
// Values must be url.QueryEscape'ed
func parseDSNParams(cfg *Config, params string) (err error) {
	for _, v := range strings.Split(params, "&") {
		param := strings.SplitN(v, "=", 2)
		if len(param) != 2 {
			continue
		}

		switch value := param[1]; param[0] {
		// Disable INFILE whitelist / enable all files
		case "account":
			cfg.Account = value
		case "warehouse":
			cfg.Warehouse = value
		case "database":
			cfg.Database = value
		case "schema":
			cfg.Schema = value
		case "role":
			cfg.Role = value
		case "protocol":
			cfg.Protocol = value
		}
	}
	return
}
