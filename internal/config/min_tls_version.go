package config

import (
	"crypto/tls"
	"fmt"
	"os"
)

const envVarMinTLSVersion = "SNOWFLAKE_MIN_TLS_VERSION"

// GetMinTLSVersion reads and parses the SNOWFLAKE_MIN_TLS_VERSION environment variable.
// Returns 0 if not set or invalid. Valid values: "1.0", "1.1", "1.2", "1.3".
func GetMinTLSVersion() (uint16, error) {
	value := os.Getenv(envVarMinTLSVersion)
	if value == "" {
		return 0, nil
	}

	switch value {
	case "1.0":
		return tls.VersionTLS10, nil
	case "1.1":
		return tls.VersionTLS11, nil
	case "1.2":
		return tls.VersionTLS12, nil
	case "1.3":
		return tls.VersionTLS13, nil
	default:
		return 0, fmt.Errorf("invalid %s value: %s. Valid values are: 1.0, 1.1, 1.2, 1.3", envVarMinTLSVersion, value)
	}
}

// ApplyMinTLSVersion applies the minimum TLS version from environment variable to the given tls.Config.
// This enforces a global security policy that cannot be bypassed by custom tls.Config.
func ApplyMinTLSVersion(tlsConfig *tls.Config) (*tls.Config, error) {
	minVersion, err := GetMinTLSVersion()
	if err != nil {
		return nil, err
	}

	if minVersion == 0 {
		// No minimum version set
		return tlsConfig, nil
	}

	if tlsConfig == nil {
		tlsConfig = &tls.Config{}
	}

	// Always set MinVersion when the env var is set, even if tlsConfig already has one
	// This ensures the environment variable acts as a security policy enforcement
	tlsConfig.MinVersion = minVersion
	logger.Infof("Applied minimum TLS version from %s: %s", envVarMinTLSVersion, getTLSVersionName(minVersion))

	return tlsConfig, nil
}

func getTLSVersionName(version uint16) string {
	switch version {
	case tls.VersionTLS10:
		return "1.0"
	case tls.VersionTLS11:
		return "1.1"
	case tls.VersionTLS12:
		return "1.2"
	case tls.VersionTLS13:
		return "1.3"
	default:
		return fmt.Sprintf("unknown (0x%04x)", version)
	}
}
