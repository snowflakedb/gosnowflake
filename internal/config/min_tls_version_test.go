package config

import (
	"crypto/tls"
	"testing"
)

func TestGetMinTLSVersion(t *testing.T) {
	tests := []struct {
		name     string
		envValue string
		expected uint16
		wantErr  bool
	}{
		{
			name:     "TLS 1.0",
			envValue: "1.0",
			expected: tls.VersionTLS10,
			wantErr:  false,
		},
		{
			name:     "TLS 1.1",
			envValue: "1.1",
			expected: tls.VersionTLS11,
			wantErr:  false,
		},
		{
			name:     "TLS 1.2",
			envValue: "1.2",
			expected: tls.VersionTLS12,
			wantErr:  false,
		},
		{
			name:     "TLS 1.3",
			envValue: "1.3",
			expected: tls.VersionTLS13,
			wantErr:  false,
		},
		{
			name:     "Empty env var",
			envValue: "",
			expected: 0,
			wantErr:  false,
		},
		{
			name:     "Invalid value",
			envValue: "1.4",
			expected: 0,
			wantErr:  true,
		},
		{
			name:     "Invalid format",
			envValue: "invalid",
			expected: 0,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.envValue == "" {
				t.Setenv(envVarMinTLSVersion, "")
			} else {
				t.Setenv(envVarMinTLSVersion, tt.envValue)
			}

			got, err := GetMinTLSVersion()
			if (err != nil) != tt.wantErr {
				t.Errorf("GetMinTLSVersion() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.expected {
				t.Errorf("GetMinTLSVersion() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestApplyMinTLSVersion(t *testing.T) {
	tests := []struct {
		name       string
		envValue   string
		inputTLS   *tls.Config
		wantMinVer uint16
		wantErr    bool
	}{
		{
			name:       "Apply to nil config",
			envValue:   "1.3",
			inputTLS:   nil,
			wantMinVer: tls.VersionTLS13,
			wantErr:    false,
		},
		{
			name:       "Apply to empty config",
			envValue:   "1.3",
			inputTLS:   &tls.Config{},
			wantMinVer: tls.VersionTLS13,
			wantErr:    false,
		},
		{
			name:     "Override existing MinVersion",
			envValue: "1.3",
			inputTLS: &tls.Config{
				MinVersion: tls.VersionTLS10,
			},
			wantMinVer: tls.VersionTLS13,
			wantErr:    false,
		},
		{
			name:     "No env var set",
			envValue: "",
			inputTLS: &tls.Config{
				MinVersion: tls.VersionTLS12,
			},
			wantMinVer: tls.VersionTLS12,
			wantErr:    false,
		},
		{
			name:       "Invalid env value",
			envValue:   "invalid",
			inputTLS:   &tls.Config{},
			wantMinVer: 0,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.envValue == "" {
				t.Setenv(envVarMinTLSVersion, "")
			} else {
				t.Setenv(envVarMinTLSVersion, tt.envValue)
			}

			got, err := ApplyMinTLSVersion(tt.inputTLS)
			if (err != nil) != tt.wantErr {
				t.Errorf("ApplyMinTLSVersion() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err == nil {
				if got.MinVersion != tt.wantMinVer {
					t.Errorf("ApplyMinTLSVersion() MinVersion = %v, want %v", got.MinVersion, tt.wantMinVer)
				}
			}
		})
	}
}

func TestGetTLSVersionName(t *testing.T) {
	tests := []struct {
		name     string
		version  uint16
		expected string
	}{
		{
			name:     "TLS 1.0",
			version:  tls.VersionTLS10,
			expected: "1.0",
		},
		{
			name:     "TLS 1.1",
			version:  tls.VersionTLS11,
			expected: "1.1",
		},
		{
			name:     "TLS 1.2",
			version:  tls.VersionTLS12,
			expected: "1.2",
		},
		{
			name:     "TLS 1.3",
			version:  tls.VersionTLS13,
			expected: "1.3",
		},
		{
			name:     "Unknown version",
			version:  0x9999,
			expected: "unknown (0x9999)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getTLSVersionName(tt.version)
			if got != tt.expected {
				t.Errorf("getTLSVersionName() = %v, want %v", got, tt.expected)
			}
		})
	}
}
