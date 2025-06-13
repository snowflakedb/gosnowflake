package gosnowflake

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
)

/*
 * for the tests, we need to capture log output and assert on their content
 * this is done by creating a fresh logger to log into a buffer and look at that buffer
 * but we also need to preserve the original global logger to not modify that with tests
 * and restore original logger after the tests
 */
func setupTestLogger() (buffer *bytes.Buffer, cleanup func()) {
	originalLogger := logger
	testLogger := CreateDefaultLogger() // from log.go
	buffer = &bytes.Buffer{}
	testLogger.SetOutput(buffer)
	_ = testLogger.SetLogLevel("INFO")
	logger = testLogger

	cleanup = func() {
		logger = originalLogger
	}

	return buffer, cleanup
}

func TestSetupTestLogger(t *testing.T) {
	// save  original global logger
	originalLogger := logger
	// and restore it after test
	defer func() { logger = originalLogger }()

	t.Run("Setup Test Logger - buffer capture and cleanup", func(t *testing.T) {
		buffer, cleanup := setupTestLogger()

		assertNotNilE(t, buffer, "buffer should not be nil")
		assertNotNilE(t, cleanup, "cleanup function should not be nil")

		// the test message should be in the buffer
		testMessage := "test log message for setupTestLogger"
		logger.Info(testMessage)
		logOutput := buffer.String()
		assertStringContainsE(t, logOutput, testMessage, "buffer should capture log output")

		// now cleanup
		cleanup()
		assertEqualE(t, logger, originalLogger, "cleanup should restore original logger")

		// clear the buffer, log a new message into it
		// logs should not go to the test logger anymore
		buffer.Reset()
		logger.Info("this should not appear in test buffer")
		assertEqualE(t, buffer.String(), "", "buffer should be empty after cleanup")
	})
}

// test case types
type tcAllowlistEntry struct {
	host      string
	port      int
	entryType string
}

type tcDiagnosticClient struct {
	name            string
	config          *Config
	expectedTimeout time.Duration
}

type tcOpenAllowlistJSON struct {
	name        string
	setup       func() (string, func())
	shouldError bool
}

type tcParseAllowlistJSON struct {
	name           string
	content        string
	shouldError    bool
	expectedLength int
}

type tcPrivateLinkHost struct {
	hostname      string
	isPrivateLink bool
}

type tcPrivateIP struct {
	ip          string
	isPrivateIP bool
}

type tcAcceptableStatusCode struct {
	statusCode   int
	isAcceptable bool
}

type tcFetchCRL struct {
	name          string
	setupServer   func() *httptest.Server
	shouldError   bool
	errorContains string
}

type tcCreateRequest struct {
	name        string
	uri         string
	shouldError bool
}

type tcDoHTTP struct {
	name          string
	setupServer   func() *httptest.Server
	setupRequest  func(serverURL string) *http.Request
	shouldError   bool
	errorContains string
}

type tcDoHTTPSGetCerts struct {
	name          string
	setupServer   func() *httptest.Server
	downloadCRLs  bool
	shouldError   bool
	errorContains string
}

type tcResolveHostname struct {
	name     string
	hostname string
}

type tcPerformConnectivityCheck struct {
	name         string
	entryType    string
	host         string
	port         int
	downloadCRLs bool
	expectedLog  string
}

func TestAllowlistEntry(t *testing.T) {
	testcases := []tcAllowlistEntry{
		{"myaccount.snowflakecomputing.com", 443, "SNOWFLAKE_DEPLOYMENT"},
		{"ocsp.snowflakecomputing.com", 80, "OCSP_CACHE"},
	}

	for _, tc := range testcases {
		t.Run(fmt.Sprintf("%s_%d_%s", tc.host, tc.port, tc.entryType), func(t *testing.T) {
			entry := AllowlistEntry{
				Host: tc.host,
				Port: tc.port,
				Type: tc.entryType,
			}

			assertEqualE(t, entry.Host, tc.host, "host did not match")
			assertEqualE(t, entry.Port, tc.port, "port did not match")
			assertEqualE(t, entry.Type, tc.entryType, "type did not match")
		})
	}
}

func TestCreateDiagnosticClient(t *testing.T) {
	testcases := []tcDiagnosticClient{
		{
			name: "Diagnostic Client with default timeout",
			config: &Config{
				ClientTimeout: 0,
			},
			expectedTimeout: defaultClientTimeout,
		},
		{
			name: "Diagnostic Client with custom timeout",
			config: &Config{
				ClientTimeout: 60 * time.Second,
			},
			expectedTimeout: 60 * time.Second,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			client := createDiagnosticClient(tc.config)

			assertNotNilE(t, client, "client should not be nil")
			assertEqualE(t, client.Timeout, tc.expectedTimeout, "timeout did not match")
			assertNotNilE(t, client.Transport, "transport should not be nil")
		})
	}
}

func TestCreateDiagnosticDialContext(t *testing.T) {
	t.Run("Diagnostic DialContext creation", func(t *testing.T) {
		dialContext := createDiagnosticDialContext()

		assertNotNilE(t, dialContext, "dialContext should not be nil")

		// new simple server to test basic connectivity
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		u, _ := url.Parse(server.URL)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		conn, err := dialContext(ctx, "tcp", u.Host)
		assertNilE(t, err, "error should be nil")
		defer conn.Close()

	})
}

func TestCreateDiagnosticTransport(t *testing.T) {
	t.Run("Diagnostic Transport creation", func(t *testing.T) {
		config := &Config{}
		transport := createDiagnosticTransport(config)

		assertNotNilE(t, transport, "transport should not be nil")
		assertNotNilE(t, transport.DialContext, "dialContext should not be nil")

		// by default we should use the SnowflakeTransport
		assertEqualE(t, transport.TLSClientConfig, SnowflakeTransport.TLSClientConfig, "TLSClientConfig did not match with SnowflakeTransport default")
		assertEqualE(t, transport.MaxIdleConns, SnowflakeTransport.MaxIdleConns, "MaxIdleConns did not match with SnowflakeTransport default")
		assertEqualE(t, transport.IdleConnTimeout, SnowflakeTransport.IdleConnTimeout, "IdleConnTimeout did not match with SnowflakeTransport default")
		if (transport.Proxy == nil) != (SnowflakeTransport.Proxy == nil) {
			t.Errorf("Proxy function presence should match SnowflakeTransport default")
		}
	})
}

func TestOpenAndReadAllowlistJSON(t *testing.T) {
	testcases := []tcOpenAllowlistJSON{
		{
			name: "Open and Read Allowlist - valid file path",
			// create a temp allowlist file and then delete it
			setup: func() (filePath string, cleanup func()) {
				content := `[{"host":"myaccount.snowflakecomputing.com","port":443,"type":"SNOWFLAKE_DEPLOYMENT"}]`
				tmpFile, _ := os.CreateTemp("", "allowlist_*.json")
				_, _ = tmpFile.WriteString(content)
				tmpFile.Close()

				return tmpFile.Name(), func() { os.Remove(tmpFile.Name()) }
			},
			shouldError: false,
		},
		{
			name: "Open and Read Allowlist - empty file path",
			setup: func() (filePath string, cleanup func()) {
				content := `[{"host":"myaccount.snowflakecomputing.com","port":443,"type":"SNOWFLAKE_DEPLOYMENT"}]`
				_ = os.WriteFile("allowlist.json", []byte(content), 0644)

				return "", func() { os.Remove("allowlist.json") }
			},
			shouldError: false,
		},
		{
			name: "Open and Read Allowlist - non existent file",
			setup: func() (filePath string, cleanup func()) {
				return "/non/existent/file.json", func() {}
			},
			shouldError: true,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			filePath, cleanup := tc.setup()
			defer cleanup()

			content, err := openAndReadAllowlistJSON(filePath)

			if tc.shouldError {
				assertNotNilE(t, err, "error should not be nil")
			} else {
				assertNilE(t, err, "error should be nil")
				assertNotNilE(t, content, "file content should not be nil")
			}
		})
	}
}

func TestParseAllowlistJSON(t *testing.T) {
	testcases := []tcParseAllowlistJSON{
		{
			name:           "Parse Allowlist - valid JSON",
			content:        `[{"host":"myaccount.snowflakecomputing.com","port":443,"type":"SNOWFLAKE_DEPLOYMENT"}]`,
			shouldError:    false,
			expectedLength: 1,
		},
		{
			name:           "Parse Allowlist - multiple entries",
			content:        `[{"host":"myaccount.snowflakecomputing.com","port":443,"type":"SNOWFLAKE_DEPLOYMENT"},{"host":"ocsp.snowflakecomputing.com","port":80,"type":"OCSP_CACHE"}]`,
			shouldError:    false,
			expectedLength: 2,
		},
		{
			name:           "Parse Allowlist - invalid JSON",
			content:        `{a fully invalid json}`,
			shouldError:    true,
			expectedLength: 0,
		},
		{
			name:           "Parse Allowlist - empty array",
			content:        `[]`,
			shouldError:    false,
			expectedLength: 0,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			allowlist, err := parseAllowlistJSON([]byte(tc.content))

			if tc.shouldError {
				assertNotNilE(t, err, "error should not be nil")
			} else {
				assertNilE(t, err, "error should be nil")
			}

			assertEqualE(t, len(allowlist.Entries), tc.expectedLength, "allowlist length did not match")
		})
	}
}

func TestIsPrivateLinkHost(t *testing.T) {
	testcases := []tcPrivateLinkHost{
		{"myaccount.eu-west-1.privatelink.snowflakecomputing.com", true},
		{"myorg-myaccount.privatelink.snowflakecomputing.com", true},
		{"myaccount.snowflakecomputing.com", false},
		{"myorg-myaccount.snowflakecomputing.com", false},
	}

	for _, tc := range testcases {
		t.Run(tc.hostname, func(t *testing.T) {
			result := isPrivateLinkHost(tc.hostname)
			assertEqualE(t, result, tc.isPrivateLink, "could not determine if host is private link")
		})
	}
}

func TestIsPrivateIP(t *testing.T) {
	testcases := []tcPrivateIP{
		{"192.168.1.1", true},
		{"10.20.30.40", true},
		{"172.16.1.1", true},
		{"8.8.8.8", false},
		{"172.32.1.1", false},
		{"100.22.16.135", false},
	}

	for _, tc := range testcases {
		t.Run(tc.ip, func(t *testing.T) {
			ip := net.ParseIP(tc.ip)
			assertNotNilE(t, ip, fmt.Sprintf("failed to parse IP: %s", tc.ip))

			result := isPrivateIP(ip)
			assertEqualE(t, result, tc.isPrivateIP, "could not determine if IP is private")
		})
	}
}

func TestIsAcceptableStatusCode(t *testing.T) {
	acceptableCodes := []int{http.StatusOK, http.StatusForbidden}

	testcases := []tcAcceptableStatusCode{
		{http.StatusOK, true},
		{http.StatusForbidden, true},
		{http.StatusInternalServerError, false},
		{http.StatusUnauthorized, false},
		{http.StatusBadRequest, false},
	}

	for _, tc := range testcases {
		t.Run(fmt.Sprintf("Is Acceptable Status Code - status %d", tc.statusCode), func(t *testing.T) {
			result := isAcceptableStatusCode(tc.statusCode, acceptableCodes)
			assertEqualE(t, result, tc.isAcceptable, "http status code acceptance is wrong")
		})
	}
}

func TestFetchCRL(t *testing.T) {
	testcases := []tcFetchCRL{
		{
			name: "Fetch CRL - successful fetch",
			setupServer: func() *httptest.Server {
				return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write([]byte("Im the CRL data"))
				}))
			},
			shouldError: false,
		},
		{
			name: "Fetch CRL - server returns 404",
			setupServer: func() *httptest.Server {
				return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusNotFound)
				}))
			},
			shouldError:   true,
			errorContains: "HTTP response status",
		},
		{
			name: "Fetch CRL - server returns 500",
			setupServer: func() *httptest.Server {
				return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusInternalServerError)
				}))
			},
			shouldError:   true,
			errorContains: "HTTP response status",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			server := tc.setupServer()
			defer server.Close()

			err := fetchCRL(server.URL)

			if tc.shouldError {
				assertNotNilE(t, err, "error should not be nil")
				if tc.errorContains != "" {
					assertStringContainsE(t, err.Error(), tc.errorContains, "error message should contain the expected string")
				}
			} else {
				assertNilE(t, err, "error should be nil")
			}
		})
	}
}

func TestCreateRequest(t *testing.T) {
	testcases := []tcCreateRequest{
		{
			name:        "Create Request - valid http url",
			uri:         "http://ocsp.snowflakecomputing.com",
			shouldError: false,
		},
		{
			name:        "Create Request - valid https url",
			uri:         "https://myaccount.snowflakecomputing.com",
			shouldError: false,
		},
		{
			name:        "Create Request - invalid url",
			uri:         ":/invalid-url",
			shouldError: true,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			req, err := createRequest(tc.uri)

			if tc.shouldError {
				assertNotNilE(t, err, "error should not be nil")
			} else {
				assertNilE(t, err, "error should be nil")
				assertNotNilE(t, req, "request should not be nil")
				assertEqualE(t, req.Method, "GET", "method should be GET")
				assertEqualE(t, req.URL.String(), tc.uri, "url should match")
			}
		})
	}
}

func TestDoHTTP(t *testing.T) {
	// 'client' is the global diagnostic client, lets preserve the original value
	// because it will be modified during test
	originalClient := client
	// after the test, restore the original value
	defer func() { client = originalClient }()

	testcases := []tcDoHTTP{
		// simple disposable server to test basic connectivity
		{
			name: "Do HTTP - successful http request",
			setupServer: func() *httptest.Server {
				return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusOK)
				}))
			},
			setupRequest: func(serverURL string) *http.Request {
				req, _ := http.NewRequest("GET", serverURL, nil)
				return req
			},
			shouldError: false,
		},
		{
			name: "Do HTTP - ocsp.snowflakecomputing.com url modification",
			setupServer: func() *httptest.Server {
				return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					// doHTTP should automatically add ocsp_response_cache.json to the full url
					assertStringContainsE(t, r.URL.Path, "ocsp_response_cache.json", "url path should contain ocsp_response_cache.json added")
					w.WriteHeader(http.StatusOK)
				}))
			},
			setupRequest: func(serverURL string) *http.Request {
				req, _ := http.NewRequest("GET", serverURL, nil)
				req.URL.Host = "ocsp.snowflakecomputing.com"
				return req
			},
			shouldError: false,
		},
		{
			name: "Do HTTP - server returns forbidden, acceptable",
			setupServer: func() *httptest.Server {
				return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusForbidden)
				}))
			},
			setupRequest: func(serverURL string) *http.Request {
				req, _ := http.NewRequest("GET", serverURL, nil)
				return req
			},
			shouldError: false,
		},
		{
			name: "Do HTTP - server returns internal server error, not acceptable",
			setupServer: func() *httptest.Server {
				return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusInternalServerError)
				}))
			},
			setupRequest: func(serverURL string) *http.Request {
				req, _ := http.NewRequest("GET", serverURL, nil)
				return req
			},
			shouldError:   true,
			errorContains: "HTTP response status",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			server := tc.setupServer()
			defer server.Close()

			// modify the global diagnostic client to use a shorter timeout
			client = &http.Client{Timeout: 10 * time.Second}

			req := tc.setupRequest(server.URL)
			err := doHTTP(req)

			if tc.shouldError {
				assertNotNilE(t, err, "error should not be nil")
				if tc.errorContains != "" {
					assertStringContainsE(t, err.Error(), tc.errorContains, "error message should contain the expected string")
				}
			} else {
				assertNilE(t, err, "error should be nil")
			}
		})
	}
}

func TestDoHTTPSGetCerts(t *testing.T) {
	// 'client' is the global diagnostic client, lets preserve the original value
	// because it will be modified during test
	originalClient := client
	// after the test, restore the original value
	defer func() { client = originalClient }()

	testcases := []tcDoHTTPSGetCerts{
		// simple disposable server with TLS to test basic connectivity
		{
			name: "Do HTTPS - successful https request",
			setupServer: func() *httptest.Server {
				return httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusOK)
				}))
			},
			downloadCRLs: false,
			shouldError:  false,
		},
		{
			name: "Do HTTPS - server returns forbidden, acceptable",
			setupServer: func() *httptest.Server {
				return httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusForbidden)
				}))
			},
			downloadCRLs: false,
			shouldError:  false,
		},
		{
			name: "Do HTTPS - server returns internal server error, not acceptable",
			setupServer: func() *httptest.Server {
				return httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusInternalServerError)
				}))
			},
			downloadCRLs:  false,
			shouldError:   true,
			errorContains: "HTTP response status",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			server := tc.setupServer()
			defer server.Close()

			// modify the global diagnostic client to use a shorter timeout
			// and to ignore the server's certificate
			client = &http.Client{
				Timeout: 10 * time.Second,
				Transport: &http.Transport{
					TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
				},
			}

			req, _ := http.NewRequest("GET", server.URL, nil)
			err := doHTTPSGetCerts(req, tc.downloadCRLs)

			if tc.shouldError {
				assertNotNilE(t, err, "error should not be nil")
				if tc.errorContains != "" {
					assertStringContainsE(t, err.Error(), tc.errorContains, "error message should contain the expected string")
				}
			} else {
				assertNilE(t, err, "error should be nil")
			}
		})
	}
}

func TestCheckProxy(t *testing.T) {
	// save the original transport because we gonna modify it during the test
	originalTransport := diagnosticTransport
	defer func() {
		diagnosticTransport = originalTransport
	}()

	t.Run("Check Proxy - with proxy configured", func(t *testing.T) {
		// setup test logger then restore original after test
		buffer, cleanup := setupTestLogger()
		defer cleanup()

		// set up transport with proxy
		proxyURL, _ := url.Parse("http://my.pro.xy:8080")
		diagnosticTransport = &http.Transport{
			Proxy: func(req *http.Request) (*url.URL, error) {
				return proxyURL, nil
			},
		}

		// this should generate a log output which indicates we use a proxy
		req, _ := http.NewRequest("GET", "https://myaccount.snowflakecomputing.com", nil)
		checkProxy(req)

		logOutput := buffer.String()
		assertStringContainsE(t, logOutput, "[checkProxy] PROXY detected in the connection:", "log should contain proxy detection message")
		assertStringContainsE(t, logOutput, "http://my.pro.xy:8080", "log should contain the proxy URL")
	})

	t.Run("Check Proxy - no proxy configured", func(t *testing.T) {
		// setup test logger then restore original after test
		buffer, cleanup := setupTestLogger()
		defer cleanup()

		// set up transport without proxy
		diagnosticTransport = &http.Transport{
			Proxy: nil,
		}

		req, _ := http.NewRequest("GET", "https://myaccount.snowflakecomputing.com", nil)
		checkProxy(req)

		// verify log output does NOT contain proxy detection
		logOutput := buffer.String()
		if strings.Contains(logOutput, "[checkProxy] PROXY detected") {
			t.Errorf("log should not contain proxy detection message when no proxy is configured, but got: %s", logOutput)
		}
	})

	t.Run("Check Proxy - proxy function returns error", func(t *testing.T) {
		// setup test logger then restore original after test
		buffer, cleanup := setupTestLogger()
		defer cleanup()

		// deliberately return an error from the proxy function
		diagnosticTransport = &http.Transport{
			Proxy: func(req *http.Request) (*url.URL, error) {
				return nil, fmt.Errorf("proxy configuration error")
			},
		}

		req, _ := http.NewRequest("GET", "https://myaccount.snowflakecomputing.com", nil)
		checkProxy(req)

		// verify log output contains error message
		logOutput := buffer.String()
		assertStringContainsE(t, logOutput, "[checkProxy] problem determining PROXY:", "log should contain proxy error message")
		assertStringContainsE(t, logOutput, "proxy configuration error", "log should contain the specific error")
	})
}

func TestResolveHostname(t *testing.T) {
	testcases := []tcResolveHostname{
		{
			name:     "Resolve Hostname - valid hostname myaccount.snowflakecomputing.com",
			hostname: "myaccount.snowflakecomputing.com",
		},
		{
			name:     "Resolve Hostname - invalid hostname",
			hostname: "this.is.invalid",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			// setup test logger then restore original after test
			buffer, cleanup := setupTestLogger()
			defer cleanup()

			resolveHostname(tc.hostname)

			logOutput := buffer.String()

			// check for expected log patterns based on hostname
			if tc.hostname == "this.is.invalid" {
				assertStringContainsE(t, logOutput, "[resolveHostname] error resolving hostname", "should contain error message for invalid hostname")
				assertStringContainsE(t, logOutput, tc.hostname, "should contain the hostname in error message")
			} else {
				// expect success message
				assertStringContainsE(t, logOutput, "[resolveHostname] resolved hostname", "should contain success message for valid hostname")
				assertStringContainsE(t, logOutput, tc.hostname, "should contain the hostname in success message")
			}
		})
	}
}

func TestPerformConnectivityCheck(t *testing.T) {
	// save the original global diagnostic client and transport as we'll modify them during test
	originalClient := client
	originalTransport := diagnosticTransport
	defer func() {
		client = originalClient
		diagnosticTransport = originalTransport
	}()

	// setup diagnostic client for tests
	config := &Config{
		ClientTimeout: 30 * time.Second,
	}
	client = createDiagnosticClient(config)
	diagnosticTransport = client.Transport.(*http.Transport)

	testcases := []tcPerformConnectivityCheck{
		{
			name:         "HTTP check for port 80",
			entryType:    "OCSP_CACHE",
			host:         "ocsp.snowflakecomputing.com",
			port:         80,
			downloadCRLs: false,
			expectedLog:  "[performConnectivityCheck] HTTP check",
		},
		{
			name:         "HTTPS check for port 443",
			entryType:    "DUMMY_SNOWFLAKE_DEPLOYMENT",
			host:         "www.snowflake.com",
			port:         443,
			downloadCRLs: false,
			expectedLog:  "[performConnectivityCheck] HTTPS check",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			// setup test logger then restore original after test
			buffer, cleanup := setupTestLogger()
			defer cleanup()

			err := performConnectivityCheck(tc.entryType, tc.host, tc.port, tc.downloadCRLs)

			logOutput := buffer.String()

			// verify expected log message appears
			assertStringContainsE(t, logOutput, tc.expectedLog, fmt.Sprintf("should contain '%s' log message", tc.expectedLog))
			assertStringContainsE(t, logOutput, tc.entryType, "should contain entry type in log")
			assertStringContainsE(t, logOutput, tc.host, "should contain host in log")

			// if error occurred, verify error log format
			if err != nil {
				assertStringContainsE(t, logOutput, "[performConnectivityCheck] error performing", "should contain error log message")
			}
		})
	}

}

func TestPerformDiagnosis(t *testing.T) {
	// save the original global diagnostic client and transport as we'll modify them during test
	originalClient := client
	originalTransport := diagnosticTransport
	defer func() {
		client = originalClient
		diagnosticTransport = originalTransport
	}()

	t.Run("Perform Diagnosis - CRL download disabled", func(t *testing.T) {
		// setup test logger then restore original after test
		buffer, cleanup := setupTestLogger()
		defer cleanup()

		allowlistContent := `[
			{"host":"ocsp.snowflakecomputing.com","port":80,"type":"OCSP_CACHE"},
			{"host":"www.snowflake.com","port":443,"type":"DUMMY_SNOWFLAKE_DEPLOYMENT"}
		]`

		tmpFile, err := os.CreateTemp("", "test_allowlist_*.json")
		assertNilE(t, err, "failed to create temp allowlist file")
		defer os.Remove(tmpFile.Name())

		_, _ = tmpFile.WriteString(allowlistContent)
		tmpFile.Close()

		config := &Config{
			ConnectionDiagnosticsAllowlistFile: tmpFile.Name(),
			ConnectionDiagnosticsDownloadCRL:   false,
			ClientTimeout:                      30 * time.Second,
		}

		// perform the diagnosis
		performDiagnosis(config)

		// verify expected log messages from performDiagnosis and underlying functions
		logOutput := buffer.String()
		assertStringContainsE(t, logOutput, "[performDiagnosis] starting connectivity diagnosis", "should contain diagnosis start message")

		// DNS resolution
		assertStringContainsE(t, logOutput, "[performDiagnosis] DNS check - resolving OCSP_CACHE hostname ocsp.snowflakecomputing.com", "should contain DNS check for OCSP cache")
		assertStringContainsE(t, logOutput, "[performDiagnosis] DNS check - resolving DUMMY_SNOWFLAKE_DEPLOYMENT hostname www.snowflake.com", "should contain DNS check for Snowflake host")
		assertStringContainsE(t, logOutput, "[resolveHostname] resolved hostname", "should contain hostname resolution results")

		// HTTP check
		assertStringContainsE(t, logOutput, "[performConnectivityCheck] HTTP check for OCSP_CACHE ocsp.snowflakecomputing.com", "should contain HTTP check message")
		assertStringContainsE(t, logOutput, "[createRequest] creating GET request to http://ocsp.snowflakecomputing.com", "should contain request creation log")
		assertStringContainsE(t, logOutput, "[doHTTP] testing HTTP connection to", "should contain HTTP connection test log")

		// HTTPS check
		assertStringContainsE(t, logOutput, "[performConnectivityCheck] HTTPS check for DUMMY_SNOWFLAKE_DEPLOYMENT www.snowflake.com", "should contain HTTPS check message")
		assertStringContainsE(t, logOutput, "[createRequest] creating GET request to https://www.snowflake.com", "should contain HTTPS request creation log")
		assertStringContainsE(t, logOutput, "[doHTTPSGetCerts] connecting to https://www.snowflake.com", "should contain HTTPS connection log")

		// diagnostic dial context
		assertStringContainsE(t, logOutput, "[createDiagnosticDialContext] Connected to", "should contain dial context connection logs")
		assertStringContainsE(t, logOutput, "remote IP:", "should contain remote IP information")

		// should NOT contain CRL download messages when disabled
		if strings.Contains(logOutput, "[performDiagnosis] CRLs will be attempted to be downloaded") {
			t.Errorf("should not contain CRL download message when disabled, but got: %s", logOutput)
		}
	})

	t.Run("Perform Diagnosis - CRL download enabled", func(t *testing.T) {
		// setup test logger then restore original after test
		buffer, cleanup := setupTestLogger()
		defer cleanup()

		// Create a temporary allowlist file with HTTPS entries to trigger CRL download attempts
		allowlistContent := `[
			{"host":"ocsp.snowflakecomputing.com","port":80,"type":"OCSP_CACHE"},
			{"host":"www.snowflake.com","port":443,"type":"DUMMY_SNOWFLAKE_DEPLOYMENT"}
		]`

		tmpFile, err := os.CreateTemp("", "test_allowlist_*.json")
		assertNilE(t, err, "failed to create temp allowlist file")
		defer os.Remove(tmpFile.Name())

		_, _ = tmpFile.WriteString(allowlistContent)
		tmpFile.Close()

		config := &Config{
			ConnectionDiagnosticsAllowlistFile: tmpFile.Name(),
			ConnectionDiagnosticsDownloadCRL:   true,
			ClientTimeout:                      30 * time.Second,
		}

		performDiagnosis(config)

		// verify that the global client was set
		assertNotNilE(t, client, "client should be set after performDiagnosis")
		assertNotNilE(t, diagnosticTransport, "diagnosticTransport should be set after performDiagnosis")

		// verify expected log messages including CRL download
		logOutput := buffer.String()
		assertStringContainsE(t, logOutput, "[performDiagnosis] starting connectivity diagnosis", "should contain diagnosis start message")
		assertStringContainsE(t, logOutput, "[performDiagnosis] CRLs will be attempted to be downloaded during https tests", "should contain CRL download enabled message")

		// DNS resolution
		assertStringContainsE(t, logOutput, "[performDiagnosis] DNS check - resolving OCSP_CACHE hostname ocsp.snowflakecomputing.com", "should contain DNS check for OCSP cache")
		assertStringContainsE(t, logOutput, "[performDiagnosis] DNS check - resolving DUMMY_SNOWFLAKE_DEPLOYMENT hostname www.snowflake.com", "should contain DNS check for Snowflake host")
		assertStringContainsE(t, logOutput, "[resolveHostname] resolved hostname", "should contain hostname resolution results")

		// HTTPS check
		assertStringContainsE(t, logOutput, "[performConnectivityCheck] HTTPS check for DUMMY_SNOWFLAKE_DEPLOYMENT www.snowflake.com", "should contain HTTPS check message")
		assertStringContainsE(t, logOutput, "[doHTTPSGetCerts] connecting to https://www.snowflake.com", "should contain HTTPS connection log")
		assertStringContainsE(t, logOutput, "[doHTTPSGetCerts] Retrieved", "should contain certificate retrieval log")
		assertStringContainsE(t, logOutput, "certificate(s)", "should contain certificate count information")

		// diagnostic dial context
		assertStringContainsE(t, logOutput, "[createDiagnosticDialContext] Connected to", "should contain dial context connection logs")
		assertStringContainsE(t, logOutput, "remote IP:", "should contain remote IP information")

		// CRL download
		// if certificate has CRLDistributionPoints this message is logged
		if strings.Contains(logOutput, "CRL Distribution Points:") {
			// and we should see CRL fetch attempts logged. we don't care if it's successful or not, we just want to see the log
			assertStringContainsE(t, logOutput, "[fetchCRL] fetching", "should contain CRL fetch attempt log")
		}
	})
}
