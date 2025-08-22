package gosnowflake

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/pem"
	"fmt"
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
}

// test case types
type tcDiagnosticClient struct {
	name            string
	config          *Config
	expectedTimeout time.Duration
}

type tcOpenAllowlistJSON struct {
	name           string
	setup          func() (string, func())
	shouldError    bool
	expectedLength int
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

func TestCreateDiagnosticClient(t *testing.T) {
	var diagTest connectivityDiagnoser
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
			client := diagTest.createDiagnosticClient(tc.config)

			assertNotNilE(t, client, "client should not be nil")
			assertEqualE(t, client.Timeout, tc.expectedTimeout, "timeout did not match")
			assertNotNilE(t, client.Transport, "transport should not be nil")
		})
	}
}

func TestCreateDiagnosticDialContext(t *testing.T) {
	var diagTest connectivityDiagnoser
	dialContext := diagTest.createDiagnosticDialContext()

	assertNotNilE(t, dialContext, "dialContext should not be nil")

	// new simple server to test basic connectivity
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	u, _ := url.Parse(server.URL)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := dialContext(ctx, "tcp", u.Host)
	assertNilE(t, err, "error should be nil")
}

func TestCreateDiagnosticTransport(t *testing.T) {
	var diagTest connectivityDiagnoser
	config := &Config{}
	transport := diagTest.createDiagnosticTransport(config)

	assertNotNilE(t, transport, "transport should not be nil")
	assertNotNilE(t, transport.DialContext, "dialContext should not be nil")

	// by default we should use the SnowflakeTransport
	assertTransportsEqual(t, SnowflakeTransport, transport, "diagnostic transport vs SnowflakeTransport")
}

func TestOpenAndReadAllowlistJSON(t *testing.T) {
	var diagTest connectivityDiagnoser
	testcases := []tcOpenAllowlistJSON{
		{
			name: "Open and Read Allowlist - valid file path, 2 entries",
			// create a temp allowlist file and then delete it
			setup: func() (filePath string, cleanup func()) {
				content := `[{"host":"myaccount.snowflakecomputing.com","port":443,"type":"SNOWFLAKE_DEPLOYMENT"},{"host":"ocsp.snowflakecomputing.com","port":80,"type":"OCSP_CACHE"}]`
				tmpFile, err := os.CreateTemp("", "allowlist_*.json")
				assertNilF(t, err, "Error during creating temp allowlist file.")
				_, err = tmpFile.WriteString(content)
				assertNilF(t, err, "Error during writing temp allowlist file.")
				tmpFile.Close()

				return tmpFile.Name(), func() { os.Remove(tmpFile.Name()) }
			},
			shouldError:    false,
			expectedLength: 2,
		},
		{
			name: "Open and Read Allowlist - empty file path",
			setup: func() (filePath string, cleanup func()) {
				content := `[{"host":"myaccount.snowflakecomputing.com","port":443,"type":"SNOWFLAKE_DEPLOYMENT"}]`
				_ = os.WriteFile("allowlist.json", []byte(content), 0644)

				return "", func() { os.Remove("allowlist.json") }
			},
			shouldError:    false,
			expectedLength: 1,
		},
		{
			name: "Open and Read Allowlist - non existent file",
			setup: func() (filePath string, cleanup func()) {
				return "/non/existent/file.json", func() {}
			},
			shouldError:    true,
			expectedLength: 0,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			filePath, cleanup := tc.setup()
			defer cleanup()

			allowlist, err := diagTest.openAndReadAllowlistJSON(filePath)

			if tc.shouldError {
				assertNotNilE(t, err, "error should not be nil")
			} else {
				assertNilE(t, err, "error should be nil")
				assertNotNilE(t, allowlist, "file content should not be nil")
				assertEqualE(t, len(allowlist.Entries), tc.expectedLength, "allowlist length did not match")
			}
		})
	}
}

func TestIsAcceptableStatusCode(t *testing.T) {
	var diagTest connectivityDiagnoser
	acceptableCodes := []int{http.StatusOK, http.StatusForbidden, http.StatusBadRequest}

	testcases := []tcAcceptableStatusCode{
		{http.StatusOK, true},
		{http.StatusForbidden, true},
		{http.StatusInternalServerError, false},
		{http.StatusUnauthorized, false},
		{http.StatusBadRequest, true},
	}

	for _, tc := range testcases {
		t.Run(fmt.Sprintf("Is Acceptable Status Code - status %d", tc.statusCode), func(t *testing.T) {
			result := diagTest.isAcceptableStatusCode(tc.statusCode, acceptableCodes)
			assertEqualE(t, result, tc.isAcceptable, "http status code acceptance is wrong")
		})
	}
}

func TestFetchCRL(t *testing.T) {
	var diagTest connectivityDiagnoser
	config := &Config{
		ClientTimeout: 30 * time.Second,
	}
	diagTest.diagnosticClient = diagTest.createDiagnosticClient(config)
	crlPEM := `-----BEGIN X509 CRL-----
MIIBuDCBoQIBATANBgkqhkiG9w0BAQsFADBeMQswCQYDVQQGEwJVUzELMAkGA1UE
CAwCQ0ExDTALBgNVBAcMBFRlc3QxEDAOBgNVBAoMB0V4YW1wbGUxDzANBgNVBAsM
BlRlc3RDQTEQMA4GA1UEAwwHVGVzdCBDQRcNMjUwNzI1MTYyMTQzWhcNMzMxMDEx
MTYyMTQzWqAPMA0wCwYDVR0UBAQCAhAAMA0GCSqGSIb3DQEBCwUAA4IBAQCakfe4
yaYe6jhSVZc177/y7a+qV6Vs8Ly+CwQiYCM/LieEI7coUpcMtF43ShfzG5FawrMI
xa3L2ew5EHDPelrMAdc56GzGCZFlOp16++3HS8qUpodctMdWWcR9Jn0OAfR1I3cY
KtLfQbYqwr+Ti6LT0SDp8kXhltH8ZfUcDWH779WF1IQatu5J+GoyHnfFCsP9gI3H
Aacyfk7Pp7MyAUChvbM6miyUbWm5NLW9nZgmMxqi9VpMnGZSCwqpS9J01k8YAbwS
S3HAS4o7ePBmhiERTPjqmwqEUdrKzEYMtdCFHHfnnDSZxdAmb+Ep6WjRgU1AHxak
6aJpJF0+Ic2kaXXI
-----END X509 CRL-----`
	block, _ := pem.Decode([]byte(crlPEM))
	testcases := []tcFetchCRL{
		{
			name: "Fetch CRL - successful fetch",
			setupServer: func() *httptest.Server {
				return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write(block.Bytes)
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

			err := diagTest.fetchCRL(server.URL)

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
	var diagTest connectivityDiagnoser
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
			req, err := diagTest.createRequest(tc.uri)

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
	var diagTest connectivityDiagnoser
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
			name: "Do HTTP - (CHINA) ocsp.snowflakecomputing.cn url modification",
			setupServer: func() *httptest.Server {
				return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					assertStringContainsE(t, r.URL.Path, "ocsp_response_cache.json", "url path should contain ocsp_response_cache.json added")
					w.WriteHeader(http.StatusOK)
				}))
			},
			setupRequest: func(serverURL string) *http.Request {
				req, _ := http.NewRequest("GET", serverURL, nil)
				req.URL.Host = "ocsp.snowflakecomputing.cn"
				return req
			},
			// http://ocsp.snowflakecomputing.cn/ocsp_response_cache.json throws HTTP404
			shouldError: true,
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

			// modify the diagnostic client to use a shorter timeout
			diagTest.diagnosticClient = &http.Client{Timeout: 10 * time.Second}

			req := tc.setupRequest(server.URL)
			err := diagTest.doHTTP(req)

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
	var diagTest connectivityDiagnoser
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

			// modify the diagnostic client to use a shorter timeout
			// and to ignore the server's certificate
			diagTest.diagnosticClient = &http.Client{
				Timeout: 10 * time.Second,
				Transport: &http.Transport{
					TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
				},
			}

			req, _ := http.NewRequest("GET", server.URL, nil)
			err := diagTest.doHTTPSGetCerts(req, tc.downloadCRLs)

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
	var diagTest connectivityDiagnoser
	config := &Config{
		ClientTimeout: 30 * time.Second,
	}
	diagTest.diagnosticClient = diagTest.createDiagnosticClient(config)

	t.Run("Check Proxy - with proxy configured", func(t *testing.T) {
		// setup test logger then restore original after test
		buffer, cleanup := setupTestLogger()
		defer cleanup()

		// set up transport with proxy
		proxyURL, _ := url.Parse("http://my.pro.xy:8080")
		diagTest.diagnosticClient.Transport = &http.Transport{
			Proxy: func(req *http.Request) (*url.URL, error) {
				return proxyURL, nil
			},
		}

		// this should generate a log output which indicates we use a proxy
		req, _ := http.NewRequest("GET", "https://myaccount.snowflakecomputing.com", nil)
		diagTest.checkProxy(req)

		logOutput := buffer.String()
		assertStringContainsE(t, logOutput, "[checkProxy] PROXY detected in the connection:", "log should contain proxy detection message")
		assertStringContainsE(t, logOutput, "http://my.pro.xy:8080", "log should contain the proxy URL")
	})

	t.Run("Check Proxy - no proxy configured", func(t *testing.T) {
		// setup test logger then restore original after test
		buffer, cleanup := setupTestLogger()
		defer cleanup()

		// set up transport without proxy
		diagTest.diagnosticClient.Transport = &http.Transport{
			Proxy: nil,
		}

		req, _ := http.NewRequest("GET", "https://myaccount.snowflakecomputing.com", nil)
		diagTest.checkProxy(req)

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
		diagTest.diagnosticClient.Transport = &http.Transport{
			Proxy: func(req *http.Request) (*url.URL, error) {
				return nil, fmt.Errorf("proxy configuration error")
			},
		}

		req, _ := http.NewRequest("GET", "https://myaccount.snowflakecomputing.com", nil)
		diagTest.checkProxy(req)

		// verify log output contains error message
		logOutput := buffer.String()
		assertStringContainsE(t, logOutput, "[checkProxy] problem determining PROXY:", "log should contain proxy error message")
		assertStringContainsE(t, logOutput, "proxy configuration error", "log should contain the specific error")
	})
}

func TestResolveHostname(t *testing.T) {
	var diagTest connectivityDiagnoser
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

			diagTest.resolveHostname(tc.hostname)

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
	var diagTest connectivityDiagnoser

	// setup diagnostic client for tests
	config := &Config{
		ClientTimeout: 30 * time.Second,
	}
	diagTest.diagnosticClient = diagTest.createDiagnosticClient(config)

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

			err := diagTest.performConnectivityCheck(tc.entryType, tc.host, tc.port, tc.downloadCRLs)

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
			ClientTimeout:                      30 * time.Second,
		}

		// perform the diagnosis without downloading CRL
		performDiagnosis(config, false)

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

		_, err = tmpFile.WriteString(allowlistContent)
		assertNilF(t, err, "Failed to write temp allowlist.json.")
		tmpFile.Close()

		config := &Config{
			ConnectionDiagnosticsAllowlistFile: tmpFile.Name(),
			CertRevocationCheckMode:            CertRevocationCheckAdvisory,
			ClientTimeout:                      30 * time.Second,
			DisableOCSPChecks:                  true,
		}
		downloadCRLs := config.CertRevocationCheckMode.String() == "ADVISORY"
		// driver should download CRLs due to ADVISORY CRL mode
		// Note that there's a log.Fatalf in performDiagnosis that may cause the test to fail.
		performDiagnosis(config, downloadCRLs)

		// verify expected log messages including CRL download
		logOutput := buffer.String()
		assertStringContainsE(t, logOutput, "[performDiagnosis] starting connectivity diagnosis", "should contain diagnosis start message")
		assertStringContainsE(t, logOutput, "[performDiagnosis] CRLs will be attempted to be downloaded and parsed during https tests", "should contain CRL download enabled message")

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
