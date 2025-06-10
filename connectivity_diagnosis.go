package gosnowflake

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

type AllowlistEntry struct {
	// structure of allowlist entries
	Host string `json:"host"`
	Port int    `json:"port"`
	Type string `json:"type"`
}

type Allowlist struct {
	// all the allowlist entries
	Entries []AllowlistEntry
}

var (
	diagnosticTransport *http.Transport
	client              *http.Client
	// acceptable HTTP status codes for connectivity diagnosis
	// for the sake of connectivity, e.g. 403 is perfectly fine
	acceptableStatusCodes = []int{http.StatusOK, http.StatusForbidden}
)

// create a diagnostic client with the appropriate transport for the given config
// by default SnowflakeTransport, if OCSP is disabled then snowflakeNoOcspTransport, if custom then that one
func createDiagnosticClient(cfg *Config) *http.Client {
	transport := createDiagnosticTransport(cfg)

	clientTimeout := cfg.ClientTimeout
	if clientTimeout == 0 {
		clientTimeout = defaultClientTimeout // 900 seconds
	}

	return &http.Client{
		Timeout:   clientTimeout,
		Transport: transport,
	}
}

// necessary to be able to log the IP address of the remote host to which we actually connected
// might be even different from the result of DNS resolution
func createDiagnosticDialContext() func(ctx context.Context, network, addr string) (net.Conn, error) {
	dialer := &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}

	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := dialer.DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}

		if remoteAddr := conn.RemoteAddr(); remoteAddr != nil {
			remoteIPStr := remoteAddr.String()
			// parse out just the IP (maybe port is present)
			if host, _, err := net.SplitHostPort(remoteIPStr); err == nil {
				remoteIPStr = host
			}

			// get hostname
			hostname, _, _ := net.SplitHostPort(addr)
			if hostname == "" {
				hostname = addr
			}

			logger.Infof("[createDiagnosticDialContext] Connected to %s (remote IP: %s)", hostname, remoteIPStr)
		}

		return conn, nil
	}
}

// enhance the transport with IP logging
func createDiagnosticTransport(cfg *Config) *http.Transport {
	// get transport based on OCSP / custom transport configuration
	// default; SnowflakeTransport with OCSP validation
	baseTransport := getTransport(cfg)

	var httpTransport *http.Transport
	if t, ok := baseTransport.(*http.Transport); ok {
		httpTransport = t
	} else {
		httpTransport = SnowflakeTransport
	}

	// return a new transport enhanced with remote IP logging
	// for SnowflakeNoOcspTransport, TLSClientConfig is nil
	return &http.Transport{
		TLSClientConfig: httpTransport.TLSClientConfig,
		MaxIdleConns:    httpTransport.MaxIdleConns,
		IdleConnTimeout: httpTransport.IdleConnTimeout,
		Proxy:           httpTransport.Proxy,
		DialContext:     createDiagnosticDialContext(),
	}
}

func openAndReadAllowlistJSON(filePath string) (fileContent []byte, err error) {
	if filePath == "" {
		logger.Info("[openAndReadAllowlistJSON] allowlist.json location not specified, trying to load from current directory.")
		filePath = "allowlist.json"
	}
	logger.Infof("[openAndReadAllowlistJSON] reading allowlist from %s.\n", filePath)
	fileContent, err = os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}
	return fileContent, nil
}

func parseAllowlistJSON(fileContent []byte) (allowlist Allowlist, err error) {
	logger.Debug("[parseAllowlistJSON] parsing allowlist.json")
	err = json.Unmarshal(fileContent, &allowlist.Entries)
	if err != nil {
		return allowlist, err
	}
	return allowlist, nil
}

// look up the host, using the local resolver
func resolveHostname(hostname string) {
	ips, err := net.LookupIP(hostname)
	if err != nil {
		logger.Errorf("[resolveHostname] error resolving hostname %s: %s\n", hostname, err)
		return
	}
	for _, ip := range ips {
		logger.Infof("[resolveHostname] resolved hostname %s to %s\n", hostname, ip.String())
		if isPrivateLinkHost(hostname) && !isPrivateIP(ip) {
			logger.Errorf("[resolveHostname] this hostname should resolve to a private IP, but %s is public IP. Please, check your DNS configuration.\n", ip.String())
		}
	}
}

func isPrivateLinkHost(hostname string) bool {
	return strings.HasSuffix(hostname, ".privatelink.snowflakecomputing.com")
}

func isPrivateIP(ip net.IP) bool {
	return ip.IsPrivate()
}

func isAcceptableStatusCode(statusCode int, acceptableCodes []int) bool {
	for _, code := range acceptableCodes {
		if statusCode == code {
			return true
		}
	}
	return false
}

func fetchCRL(uri string) error {
	logger.Infof("[fetchCRL] fetching  %s\n", uri)
	resp, err := http.Get(uri)
	if err != nil {
		return fmt.Errorf("[fetchCRL] HTTP GET to %s endpoint failed: %w", uri, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("[fetchCRL] HTTP response status from endpoint: %s", resp.Status)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("[fetchCRL] failed to read response body: %w", err)
	}
	// TODO: do we want to persist CRL to disk ? (then cleanup, deal with read-only fs, etc)
	logger.Infof("[fetchCRL] %s retrieved successfully (%d bytes), not persisting to disk.\n", uri, len(body))
	return nil
}

func doHTTP(request *http.Request) error {
	if request.URL.Host == "ocsp.snowflakecomputing.com" {
		fullOCSPCacheURI := request.URL.String() + "/ocsp_response_cache.json"
		newURL, _ := url.Parse(fullOCSPCacheURI)
		request.URL = newURL
	}
	logger.Infof("[doHTTP] testing HTTP connection to %s\n", request.URL.String())
	//resp, err := http.DefaultClient.Do(request)
	resp, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("HTTP GET to %s endpoint failed: %w", request.URL.String(), err)
	}
	defer resp.Body.Close()

	if !isAcceptableStatusCode(resp.StatusCode, acceptableStatusCodes) {
		return fmt.Errorf("HTTP response status from %s endpoint: %s", request.URL.String(), resp.Status)
	}
	logger.Infof("[doHTTP] Successfully connected to %s, HTTP response status: %s", request.URL.String(), resp.Status)
	return nil
}

func doHTTPSGetCerts(request *http.Request, downloadCRLs bool) error {
	logger.Infof("[doHTTPSGetCerts] connecting to %s\n", request.URL.String())
	resp, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("failed to connect: %v", err)
	}
	defer resp.Body.Close()

	if !isAcceptableStatusCode(resp.StatusCode, acceptableStatusCodes) {
		return fmt.Errorf("HTTP response status from %s endpoint: %s", request.URL.String(), resp.Status)
	}
	logger.Infof("[doHTTPSGetCerts] Successfully connected to %s, HTTP response status: %s", request.URL.String(), resp.Status)

	// get TLS connection state
	logger.Debug("[doHTTPSGetCerts] getting TLS connection state")
	tlsState := resp.TLS
	if tlsState == nil {
		return fmt.Errorf("no TLS connection state available")
	}

	// get cert chain
	logger.Debug("[doHTTPSGetCerts] getting certificate chain")
	certs := tlsState.PeerCertificates
	logger.Infof("[doHTTPSGetCerts] Retrieved %d certificate(s).\n", len(certs))

	// log individual cert details
	for i, cert := range certs {
		logger.Infof("[doHTTPSGetCerts] Certificate %d, serial number: %x\n", i+1, cert.SerialNumber)
		logger.Infof("[doHTTPSGetCerts]   Subject: %s\n", cert.Subject)
		logger.Infof("[doHTTPSGetCerts]   Issuer:  %s\n", cert.Issuer)
		logger.Infof("[doHTTPSGetCerts]   Valid:   %s to %s\n", cert.NotBefore, cert.NotAfter)
		logger.Infof("[doHTTPSGetCerts]   For further details, check https://crt.sh/?serial=%x (non-Snowflake site)\n", cert.SerialNumber)

		// if cert has CRL endpoint, log them too
		if len(cert.CRLDistributionPoints) > 0 {
			logger.Infof("[doHTTPSGetCerts]   CRL Distribution Points:")
			for _, dp := range cert.CRLDistributionPoints {
				logger.Infof("[doHTTPSGetCerts]    - %s\n", dp)
				// only try to download the actual CRL if configured to do so
				if downloadCRLs {
					if err := fetchCRL(dp); err != nil {
						logger.Errorf("[doHTTPSGetCerts]      Failed to fetch CRL: %v\n", err)
					}
				}
			}
		} else {
			logger.Infof("[doHTTPSGetCerts]   CRL Distribution Points not available")
		}

		// dump the full PEM data too on DEBUG loglevel
		pemData := pem.EncodeToMemory(&pem.Block{
			Type:  "CERTIFICATE",
			Bytes: cert.Raw,
		})
		logger.Debugf("[doHTTPSGetCerts]   certificate PEM:\n%s\n", string(pemData))
	}
	return nil
}

func createRequest(uri string) (*http.Request, error) {
	logger.Infof("[createRequest] creating GET request to %s\n", uri)
	req, err := http.NewRequest("GET", uri, nil)
	if err != nil {
		return nil, err
	}
	return req, nil
}

func checkProxy(req *http.Request) {
	if diagnosticTransport == nil {
		logger.Errorf("[checkProxy] diagnosticTransport is nil")
		return
	}
	if diagnosticTransport.Proxy == nil {
		// no proxy configured, nothing to log
		return
	}
	p, err := diagnosticTransport.Proxy(req)
	if p != nil {
		logger.Infof("[checkProxy] PROXY detected in the connection: %v\n", p)
	}
	if err != nil {
		logger.Errorf("[checkProxy] problem determining PROXY: %v\n", err)
	}
}

func performDiagnosis(cfg *Config) {
	allowlistFile := cfg.ConnectionDiagnosticsAllowlistFile
	downloadCRLs := cfg.ConnectionDiagnosticsDownloadCRL

	logger.Info("[performDiagnosis] starting connectivity diagnosis based on allowlist file.")
	if downloadCRLs {
		logger.Info("[performDiagnosis] CRLs will be attempted to be downloaded during https tests.")
	}

	// diagnostic client - its transport is based on the Config
	// default: SnowflakeTransport with OCSP
	client = createDiagnosticClient(cfg)
	diagnosticTransport = client.Transport.(*http.Transport)

	allowlistContent, err := openAndReadAllowlistJSON(allowlistFile)
	if err != nil {
		logger.Errorf("[performDiagnosis] error opening allowlist file: %v\n", err)
		return
	}
	allowlist, err := parseAllowlistJSON(allowlistContent)
	if err != nil {
		logger.Errorf("[performDiagnosis] error parsing allowlist: %v\n", err)
		return
	}
	for _, entry := range allowlist.Entries {
		host := entry.Host
		port := entry.Port
		entryType := entry.Type
		logger.Infof("[performDiagnosis] DNS check - resolving %s hostname %s\n", entryType, host)
		resolveHostname(host)
		if port == 80 {
			logger.Infof("[performDiagnosis] HTTP check for %s %s\n", entryType, host)
			req, err := createRequest(fmt.Sprintf("http://%s", host))
			if err != nil {
				logger.Errorf("[performDiagnosis] error creating request: %v\n", err)
			}
			checkProxy(req)
			err = doHTTP(req)
			if err != nil {
				logger.Errorf("[performDiagnosis] error performing HTTP check: %v\n", err)
				continue
			}
		} else if port == 443 {
			logger.Infof("[performDiagnosis] HTTPS check - testing HTTPS connection to %s %s\n", entryType, host)
			req, err := createRequest(fmt.Sprintf("https://%s", host))
			if err != nil {
				logger.Errorf("[performDiagnosis] error creating request: %v\n", err)
			}
			checkProxy(req)
			err = doHTTPSGetCerts(req, downloadCRLs)
			if err != nil {
				logger.Errorf("[performDiagnosis] error performing HTTPS check: %v\n", err)
				continue
			}
		}
	}
}
