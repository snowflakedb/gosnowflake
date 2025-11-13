package gosnowflake

import (
	"context"
	"crypto/rsa"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"
)

// TestIPv6Connectivity tests IPv6 connectivity with Snowflake using key-pair authentication.
// This test performs:
// 1. DNS resolution check for IPv4/IPv6
// 2. SELECT 1 query
// 3. SELECT pi() query
// 4. PUT operation (upload file to user stage)
// 5. GET operation (download file from user stage)
//
// Configuration:
// The test can be configured in two ways:
//
// Option 1: JSON Config File (Recommended - no environment variables needed)
//   - Create `ipv6_test_config.json` in the project root (see `ipv6_test_config.json.example`)
//   - Or set IPV6_TEST_CONFIG_FILE environment variable to point to your config file
//   - The config file should contain:
//     {
//     "account": "your-account",
//     "user": "your-user",
//     "host": "your-account.snowflakecomputing.com",
//     "port": "443",
//     "protocol": "https",
//     "database": "your-database",
//     "schema": "your-schema",
//     "warehouse": "your-warehouse",
//     "authenticator": "SNOWFLAKE_JWT",
//     "private_key_path": "./path/to/your/private_key.p8"
//     }
//
// Option 2: Environment Variables (Fallback)
//   - Set SNOWFLAKE_TEST_AUTHENTICATOR=SNOWFLAKE_JWT
//   - Set SNOWFLAKE_TEST_PRIVATE_KEY to path of your private key file
//   - Set other standard SNOWFLAKE_TEST_* environment variables (account, user, host, etc.)
func TestIPv6Connectivity(t *testing.T) {
	// Enable debug logging
	_ = GetLogger().SetLogLevel("debug")

	t.Log("=" + strings.Repeat("=", 60))
	t.Log("Starting IPv6 Connectivity Test")
	t.Log("=" + strings.Repeat("=", 60))

	// Setup key-pair authentication
	cfg := setupIPv6TestConfig(t)

	// Check DNS resolution
	hostname := cfg.Host
	if hostname == "" {
		// Try to get hostname from environment
		hostname, _ = GetFromEnv("SNOWFLAKE_TEST_HOST", false)
	}
	if hostname != "" {
		t.Log("=" + strings.Repeat("=", 60))
		t.Log("DNS Resolution Check")
		t.Log("=" + strings.Repeat("=", 60))
		checkDNSResolution(t, hostname)
		t.Log("=" + strings.Repeat("=", 60))
		t.Log("Note: If you get HTTP 403 Forbidden with IPv6, it means:")
		t.Log("  - Connection reached Snowflake server (network works)")
		t.Log("  - Server rejected IPv6 connection (endpoint may not support IPv6)")
		t.Log("  - This is a server-side policy, not a network issue")
		t.Log("=" + strings.Repeat("=", 60))
	}

	// Create DSN
	dsn, err := DSN(cfg)
	assertNilF(t, err, fmt.Sprintf("Failed to create DSN: %v", err))

	// Open database connection
	db, err := sql.Open("snowflake", dsn)
	assertNilF(t, err, fmt.Sprintf("Failed to open database: %v", err))
	defer func() {
		assertNilF(t, db.Close())
	}()

	// Log connection details
	t.Logf("Connected to Snowflake: %s", hostname)
	t.Logf("Account: %s, User: %s", cfg.Account, cfg.User)

	// Set up database and warehouse if specified
	ctx := context.Background()
	if cfg.Database != "" {
		t.Logf("Using database: %s", cfg.Database)
		_, err = db.ExecContext(ctx, fmt.Sprintf("USE DATABASE %s", cfg.Database))
		assertNilE(t, err, fmt.Sprintf("Failed to use database: %v", err))
	}
	if cfg.Warehouse != "" {
		t.Logf("Using warehouse: %s", cfg.Warehouse)
		_, err = db.ExecContext(ctx, fmt.Sprintf("USE WAREHOUSE %s", cfg.Warehouse))
		assertNilE(t, err, fmt.Sprintf("Failed to use warehouse: %v", err))
	}

	// Test 1: SELECT 1
	t.Log("Test 1: Executing SELECT 1")
	var result1 int
	err = db.QueryRowContext(ctx, "SELECT 1").Scan(&result1)
	assertNilF(t, err, "Failed to execute SELECT 1")
	assertEqualF(t, result1, 1, fmt.Sprintf("Expected 1, got %d", result1))
	t.Logf("SELECT 1 result: %d", result1)

	// Test 2: SELECT pi()
	t.Log("Test 2: Executing SELECT pi()")
	var piValue float64
	err = db.QueryRowContext(ctx, "SELECT pi()").Scan(&piValue)
	assertNilF(t, err, "Failed to execute SELECT pi()")
	expectedPi := math.Pi
	assertTrueF(t, math.Abs(piValue-expectedPi) < 0.000001, fmt.Sprintf("Expected pi (~3.14159), got %f", piValue))
	t.Logf("SELECT pi() result: %f", piValue)

	// Test 3 & 4: PUT and GET operations
	t.Log("Test 3 & 4: Starting PUT and GET operations")
	testPutGetOperations(t, db, ctx)

	t.Log("=" + strings.Repeat("=", 60))
	t.Log("IPv6 Connectivity Test Completed Successfully")
	t.Log("=" + strings.Repeat("=", 60))
}

// IPv6TestConfig represents the JSON configuration structure for IPv6 tests
type IPv6TestConfig struct {
	Account        string `json:"account"`
	User           string `json:"user"`
	Host           string `json:"host"`
	Port           string `json:"port"`
	Protocol       string `json:"protocol"`
	Database       string `json:"database"`
	Schema         string `json:"schema"`
	Warehouse      string `json:"warehouse"`
	Authenticator  string `json:"authenticator"`
	PrivateKeyPath string `json:"private_key_path"`
}

// loadIPv6ConfigFromFile loads configuration from a JSON file
func loadIPv6ConfigFromFile(filePath string) (*IPv6TestConfig, error) {
	bytes, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config IPv6TestConfig
	if err := json.Unmarshal(bytes, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	return &config, nil
}

// setupIPv6TestConfig creates a Config for IPv6 testing with key-pair authentication
// It first tries to load from a JSON config file (ipv6_test_config.json or path from IPV6_TEST_CONFIG_FILE),
// then falls back to environment variables if the file doesn't exist.
func setupIPv6TestConfig(t *testing.T) *Config {
	var config *IPv6TestConfig
	var err error

	// Try to load from config file first
	configFilePath := os.Getenv("IPV6_TEST_CONFIG_FILE")
	if configFilePath == "" {
		configFilePath = "ipv6_test_config.json"
	}

	config, err = loadIPv6ConfigFromFile(configFilePath)
	if err != nil {
		// File doesn't exist or can't be read, fall back to environment variables
		t.Logf("Config file not found or unreadable (%s), falling back to environment variables: %v", configFilePath, err)
		config = nil
	} else {
		t.Logf("Loaded configuration from file: %s", configFilePath)
	}

	var account, user, host, port, protocol, database, schema, warehouse, authenticator, privateKeyPath string

	if config != nil {
		// Use values from config file
		account = config.Account
		user = config.User
		host = config.Host
		port = config.Port
		protocol = config.Protocol
		database = config.Database
		schema = config.Schema
		warehouse = config.Warehouse
		authenticator = config.Authenticator
		privateKeyPath = config.PrivateKeyPath
	} else {
		// Fall back to environment variables
		authenticator = os.Getenv("SNOWFLAKE_TEST_AUTHENTICATOR")
		if authenticator == "" {
			authenticator = "SNOWFLAKE_JWT" // Default to JWT
		}

		var err error
		account, err = GetFromEnv("SNOWFLAKE_TEST_ACCOUNT", true)
		assertNilF(t, err, "SNOWFLAKE_TEST_ACCOUNT is required")

		user, err = GetFromEnv("SNOWFLAKE_TEST_USER", true)
		assertNilF(t, err, "SNOWFLAKE_TEST_USER is required")

		host, _ = GetFromEnv("SNOWFLAKE_TEST_HOST", false)
		port, _ = GetFromEnv("SNOWFLAKE_TEST_PORT", false)
		protocol, _ = GetFromEnv("SNOWFLAKE_TEST_PROTOCOL", false)
		database, _ = GetFromEnv("SNOWFLAKE_TEST_DATABASE", false)
		schema, _ = GetFromEnv("SNOWFLAKE_TEST_SCHEMA", false)
		warehouse, _ = GetFromEnv("SNOWFLAKE_TEST_WAREHOUSE", false)

		privateKeyPath, _ = GetFromEnv("SNOWFLAKE_TEST_PRIVATE_KEY", false)
		if privateKeyPath == "" {
			// Try alternative env var name
			privateKeyPath, _ = GetFromEnv("SNOWFLAKE_AUTH_TEST_PRIVATE_KEY_PATH", false)
		}
	}

	// Validate authenticator
	if authenticator != "SNOWFLAKE_JWT" {
		t.Skip("Skipping IPv6 test: authenticator must be set to SNOWFLAKE_JWT")
	}

	// Load private key
	var privateKey *rsa.PrivateKey
	if privateKeyPath != "" {
		// Load directly from file path
		bytes, err := os.ReadFile(privateKeyPath)
		assertNilF(t, err, fmt.Sprintf("Failed to read private key file: %v", err))

		key, err := ssh.ParseRawPrivateKey(bytes)
		assertNilF(t, err, fmt.Sprintf("Failed to parse private key: %v", err))

		rsaKey, ok := key.(*rsa.PrivateKey)
		assertTrueF(t, ok, "Private key is not an RSA key")
		privateKey = rsaKey
	} else {
		// Try to load from environment variable (as file path or env var name)
		envKeyPath, _ := GetFromEnv("SNOWFLAKE_TEST_PRIVATE_KEY", false)
		if envKeyPath != "" {
			// Check if it's a file path (contains / or \)
			if strings.Contains(envKeyPath, "/") || strings.Contains(envKeyPath, "\\") {
				// It's a file path, load directly
				bytes, err := os.ReadFile(envKeyPath)
				assertNilF(t, err, fmt.Sprintf("Failed to read private key file: %v", err))

				key, err := ssh.ParseRawPrivateKey(bytes)
				assertNilF(t, err, fmt.Sprintf("Failed to parse private key: %v", err))

				rsaKey, ok := key.(*rsa.PrivateKey)
				assertTrueF(t, ok, "Private key is not an RSA key")
				privateKey = rsaKey
			} else {
				// It's an environment variable name, use helper
				privateKey = loadRsaPrivateKeyForKeyPair(t, envKeyPath)
			}
		} else {
			t.Fatal("Private key path must be specified in config file (private_key_path) or environment variable (SNOWFLAKE_TEST_PRIVATE_KEY)")
		}
	}

	// Convert port string to int
	portInt := 443 // default port
	if port != "" {
		var err error
		portInt, err = strconv.Atoi(port)
		assertNilF(t, err, fmt.Sprintf("Invalid port value: %s", port))
	}

	// Create config
	cfg := &Config{
		Account:       account,
		User:          user,
		Host:          host,
		Port:          portInt,
		Protocol:      protocol,
		Database:      database,
		Schema:        schema,
		Warehouse:     warehouse,
		Authenticator: AuthTypeJwt,
		PrivateKey:    privateKey,
	}

	return cfg
}

// checkDNSResolution checks DNS resolution for IPv4 and IPv6 addresses
func checkDNSResolution(t *testing.T, hostname string) {
	t.Logf("Checking DNS resolution for: %s", hostname)

	ips, err := net.LookupIP(hostname)
	if err != nil {
		t.Logf("WARNING: Could not resolve addresses: %v", err)
		return
	}

	var ipv4Addrs []string
	var ipv6Addrs []string

	for _, ip := range ips {
		if ip.To4() != nil {
			ipv4Addrs = append(ipv4Addrs, ip.String())
		} else {
			ipv6Addrs = append(ipv6Addrs, ip.String())
		}
	}

	t.Logf("Summary: Found %d IPv4 address(es) and %d IPv6 address(es)", len(ipv4Addrs), len(ipv6Addrs))

	if len(ipv6Addrs) > 0 {
		// Show first 3 IPv6 addresses
		maxShow := 3
		if len(ipv6Addrs) < maxShow {
			maxShow = len(ipv6Addrs)
		}
		t.Logf("IPv6 addresses available: %s", strings.Join(ipv6Addrs[:maxShow], ", "))
	} else {
		t.Log("WARNING: No IPv6 addresses found in DNS resolution!")
	}

	if len(ipv4Addrs) > 0 {
		// Show first 3 IPv4 addresses
		maxShow := 3
		if len(ipv4Addrs) < maxShow {
			maxShow = len(ipv4Addrs)
		}
		t.Logf("IPv4 addresses available: %s", strings.Join(ipv4Addrs[:maxShow], ", "))
	} else {
		t.Log("WARNING: No IPv4 addresses found in DNS resolution!")
	}
}

// testPutGetOperations tests PUT and GET operations using user stage
func testPutGetOperations(t *testing.T, db *sql.DB, ctx context.Context) {
	// Create temporary directory
	tmpDir := t.TempDir()

	// Generate random test file
	testFileName := "test_ipv6_file.txt"
	testFilePath := filepath.Join(tmpDir, testFileName)
	testFileSize := 5 * 1024 // 5KB

	err := generateRandomFile(testFilePath, testFileSize)
	assertNilF(t, err, fmt.Sprintf("Failed to generate test file: %v", err))

	fileInfo, err := os.Stat(testFilePath)
	assertNilF(t, err, "Failed to stat test file")
	t.Logf("Generating test file: %s", testFilePath)
	t.Logf("Test file size: %d bytes", fileInfo.Size())
	assertTrueF(t, fileInfo.Size() > 0, "Test file should not be empty")

	// Use user stage (internal stage, no AWS credentials needed)
	stageName := "~" // User stage
	t.Logf("Using user stage: %s", stageName)

	// PUT file to stage
	putSQL := fmt.Sprintf("PUT 'file://%s' @%s", strings.ReplaceAll(testFilePath, "\\", "\\\\"), stageName)
	t.Logf("Executing PUT: %s", putSQL)

	rows, err := db.QueryContext(ctx, putSQL)
	assertNilF(t, err, fmt.Sprintf("Failed to execute PUT: %v", err))
	defer func() {
		assertNilF(t, rows.Close())
	}()

	// Parse PUT result
	var source, target, sourceSize, targetSize, sourceCompression, targetCompression, status, message string
	assertTrueF(t, rows.Next(), "PUT should return results")
	err = rows.Scan(&source, &target, &sourceSize, &targetSize, &sourceCompression, &targetCompression, &status, &message)
	assertNilF(t, err, "Failed to scan PUT result")

	t.Logf("PUT result: source=%s, target=%s, status=%s", source, target, status)
	assertTrueF(t, status == "UPLOADED" || status == "SKIPPED", fmt.Sprintf("File should be uploaded, got status: %s", status))

	// List files in stage
	t.Logf("Listing files in stage: %s", stageName)
	listSQL := fmt.Sprintf("LIST @%s", stageName)
	listRows, err := db.QueryContext(ctx, listSQL)
	assertNilF(t, err, fmt.Sprintf("Failed to list files: %v", err))
	defer func() {
		assertNilF(t, listRows.Close())
	}()

	var uploadedFile string
	found := false
	for listRows.Next() {
		var name, size, md5, lastModified string
		err = listRows.Scan(&name, &size, &md5, &lastModified)
		assertNilE(t, err, "Failed to scan list result")
		if strings.Contains(name, testFileName) {
			uploadedFile = name
			found = true
			break
		}
	}

	assertTrueF(t, found, "Uploaded file should be found in stage listing")
	t.Logf("Found uploaded file: %s", uploadedFile)

	// GET file from stage
	outputDir := filepath.Join(tmpDir, "download")
	err = os.MkdirAll(outputDir, 0755)
	assertNilF(t, err, "Failed to create output directory")

	// Extract just the filename from uploadedFile (LIST may return full path like ~/filename or just filename)
	// Remove any stage prefix and @ prefix to get just the filename
	filename := uploadedFile
	filename = strings.TrimPrefix(filename, "@")
	filename = strings.TrimPrefix(filename, "~/")
	filename = strings.TrimPrefix(filename, "~")
	filename = strings.TrimLeft(filename, "/")

	// Construct GET command using explicit stage path with filename
	// This ensures Snowflake parses it correctly as a stage file, not a database name
	outputDirEscaped := strings.ReplaceAll(outputDir, "\\", "/")
	getSQL := fmt.Sprintf("GET @~/%s 'file://%s/'", filename, outputDirEscaped)
	t.Logf("Executing GET: %s", getSQL)

	getRows, err := db.QueryContext(ctx, getSQL)
	assertNilF(t, err, fmt.Sprintf("Failed to execute GET: %v", err))
	defer func() {
		assertNilF(t, getRows.Close())
	}()

	// Parse GET result
	var getFile, getSize, getStatus, getMessage string
	if getRows.Next() {
		err = getRows.Scan(&getFile, &getSize, &getStatus, &getMessage)
		assertNilE(t, err, "Failed to scan GET result")
		t.Logf("GET result: file=%s, size=%s, status=%s", getFile, getSize, getStatus)
	}

	// Verify file was downloaded
	files, err := os.ReadDir(outputDir)
	assertNilF(t, err, "Failed to read output directory")

	var downloadedFiles []string
	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".gz") {
			downloadedFiles = append(downloadedFiles, file.Name())
		}
	}

	assertTrueF(t, len(downloadedFiles) > 0, "File should be downloaded")
	t.Logf("Downloaded files: %v", downloadedFiles)

	downloadedFilePath := filepath.Join(outputDir, downloadedFiles[0])
	downloadedFileInfo, err := os.Stat(downloadedFilePath)
	assertNilF(t, err, "Failed to stat downloaded file")
	t.Logf("Downloaded file size: %d bytes", downloadedFileInfo.Size())
	assertTrueF(t, downloadedFileInfo.Size() > 0, "Downloaded file should not be empty")

	// Clean up: remove file from stage
	t.Log("Cleaning up: removing file from stage")
	// Use the same filename extraction logic as GET command
	removeFilename := uploadedFile
	removeFilename = strings.TrimPrefix(removeFilename, "@")
	removeFilename = strings.TrimPrefix(removeFilename, "~/")
	removeFilename = strings.TrimPrefix(removeFilename, "~")
	removeFilename = strings.TrimLeft(removeFilename, "/")
	removeSQL := fmt.Sprintf("REMOVE @~/%s", removeFilename)
	t.Logf("Executing REMOVE: %s", removeSQL)
	_, err = db.ExecContext(ctx, removeSQL)
	assertNilE(t, err, fmt.Sprintf("Failed to remove file from stage: %v", err))
}

// generateRandomFile generates a random text file with the specified size
func generateRandomFile(filePath string, sizeBytes int) error {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789\n"

	// Use a local random source (rand.Seed is deprecated in Go 1.20+)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	content := make([]byte, sizeBytes)
	for i := range content {
		content[i] = charset[rng.Intn(len(charset))]
	}

	return os.WriteFile(filePath, content, 0644)
}
