package gosnowflake

import (
	"context"
	"fmt"
	"os"
	"slices"
	"sync"
	"testing"
	"time"
)

type platformDetectionTestCase struct {
	name             string
	envVars          map[string]string
	wiremockMappings []wiremockMapping
	expectedResult   []string
}

type envSnapshot map[string]string

func setupCleanPlatformEnv() func() {
	platformEnvVars := []string{
		"LAMBDA_TASK_ROOT",
		"GITHUB_ACTIONS",
		"FUNCTIONS_WORKER_RUNTIME",
		"FUNCTIONS_EXTENSION_VERSION",
		"AzureWebJobsStorage",
		"K_SERVICE",
		"K_REVISION",
		"K_CONFIGURATION",
		"CLOUD_RUN_JOB",
		"CLOUD_RUN_EXECUTION",
		"IDENTITY_HEADER",
		disablePlatformDetectionEnv,
	}

	snapshot := make(envSnapshot)
	for _, env := range platformEnvVars {
		snapshot[env] = os.Getenv(env)
	}

	for _, env := range platformEnvVars {
		os.Unsetenv(env)
	}

	return func() {
		for env, value := range snapshot {
			os.Setenv(env, value)
		}
	}
}

func setupWiremockMetadataEndpoints() func() {
	originalAzureURL := azureMetadataBaseURL
	originalGceRootURL := gceMetadataRootURL
	originalGcpBaseURL := gcpMetadataBaseURL

	wiremockURL := wiremock.baseURL()
	azureMetadataBaseURL = wiremockURL
	gceMetadataRootURL = wiremockURL
	gcpMetadataBaseURL = wiremockURL + "/computeMetadata/v1"
	os.Setenv("AWS_EC2_METADATA_SERVICE_ENDPOINT", wiremockURL)
	os.Setenv("AWS_ENDPOINT_URL_STS", wiremockURL)

	return func() {
		azureMetadataBaseURL = originalAzureURL
		gceMetadataRootURL = originalGceRootURL
		gcpMetadataBaseURL = originalGcpBaseURL
		os.Unsetenv("AWS_EC2_METADATA_SERVICE_ENDPOINT")
		os.Unsetenv("AWS_ENDPOINT_URL_STS")
	}
}

func TestPlatformDetectionCachingAndSyncOnce(t *testing.T) {
	cleanup := setupCleanPlatformEnv()
	defer cleanup()

	originalDone, originalCache := platformDetectionDone, detectedPlatformsCache
	initPlatformDetectionOnce, platformDetectionDone, detectedPlatformsCache = sync.Once{}, make(chan struct{}), nil
	defer func() { platformDetectionDone, detectedPlatformsCache = originalDone, originalCache }()

	os.Setenv("LAMBDA_TASK_ROOT", "/var/task")
	initPlatformDetection()
	platforms1 := getDetectedPlatforms()

	// Verify caching works and AWS Lambda detected
	assertDeepEqualE(t, platforms1, detectedPlatformsCache)
	assertTrueE(t, slices.Contains(platforms1, "is_aws_lambda"), "Should detect AWS Lambda")

	// Change environment and test sync.Once behavior
	cleanup()
	os.Setenv("GITHUB_ACTIONS", "true")
	initPlatformDetection()
	platforms2 := getDetectedPlatforms()

	assertDeepEqualE(t, platforms1, platforms2)
	assertTrueE(t, slices.Contains(platforms2, "is_aws_lambda"), "Should still show cached AWS Lambda result")
	assertFalseE(t, slices.Contains(platforms2, "is_github_action"), "Should NOT detect GitHub Actions due to caching")
}

func TestDetectPlatforms(t *testing.T) {
	testCases := []platformDetectionTestCase{
		{
			name: "returns disabled when SNOWFLAKE_DISABLE_PLATFORM_DETECTION is set",
			envVars: map[string]string{
				"SNOWFLAKE_DISABLE_PLATFORM_DETECTION": "true",
			},
			expectedResult: []string{"disabled"},
		},
		{
			name:           "returns empty when no platforms detected",
			expectedResult: []string{},
		},
		{
			name: "detects AWS Lambda",
			envVars: map[string]string{
				"LAMBDA_TASK_ROOT": "/var/task",
			},
			expectedResult: []string{"is_aws_lambda"},
		},
		{
			name: "detects GitHub Actions",
			envVars: map[string]string{
				"GITHUB_ACTIONS": "true",
			},
			expectedResult: []string{"is_github_action"},
		},
		{
			name: "detects Azure Function",
			envVars: map[string]string{
				"FUNCTIONS_WORKER_RUNTIME":    "node",
				"FUNCTIONS_EXTENSION_VERSION": "~4",
				"AzureWebJobsStorage":         "DefaultEndpointsProtocol=https;AccountName=test",
			},
			expectedResult: []string{"is_azure_function"},
		},
		{
			name: "detects GCE Cloud Run Service",
			envVars: map[string]string{
				"K_SERVICE":       "my-service",
				"K_REVISION":      "my-service-00001",
				"K_CONFIGURATION": "my-service",
			},
			expectedResult: []string{"is_gce_cloud_run_service"},
		},
		{
			name: "detects GCE Cloud Run Job",
			envVars: map[string]string{
				"CLOUD_RUN_JOB":       "my-job",
				"CLOUD_RUN_EXECUTION": "my-job-execution-1",
			},
			expectedResult: []string{"is_gce_cloud_run_job"},
		},
		{
			name: "detects EC2 instance",
			wiremockMappings: []wiremockMapping{
				newWiremockMapping("platform_detection/aws_ec2_instance_success.json"),
			},
			expectedResult: []string{"is_ec2_instance"},
		},
		{
			name: "detects AWS identity",
			wiremockMappings: []wiremockMapping{
				newWiremockMapping("platform_detection/aws_identity_success.json"),
			},
			expectedResult: []string{"has_aws_identity"},
		},
		{
			name: "detects Azure VM",
			wiremockMappings: []wiremockMapping{
				newWiremockMapping("platform_detection/azure_vm_success.json"),
			},
			expectedResult: []string{"is_azure_vm"},
		},
		{
			name: "detects Azure Managed Identity using IDENTITY_HEADER",
			envVars: map[string]string{
				"FUNCTIONS_WORKER_RUNTIME":    "node",
				"FUNCTIONS_EXTENSION_VERSION": "~4",
				"AzureWebJobsStorage":         "DefaultEndpointsProtocol=https;AccountName=test",
				"IDENTITY_HEADER":             "test-header",
			},
			expectedResult: []string{"is_azure_function", "has_azure_managed_identity"},
		},
		{
			name: "detects Azure Manage Identity using metadata service",
			wiremockMappings: []wiremockMapping{
				newWiremockMapping("platform_detection/azure_managed_identity_success.json"),
			},
			expectedResult: []string{"has_azure_managed_identity"},
		},
		{
			name: "detects GCE VM",
			wiremockMappings: []wiremockMapping{
				newWiremockMapping("platform_detection/gce_vm_success.json"),
			},
			expectedResult: []string{"is_gce_vm"},
		},
		{
			name: "detects GCP identity",
			wiremockMappings: []wiremockMapping{
				newWiremockMapping("platform_detection/gce_identity_success.json"),
			},
			expectedResult: []string{"has_gcp_identity"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cleanup := setupCleanPlatformEnv()
			defer cleanup()

			for key, value := range tc.envVars {
				os.Setenv(key, value)
			}

			wiremock.registerMappings(t, tc.wiremockMappings)
			wiremockCleanup := setupWiremockMetadataEndpoints()
			defer wiremockCleanup()

			platforms, detectionResults := detectPlatformsWithResults(context.Background(), 200*time.Millisecond)
			if hasPlatformDetectionTimeout(detectionResults) && !slices.Equal(platforms, tc.expectedResult) {
				t.Logf("platform detection timed out: %v, retrying once", detectionResults)
				platforms, detectionResults = detectPlatformsWithResults(context.Background(), 200*time.Millisecond)
			}
			assertDeepEqualE(t, platforms, tc.expectedResult, fmt.Sprintf("platforms=%v results=%v", platforms, detectionResults))
		})
	}
}

func TestDetectPlatformsTimeout(t *testing.T) {
	cleanup := setupCleanPlatformEnv()
	defer cleanup()

	wiremock.registerMappings(t, newWiremockMapping("platform_detection/timeout_response.json"))
	wiremockCleanup := setupWiremockMetadataEndpoints()
	defer wiremockCleanup()

	const detectionTimeout = 200 * time.Millisecond
	// Matches fixedDelayMilliseconds in platform_detection/timeout_response.json.
	const stubbedResponseDelay = time.Second

	start := time.Now()
	platforms, detectionResults := detectPlatformsWithResults(context.Background(), detectionTimeout)
	executionTime := time.Since(start)

	assertEqualE(t, len(platforms), 0, fmt.Sprintf("Expected empty platforms, got: %v", platforms))
	// Detectors must give up before the stubbed 1s delay. The lower bound is
	// not a contract: Windows timer slack can make a 200ms deadline fire early.
	assertTrueE(t, executionTime < stubbedResponseDelay,
		fmt.Sprintf("Expected execution time below %v, got: %v", stubbedResponseDelay, executionTime))
	for _, name := range []string{"is_ec2_instance", "has_aws_identity", "is_azure_vm", "has_azure_managed_identity", "is_gce_vm", "has_gcp_identity"} {
		assertEqualE(t, detectionResults[name].state, platformDetectionTimeout,
			fmt.Sprintf("%s should report a timeout, got %s", name, detectionResults[name]))
	}
}

func hasPlatformDetectionTimeout(detectionResults map[string]platformDetectionResult) bool {
	for _, detectionResult := range detectionResults {
		if detectionResult.state == platformDetectionTimeout {
			return true
		}
	}
	return false
}

func TestIsDetectionTimeout(t *testing.T) {
	assertFalseE(t, isDetectionTimeout(nil), "nil is not a timeout")
	assertTrueE(t, isDetectionTimeout(context.DeadlineExceeded), "context.DeadlineExceeded")
	assertTrueE(t, isDetectionTimeout(os.ErrDeadlineExceeded), "os.ErrDeadlineExceeded")
	assertTrueE(t, isDetectionTimeout(fmt.Errorf("wrapped: %w", context.DeadlineExceeded)), "wrapped context.DeadlineExceeded")
	assertTrueE(t, isDetectionTimeout(&timeoutNetError{}), "net.Error with Timeout() == true")
	assertFalseE(t, isDetectionTimeout(fmt.Errorf("request failed")), "plain error is not a timeout")
}

type timeoutNetError struct{}

func (e *timeoutNetError) Error() string   { return "i/o timeout" }
func (e *timeoutNetError) Timeout() bool   { return true }
func (e *timeoutNetError) Temporary() bool { return true }

func TestIsValidArnForWif(t *testing.T) {
	testCases := []struct {
		arn      string
		expected bool
	}{
		{"arn:aws:iam::123456789012:user/JohnDoe", true},
		{"arn:aws:sts::123456789012:assumed-role/RoleName/SessionName", true},
		{"invalid-arn-format", false},
		{"arn:aws:iam::account:root", false},
		{"arn:aws:iam::123456789012:group/Developers", false},
		{"arn:aws:iam::123456789012:role/S3Access", false},
		{"arn:aws:iam::123456789012:policy/UsersManageOwnCredentials", false},
		{"arn:aws:iam::123456789012:instance-profile/Webserver", false},
		{"arn:aws:sts::123456789012:federated-user/John", false},
		{"arn:aws:sts::account:self", false},
		{"arn:aws:iam::123456789012:mfa/JaneMFA", false},
		{"arn:aws:iam::123456789012:u2f/user/John/default", false},
		{"arn:aws:iam::123456789012:server-certificate/ProdServerCert", false},
		{"arn:aws:iam::123456789012:saml-provider/ADFSProvider", false},
		{"arn:aws:iam::123456789012:oidc-provider/GoogleProvider", false},
		{"arn:aws:iam::aws:contextProvider/IdentityCenter", false},
	}

	for _, tc := range testCases {
		t.Run(tc.arn, func(t *testing.T) {
			result := isValidArnForWif(tc.arn)
			assertEqualE(t, result, tc.expected, "ARN validation failed for: "+tc.arn)
		})
	}
}
