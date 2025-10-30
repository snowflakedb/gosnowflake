package gosnowflake

import (
	"context"
	"fmt"
	"os"
	"slices"
	"testing"
	"time"
)

type platformDetectionTestCase struct {
	name             string
	envVars          map[string]string
	wiremockMappings []wiremockMapping
	expectedResult   []string
}

func clearPlatformEnvVars() {
	envVars := []string{
		"AWS_LAMBDA_TASK_ROOT",
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
	for _, env := range envVars {
		os.Unsetenv(env)
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

func TestGetDetectedPlatformsReturnsCachedResult(t *testing.T) {
	platforms := getDetectedPlatforms()
	assertTrueF(t, slices.Equal(platforms, detectedPlatformsCache),
		"getDetectedPlatforms should return the cached result")
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
			name: "returns empty when no platforms detected",
			expectedResult: []string{},
		},
		{
			name: "detects AWS Lambda",
			envVars: map[string]string{
				"AWS_LAMBDA_TASK_ROOT": "/var/task",
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
			clearPlatformEnvVars()
			for key, value := range tc.envVars {
				os.Setenv(key, value)
			}
			defer clearPlatformEnvVars()

			wiremock.registerMappings(t, tc.wiremockMappings...)
			wiremockCleanup := setupWiremockMetadataEndpoints()
			defer wiremockCleanup()

			platforms := detectPlatforms(context.Background(), 200*time.Millisecond)

			assertTrueF(t, slices.Equal(platforms, tc.expectedResult),
				fmt.Sprintf("Platform detection mismatch. Expected: %v, Got: %v", tc.expectedResult, platforms))
		})
	}
}

func TestDetectPlatformsTimeout(t *testing.T) {
	clearPlatformEnvVars()
	wiremock.registerMappings(t, newWiremockMapping("platform_detection/timeout_response.json"))
	wiremockCleanup := setupWiremockMetadataEndpoints()
	defer wiremockCleanup()

	start := time.Now()
	platforms := detectPlatforms(context.Background(), 200*time.Millisecond)
	executionTime := time.Since(start)

	assertTrueF(t, len(platforms) == 0, fmt.Sprintf("Expected empty platforms, got: %v", platforms))
	assertTrueF(t, executionTime >= 200*time.Millisecond && executionTime < 250*time.Millisecond,
		fmt.Sprintf("Expected execution time around 200ms, got: %v", executionTime))
}
