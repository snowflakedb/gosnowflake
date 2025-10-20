package gosnowflake

import (
	"context"
	"net/http"
	"os"
	"testing"
	"time"
)

func TestEnvDetectors(t *testing.T) {
	t.Setenv("AWS_LAMBDA_TASK_ROOT", "/tmp")
	t.Setenv("GITHUB_ACTIONS", "true")
	t.Setenv("FUNCTIONS_WORKER_RUNTIME", "go")
	t.Setenv("FUNCTIONS_EXTENSION_VERSION", "~4")
	t.Setenv("AzureWebJobsStorage", "UseDevelopmentStorage=true")
	t.Setenv("K_SERVICE", "svc")
	t.Setenv("K_REVISION", "rev")
	t.Setenv("K_CONFIGURATION", "cfg")
	t.Setenv("CLOUD_RUN_JOB", "job")
	t.Setenv("CLOUD_RUN_EXECUTION", "exec")

	resetPlatformDetectionForTest()
	res := DetectedPlatforms(context.Background(), 200*time.Millisecond)

	// Validate that at least some env-based detections are present
	assertTrueF(t, contains(res, "is_aws_lambda"))
	assertTrueF(t, contains(res, "is_github_action"))
	assertTrueF(t, contains(res, "is_azure_function"))
	assertTrueF(t, contains(res, "is_gce_cloud_run_service"))
	assertTrueF(t, contains(res, "is_gce_cloud_run_job"))
}

func TestDisableFlag(t *testing.T) {
	t.Setenv(disablePlatformDetectionEnv, "true")
	resetPlatformDetectionForTest()
	res := DetectedPlatforms(context.Background(), 200*time.Millisecond)
	assertEmptyE(t, res)
}

func TestIsValidArnForWif(t *testing.T) {
	tests := []struct {
		arn   string
		valid bool
	}{
		{"", false},
		{"arn:aws:iam::123456789012:role/MyRole", true},
		{"arn:aws:sts::123456789012:assumed-role/MyRole/MySession", true},
		{"arn:aws:iam::123456789012:user/SomeUser", false},
		{"random", false},
	}
	for _, tt := range tests {
		assertEqualE(t, isValidArnForWif(tt.arn), tt.valid)
	}
}

func TestWiremockMetadataPaths(t *testing.T) {
	// Set base URLs to Wiremock
	wm := newWiremock()
	ec2MetadataBaseURL = wm.baseURL()
	azureMetadataBaseURL = wm.baseURL()
	gcpMetadataBaseURL = wm.baseURL() + "/computeMetadata/v1"
	gceMetadataRootURL = wm.baseURL() // root returns Metadata-Flavor

	// Register mappings
	wiremock.registerMappings(t,
		newWiremockMapping("metadata/ec2_imds_success.json"),
		newWiremockMapping("metadata/azure_imds_success.json"),
		newWiremockMapping("metadata/gcp_metadata_success.json"),
		newWiremockMapping("metadata/gcp_identity_success.json"),
	)

	resetPlatformDetectionForTest()
	res := DetectedPlatforms(context.Background(), 500*time.Millisecond)

	assertTrueF(t, contains(res, "is_ec2_instance"))
	assertTrueF(t, contains(res, "is_azure_vm"))
	assertTrueF(t, contains(res, "is_gce_vm"))
	assertTrueF(t, contains(res, "has_gcp_identity"))
}

func TestAzureFunctionsManagedIdentityShortcut(t *testing.T) {
	// Azure Functions env + IDENTITY_HEADER present should mark managed identity
	t.Setenv("FUNCTIONS_WORKER_RUNTIME", "go")
	t.Setenv("FUNCTIONS_EXTENSION_VERSION", "~4")
	t.Setenv("AzureWebJobsStorage", "UseDevelopmentStorage=true")
	t.Setenv("IDENTITY_HEADER", "header")
	resetPlatformDetectionForTest()
	res := DetectedPlatforms(context.Background(), 100*time.Millisecond)
	assertTrueF(t, contains(res, "has_azure_managed_identity"))
}

// Ensure we don’t accidentally import net/http/httptest for Wiremock case here
var _ = http.MethodGet

// Ensure we touch os so linter doesn’t remove imports in tests using env
var _ = os.Getenv
