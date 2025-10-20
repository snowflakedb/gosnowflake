package gosnowflake

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

type platformDetectionState string

const (
	platformDetected         platformDetectionState = "detected"
	platformNotDetected      platformDetectionState = "not_detected"
	platformDetectionTimeout platformDetectionState = "timeout"
)

const disablePlatformDetectionEnv = "GOSNOWFLAKE_DISABLE_PLATFORM_DETECTION" // TODO: sync on prefix

var (
	ec2MetadataBaseURL   = defaultMetadataServiceBase
	azureMetadataBaseURL = defaultMetadataServiceBase
	gceMetadataRootURL   = "http://metadata.google.internal"
	gcpMetadataBaseURL   = "http://metadata.google.internal/computeMetadata/v1"
)

var cachedPlatforms atomic.Value

const defaultDetectorTimeout = 200 * time.Millisecond

func init() {
	go func() {
		platforms := detectPlatforms(context.Background(), defaultDetectorTimeout)
		cachedPlatforms.Store(platforms)
	}()
}

func DetectedPlatforms(ctx context.Context, timeout time.Duration) []string {
	if isPlatformDetectionDisabled() {
		return []string{}
	}
	if v := cachedPlatforms.Load(); v != nil {
		if s, ok := v.([]string); ok {
			return append([]string(nil), s...)
		}
	}
	if timeout <= 0 {
		timeout = defaultDetectorTimeout
	}
	platforms := detectPlatforms(ctx, timeout)
	cachedPlatforms.Store(platforms)
	return append([]string(nil), platforms...)
}

func resetPlatformDetectionForTest() {
	cachedPlatforms.Store(([]string)(nil))
}

func isPlatformDetectionDisabled() bool {
	val := strings.TrimSpace(strings.ToLower(os.Getenv(disablePlatformDetectionEnv)))
	switch val {
	case "1", "true", "yes", "y", "on":
		return true
	default:
		return false
	}
}

type detectorFunc struct {
	name string
	fn   func(ctx context.Context, deadline time.Time) platformDetectionState
}

func detectAllStates(ctx context.Context, timeout time.Duration) map[string]platformDetectionState {
	if timeout <= 0 {
		timeout = defaultDetectorTimeout
	}
	deadline := time.Now().Add(timeout)

	detectors := []detectorFunc{
		{name: "is_aws_lambda", fn: detectAwsLambdaEnv},
		{name: "is_azure_function", fn: detectAzureFunctionEnv},
		{name: "is_gce_cloud_run_service", fn: detectGceCloudRunServiceEnv},
		{name: "is_gce_cloud_run_job", fn: detectGceCloudRunJobEnv},
		{name: "is_github_action", fn: detectGithubActionsEnv},
		{name: "is_ec2_instance", fn: detectEc2Instance},
		{name: "has_aws_identity", fn: detectAwsIdentity},
		{name: "is_azure_vm", fn: detectAzureVm},
		{name: "has_azure_managed_identity", fn: detectAzureManagedIdentity},
		{name: "is_gce_vm", fn: detectGceVm},
		{name: "has_gcp_identity", fn: detectGcpIdentity},
	}

	result := make(map[string]platformDetectionState, len(detectors))
	var wg sync.WaitGroup
	wg.Add(len(detectors))
	mu := &sync.Mutex{}

	for _, d := range detectors {
		d := d
		go func() {
			defer wg.Done()
			state := d.fn(ctx, deadline)
			mu.Lock()
			result[d.name] = state
			mu.Unlock()
		}()
	}
	wg.Wait()
	return result
}

func detectPlatforms(ctx context.Context, timeout time.Duration) []string {
	if isPlatformDetectionDisabled() {
		return []string{}
	}
	states := detectAllStates(ctx, timeout)
	order := []string{
		"is_aws_lambda",
		"is_azure_function",
		"is_gce_cloud_run_service",
		"is_gce_cloud_run_job",
		"is_github_action",
		"is_ec2_instance",
		"has_aws_identity",
		"is_azure_vm",
		"has_azure_managed_identity",
		"is_gce_vm",
		"has_gcp_identity",
	}
	var out []string
	for _, name := range order {
		if states[name] == platformDetected {
			out = append(out, name)
		}
	}
	return out
}

func detectAwsLambdaEnv(_ context.Context, _ time.Time) platformDetectionState {
	if os.Getenv("AWS_LAMBDA_TASK_ROOT") != "" {
		return platformDetected
	}
	return platformNotDetected
}

func detectGithubActionsEnv(_ context.Context, _ time.Time) platformDetectionState {
	if os.Getenv("GITHUB_ACTIONS") != "" {
		return platformDetected
	}
	return platformNotDetected
}

func detectAzureFunctionEnv(_ context.Context, _ time.Time) platformDetectionState {
	if os.Getenv("FUNCTIONS_WORKER_RUNTIME") != "" &&
		os.Getenv("FUNCTIONS_EXTENSION_VERSION") != "" &&
		os.Getenv("AzureWebJobsStorage") != "" {
		return platformDetected
	}
	return platformNotDetected
}

func detectGceCloudRunServiceEnv(_ context.Context, _ time.Time) platformDetectionState {
	if os.Getenv("K_SERVICE") != "" && os.Getenv("K_REVISION") != "" && os.Getenv("K_CONFIGURATION") != "" {
		return platformDetected
	}
	return platformNotDetected
}

func detectGceCloudRunJobEnv(_ context.Context, _ time.Time) platformDetectionState {
	if os.Getenv("CLOUD_RUN_JOB") != "" && os.Getenv("CLOUD_RUN_EXECUTION") != "" {
		return platformDetected
	}
	return platformNotDetected
}

func remainingTimeout(deadline time.Time) time.Duration {
	d := time.Until(deadline)
	if d <= 0 {
		return 1 * time.Millisecond
	}
	return d
}

func metadataHTTPClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			Proxy:             nil,
			DisableKeepAlives: true,
		},
	}
}

func detectEc2Instance(ctx context.Context, deadline time.Time) platformDetectionState {
	timeout := remainingTimeout(deadline)
	client := metadataHTTPClient(timeout)

	// Try to get IMDSv2 token (optional)
	var token string
	tokenReq, err := http.NewRequestWithContext(ctx, http.MethodPut, ec2MetadataBaseURL+"/latest/api/token", nil)
	if err == nil {
		tokenReq.Header.Set("X-aws-ec2-metadata-token-ttl-seconds", "60")
		resp, err := client.Do(tokenReq)
		if err == nil {
			func() {
				if resp != nil && resp.Body != nil {
					defer resp.Body.Close()
				}
				if resp != nil && resp.StatusCode == http.StatusOK {
					b, _ := io.ReadAll(resp.Body)
					token = string(b)
				}
			}()
		}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ec2MetadataBaseURL+"/latest/dynamic/instance-identity/document", nil)
	if err != nil {
		logger.Debugf("is_ec2_instance: failed to create request: %v", err)
		return platformNotDetected
	}
	if token != "" {
		req.Header.Set("X-aws-ec2-metadata-token", token)
	}

	resp, err := client.Do(req)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || strings.Contains(strings.ToLower(err.Error()), "timeout") {
			return platformDetectionTimeout
		}
		logger.Debugf("is_ec2_instance: metadata request failed: %v", err)
		return platformNotDetected
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		if len(body) > 0 {
			return platformDetected
		}
	}
	return platformNotDetected
}

func detectGceVm(ctx context.Context, deadline time.Time) platformDetectionState {
	timeout := remainingTimeout(deadline)
	client := metadataHTTPClient(timeout)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, gceMetadataRootURL, nil)
	if err != nil {
		logger.Debugf("is_gce_vm: failed to create request: %v", err)
		return platformNotDetected
	}
	resp, err := client.Do(req)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || strings.Contains(strings.ToLower(err.Error()), "timeout") {
			return platformDetectionTimeout
		}
		logger.Debugf("is_gce_vm: metadata request failed: %v", err)
		return platformNotDetected
	}
	defer resp.Body.Close()
	if resp.Header.Get(gcpMetadataFlavorHeaderName) == gcpMetadataFlavor {
		return platformDetected
	}
	return platformNotDetected
}

func detectGcpIdentity(ctx context.Context, deadline time.Time) platformDetectionState {
	timeout := remainingTimeout(deadline)
	client := metadataHTTPClient(timeout)
	url := gcpMetadataBaseURL + "/instance/service-accounts/default/email"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		logger.Debugf("has_gcp_identity: failed to create request: %v", err)
		return platformNotDetected
	}
	req.Header.Set(gcpMetadataFlavorHeaderName, gcpMetadataFlavor)
	resp, err := client.Do(req)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || strings.Contains(strings.ToLower(err.Error()), "timeout") {
			return platformDetectionTimeout
		}
		logger.Debugf("has_gcp_identity: metadata request failed: %v", err)
		return platformNotDetected
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		return platformDetected
	}
	return platformNotDetected
}

func detectAzureVm(ctx context.Context, deadline time.Time) platformDetectionState {
	timeout := remainingTimeout(deadline)
	client := metadataHTTPClient(timeout)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, azureMetadataBaseURL+"/metadata/instance?api-version=2019-03-11", nil)
	if err != nil {
		logger.Debugf("is_azure_vm: failed to create request: %v", err)
		return platformNotDetected
	}
	req.Header.Set("Metadata", "true")
	resp, err := client.Do(req)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || strings.Contains(strings.ToLower(err.Error()), "timeout") {
			return platformDetectionTimeout
		}
		logger.Debugf("is_azure_vm: metadata request failed: %v", err)
		return platformNotDetected
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		return platformDetected
	}
	return platformNotDetected
}

func detectAzureManagedIdentity(ctx context.Context, deadline time.Time) platformDetectionState {
	// Shortcut for Functions with IDENTITY_HEADER present
	if detectAzureFunctionEnv(ctx, deadline) == platformDetected && os.Getenv("IDENTITY_HEADER") != "" {
		return platformDetected
	}
	timeout := remainingTimeout(deadline)
	client := metadataHTTPClient(timeout)
	values := url.Values{}
	values.Set("api-version", "2018-02-01")
	values.Set("resource", "api://fd3f753b-eed3-462c-b6a7-a4b5bb650aad")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, azureMetadataBaseURL+"/metadata/identity/oauth2/token?"+values.Encode(), nil)
	if err != nil {
		logger.Debugf("has_azure_managed_identity: failed to create request: %v", err)
		return platformNotDetected
	}
	req.Header.Set("Metadata", "True")
	resp, err := client.Do(req)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || strings.Contains(strings.ToLower(err.Error()), "timeout") {
			return platformDetectionTimeout
		}
		logger.Debugf("has_azure_managed_identity: metadata request failed: %v", err)
		return platformNotDetected
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		// Best-effort parse: presence of access_token is a strong signal
		body, _ := io.ReadAll(resp.Body)
		var payload map[string]any
		if err := json.Unmarshal(body, &payload); err == nil {
			if _, ok := payload["access_token"]; ok {
				return platformDetected
			}
		}
		// If parsing fails but status OK, still consider detected
		return platformDetected
	}
	return platformNotDetected
}

func detectAwsIdentity(ctx context.Context, deadline time.Time) platformDetectionState {
	// Allow corporate proxies: use default AWS SDK config
	// Apply deadline via context
	timeout := remainingTimeout(deadline)
	if timeout <= 0 {
		timeout = 1 * time.Millisecond
	}
	ctx2, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cfg, err := config.LoadDefaultConfig(ctx2, config.WithEC2IMDSRegion())
	if err != nil {
		logger.Debugf("has_aws_identity: failed to load AWS config: %v", err)
		if errors.Is(err, context.DeadlineExceeded) {
			return platformDetectionTimeout
		}
		return platformNotDetected
	}
	client := sts.NewFromConfig(cfg)
	out, err := client.GetCallerIdentity(ctx2, &sts.GetCallerIdentityInput{})
	if err != nil {
		logger.Debugf("has_aws_identity: GetCallerIdentity failed: %v", err)
		if errors.Is(err, context.DeadlineExceeded) {
			return platformDetectionTimeout
		}
		return platformNotDetected
	}
	if out == nil || out.Arn == nil || *out.Arn == "" {
		return platformNotDetected
	}
	if isValidArnForWif(*out.Arn) {
		return platformDetected
	}
	return platformNotDetected
}

// isValidArnForWif validates whether the provided ARN represents a role identity usable for WIF.
// We consider both IAM role ARNs and STS assumed-role ARNs as valid; user and other principals are not.
func isValidArnForWif(arn string) bool {
	// Examples:
	// - arn:aws:iam::123456789012:role/MyRole
	// - arn:aws:sts::123456789012:assumed-role/MyRole/MySession
	arn = strings.TrimSpace(arn)
	if arn == "" || !strings.HasPrefix(arn, "arn:") {
		return false
	}
	// Quick filters
	if strings.Contains(arn, ":role/") {
		return true
	}
	if strings.Contains(arn, ":assumed-role/") {
		return true
	}
	return false
}
