package gosnowflake

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/ec2/imds"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

type platformDetectionState string

const (
	platformDetected         platformDetectionState = "detected"
	platformNotDetected      platformDetectionState = "not_detected"
	platformDetectionTimeout platformDetectionState = "timeout"
)

const disablePlatformDetectionEnv = "SNOWFLAKE_DISABLE_PLATFORM_DETECTION"

var (
	azureMetadataBaseURL = "http://169.254.169.254"
	gceMetadataRootURL   = "http://metadata.google.internal"
	gcpMetadataBaseURL   = "http://metadata.google.internal/computeMetadata/v1"
)

var (
	detectedPlatformsCache   []string
	platformDetectionDone    chan struct{}
)

func init() {
	platformDetectionDone = make(chan struct{})
	go func() {
		time.Sleep(500 * time.Millisecond) // TODO: temporary for log to work
		initializePlatformDetection()
	}()
}

func initializePlatformDetection() {
	var platforms []string
	if os.Getenv(disablePlatformDetectionEnv) != "" {
		logger.Debugf("initializePlatformDetection: platform detection disabled via %s environment variable", disablePlatformDetectionEnv)
		// TODO: discussion on this value in progress
		platforms = []string{"disabled"}
	} else {
		platforms = detectPlatforms(context.Background(), 200 * time.Millisecond)
	}
	detectedPlatformsCache = platforms
	close(platformDetectionDone)
}

func GetDetectedPlatforms() []string {
	logger.Debugf("GetDetectedPlatforms: waiting for platform detection to complete")
	<-platformDetectionDone
	logger.Debugf("GetDetectedPlatforms: returning cached detected platforms: %v", detectedPlatformsCache)
	return detectedPlatformsCache
}

type detectorFunc struct {
	name string
	fn   func(ctx context.Context, timeout time.Duration) platformDetectionState
}

func detectPlatforms(ctx context.Context, timeout time.Duration) []string {
	logger.Debugf("detectPlatforms: starting with timeout: %v", timeout)

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

	detectionStates := make(map[string]platformDetectionState, len(detectors))
	var waitGroup sync.WaitGroup
	waitGroup.Add(len(detectors))

	for _, detector := range detectors {
		detector := detector // capture loop variable
		go func() {
			defer waitGroup.Done()
			detectionState := detector.fn(ctx, timeout)
			detectionStates[detector.name] = detectionState
			logger.Debugf("detectPlatforms: %s - %s", detector.name, detectionState)
		}()
	}
	waitGroup.Wait()

	var detectedPlatformNames []string
	for _, detector := range detectors {
		if detectionStates[detector.name] == platformDetected {
			detectedPlatformNames = append(detectedPlatformNames, detector.name)
		}
	}

	logger.Debugf("detectPlatforms: completed. Detected platforms: %v", detectedPlatformNames)
	return detectedPlatformNames
}

func detectAwsLambdaEnv(_ context.Context, _ time.Duration) platformDetectionState {
	if os.Getenv("AWS_LAMBDA_TASK_ROOT") != "" {
		return platformDetected
	}
	return platformNotDetected
}

func detectGithubActionsEnv(_ context.Context, _ time.Duration) platformDetectionState {
	if os.Getenv("GITHUB_ACTIONS") != "" {
		return platformDetected
	}
	return platformNotDetected
}

func detectAzureFunctionEnv(_ context.Context, _ time.Duration) platformDetectionState {
	if os.Getenv("FUNCTIONS_WORKER_RUNTIME") != "" &&
		os.Getenv("FUNCTIONS_EXTENSION_VERSION") != "" &&
		os.Getenv("AzureWebJobsStorage") != "" {
		return platformDetected
	}
	return platformNotDetected
}

func detectGceCloudRunServiceEnv(_ context.Context, _ time.Duration) platformDetectionState {
	if os.Getenv("K_SERVICE") != "" && os.Getenv("K_REVISION") != "" && os.Getenv("K_CONFIGURATION") != "" {
		return platformDetected
	}
	return platformNotDetected
}

func detectGceCloudRunJobEnv(_ context.Context, _ time.Duration) platformDetectionState {
	if os.Getenv("CLOUD_RUN_JOB") != "" && os.Getenv("CLOUD_RUN_EXECUTION") != "" {
		return platformDetected
	}
	return platformNotDetected
}

func detectEc2Instance(ctx context.Context, timeout time.Duration) platformDetectionState {
	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		logger.Debugf("is_ec2_instance: failed to load AWS config: %v", err)
		return platformNotDetected
	}

	client := imds.NewFromConfig(cfg)
	result, err := client.GetInstanceIdentityDocument(timeoutCtx, &imds.GetInstanceIdentityDocumentInput{})
	if err != nil {
		logger.Debugf("is_ec2_instance: IMDS request failed: %v", err)
		if errors.Is(err, context.DeadlineExceeded) {
			return platformDetectionTimeout
		}
		return platformNotDetected
	}

	if result != nil && result.InstanceIdentityDocument.InstanceID != "" {
		return platformDetected
	}

	return platformNotDetected
}

func platformDetectionHTTPClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			Proxy:             nil,
			DisableKeepAlives: true,
		},
	}
}

func detectGceVm(ctx context.Context, timeout time.Duration) platformDetectionState {
	client := platformDetectionHTTPClient(timeout)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, gceMetadataRootURL, nil)
	if err != nil {
		logger.Debugf("is_gce_vm: failed to create request: %v", err)
		return platformNotDetected
	}
	resp, err := client.Do(req)
	if err != nil {
		logger.Debugf("is_gce_vm: metadata request failed: %v", err)
		if errors.Is(err, context.DeadlineExceeded) {
			return platformDetectionTimeout
		}
		return platformNotDetected
	}
	defer resp.Body.Close()
	if resp.Header.Get(gcpMetadataFlavorHeaderName) == gcpMetadataFlavor {
		return platformDetected
	}
	return platformNotDetected
}

func detectGcpIdentity(ctx context.Context, timeout time.Duration) platformDetectionState {
	client := platformDetectionHTTPClient(timeout)
	url := gcpMetadataBaseURL + "/instance/service-accounts/default/email"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		logger.Debugf("has_gcp_identity: failed to create request: %v", err)
		return platformNotDetected
	}
	req.Header.Set(gcpMetadataFlavorHeaderName, gcpMetadataFlavor)
	resp, err := client.Do(req)
	if err != nil {
		logger.Debugf("has_gcp_identity: metadata request failed: %v", err)
		if errors.Is(err, context.DeadlineExceeded) {
			return platformDetectionTimeout
		}
		return platformNotDetected
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		return platformDetected
	}
	return platformNotDetected
}

func detectAzureVm(ctx context.Context, timeout time.Duration) platformDetectionState {
	client := platformDetectionHTTPClient(timeout)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, azureMetadataBaseURL+"/metadata/instance?api-version=2019-03-11", nil)
	if err != nil {
		logger.Debugf("is_azure_vm: failed to create request: %v", err)
		return platformNotDetected
	}
	req.Header.Set("Metadata", "true")
	resp, err := client.Do(req)
	if err != nil {
		logger.Debugf("is_azure_vm: metadata request failed: %v", err)
		if errors.Is(err, context.DeadlineExceeded) {
			return platformDetectionTimeout
		}
		return platformNotDetected
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		return platformDetected
	}
	return platformNotDetected
}

func detectAzureManagedIdentity(ctx context.Context, timeout time.Duration) platformDetectionState {
	if detectAzureFunctionEnv(ctx, timeout) == platformDetected && os.Getenv("IDENTITY_HEADER") != "" {
		logger.Debugf("has_azure_managed_identity: detected Azure Function with managed identity")
		return platformDetected
	}
	client := platformDetectionHTTPClient(timeout)
	values := url.Values{}
	values.Set("api-version", "2018-02-01")
	values.Set("resource", "https://management.azure.com")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, azureMetadataBaseURL+"/metadata/identity/oauth2/token?"+values.Encode(), nil)
	if err != nil {
		logger.Debugf("has_azure_managed_identity: failed to create request: %v", err)
		return platformNotDetected
	}
	req.Header.Set("Metadata", "True")
	resp, err := client.Do(req)
	if err != nil {
		logger.Debugf("has_azure_managed_identity: metadata request failed: %v", err)
		if errors.Is(err, context.DeadlineExceeded) {
			return platformDetectionTimeout
		}
		return platformNotDetected
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		return platformDetected
	}
	return platformNotDetected
}

func detectAwsIdentity(ctx context.Context, timeout time.Duration) platformDetectionState {
	cancelCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cfg, err := config.LoadDefaultConfig(cancelCtx, config.WithEC2IMDSRegion())
	if err != nil {
		logger.Debugf("has_aws_identity: failed to load AWS config: %v", err)
		if errors.Is(err, context.DeadlineExceeded) {
			return platformDetectionTimeout
		}
		return platformNotDetected
	}
	client := sts.NewFromConfig(cfg)
	out, err := client.GetCallerIdentity(cancelCtx, &sts.GetCallerIdentityInput{})
	if err != nil {
		logger.Debugf("has_aws_identity: GetCallerIdentity failed: %v", err)
		if errors.Is(err, context.DeadlineExceeded) {
			return platformDetectionTimeout
		}
		return platformNotDetected
	}
	if out == nil || out.Arn == nil || *out.Arn == "" {
		logger.Debugf("has_aws_identity: no valid ARN returned")
		return platformNotDetected
	}
	if isValidArnForWif(*out.Arn) {
		logger.Debugf("has_aws_identity: detected valid AWS identity with ARN: %s", *out.Arn)
		return platformDetected
	}
	logger.Debugf("has_aws_identity: ARN not valid for WIF: %s", *out.Arn)
	return platformNotDetected
}

func isValidArnForWif(arn string) bool {
	patterns := []string{
		`^arn:[^:]+:iam::[^:]+:user/.+$`,
		`^arn:[^:]+:sts::[^:]+:assumed-role/.+$`,
	}
	for _, pattern := range patterns {
		matched, err := regexp.MatchString(pattern, arn)
		if err == nil && matched {
			return true
		}
	}
	return false
}
