package logger

import (
	"regexp"
)

const (
	awsKeyPattern          = `(?i)(aws_key_id|aws_secret_key|access_key_id|secret_access_key)\s*=\s*'([^']+)'`
	awsTokenPattern        = `(?i)(accessToken|tempToken|keySecret)"\s*:\s*"([a-z0-9/+]{32,}={0,2})"`
	sasTokenPattern        = `(?i)(sig|signature|AWSAccessKeyId|password|passcode)=(?P<secret>[a-z0-9%/+]{16,})`
	privateKeyPattern      = `(?im)-----BEGIN PRIVATE KEY-----\\n([a-z0-9/+=\\n]{32,})\\n-----END PRIVATE KEY-----` // pragma: allowlist secret
	privateKeyDataPattern  = `(?i)"privateKeyData": "([a-z0-9/+=\\n]{10,})"`
	privateKeyParamPattern = `(?i)privateKey=([A-Za-z0-9/+=_%-]+)(&|$|\s)`
	connectionTokenPattern = `(?i)(token|assertion content)([\'\"\s:=]+)([a-z0-9=/_\-\+]{8,})`
	passwordPattern        = `(?i)(password|pwd)([\'\"\s:=]+)([a-z0-9!\"#\$%&\\\'\(\)\*\+\,-\./:;<=>\?\@\[\]\^_\{\|\}~]{8,})`
	dsnPasswordPattern     = `([^/:]+):([^@/:]{3,})@` // Matches user:password@host format in DSN strings
	clientSecretPattern    = `(?i)(clientSecret)([\'\"\s:= ]+)([a-z0-9!\"#\$%&\\\'\(\)\*\+\,-\./:;<=>\?\@\[\]\^_\{\|\}~]+)`
	jwtTokenPattern        = `(?i)(jwt|bearer)[\s:=]*([a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+)` // pragma: allowlist secret
)

type patternAndReplace struct {
	regex       *regexp.Regexp
	replacement string
}

var secretDetectorPatterns = []patternAndReplace{
	{regexp.MustCompile(awsKeyPattern), "$1=****$2"},
	{regexp.MustCompile(awsTokenPattern), "${1}XXXX$2"},
	{regexp.MustCompile(sasTokenPattern), "${1}****$2"},
	{regexp.MustCompile(privateKeyPattern), "-----BEGIN PRIVATE KEY-----\\\\\\\\nXXXX\\\\\\\\n-----END PRIVATE KEY-----"}, // pragma: allowlist secret
	{regexp.MustCompile(privateKeyDataPattern), `"privateKeyData": "XXXX"`},
	{regexp.MustCompile(privateKeyParamPattern), "privateKey=****$2"},
	{regexp.MustCompile(connectionTokenPattern), "$1${2}****"},
	{regexp.MustCompile(passwordPattern), "$1${2}****"},
	{regexp.MustCompile(dsnPasswordPattern), "$1:****@"},
	{regexp.MustCompile(clientSecretPattern), "$1${2}****"},
	{regexp.MustCompile(jwtTokenPattern), "$1 ****"},
}

// MaskSecrets masks secrets in text (exported for use by main package and secret masking logger)
func MaskSecrets(text string) (masked string) {
	res := text
	for _, pattern := range secretDetectorPatterns {
		res = pattern.regex.ReplaceAllString(res, pattern.replacement)
	}
	return res
}
