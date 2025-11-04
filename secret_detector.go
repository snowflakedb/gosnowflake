package gosnowflake

import (
	"fmt"
	"regexp"
	"sync"
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

var (
	initRegexpOnce        sync.Once
	awsKeyRegexp          *regexp.Regexp
	awsTokenRegexp        *regexp.Regexp
	sasTokenRegexp        *regexp.Regexp
	privateKeyRegexp      *regexp.Regexp
	privateKeyDataRegexp  *regexp.Regexp
	privateKeyParamRegexp *regexp.Regexp
	connectionTokenRegexp *regexp.Regexp
	passwordRegexp        *regexp.Regexp
	dsnPasswordRegexp     *regexp.Regexp
	clientSecretRegexp    *regexp.Regexp
	jwtTokenRegexp        *regexp.Regexp
)

func registerRegexps() {
	awsKeyRegexp = regexp.MustCompile(awsKeyPattern)
	awsTokenRegexp = regexp.MustCompile(awsTokenPattern)
	sasTokenRegexp = regexp.MustCompile(sasTokenPattern)
	privateKeyRegexp = regexp.MustCompile(privateKeyPattern)
	privateKeyDataRegexp = regexp.MustCompile(privateKeyDataPattern)
	privateKeyParamRegexp = regexp.MustCompile(privateKeyParamPattern)
	connectionTokenRegexp = regexp.MustCompile(connectionTokenPattern)
	passwordRegexp = regexp.MustCompile(passwordPattern)
	dsnPasswordRegexp = regexp.MustCompile(dsnPasswordPattern)
	clientSecretRegexp = regexp.MustCompile(clientSecretPattern)
	jwtTokenRegexp = regexp.MustCompile(jwtTokenPattern)
}

type secretmasker string

func (s secretmasker) maskConnectionToken() secretmasker {
	return secretmasker(connectionTokenRegexp.ReplaceAllString(fmt.Sprint(s), "$1${2}****"))
}

func (s secretmasker) maskPassword() secretmasker {
	return secretmasker(passwordRegexp.ReplaceAllString(fmt.Sprint(s), "$1${2}****"))
}

func (s secretmasker) maskDsnPassword() secretmasker {
	return secretmasker(dsnPasswordRegexp.ReplaceAllString(fmt.Sprint(s), "$1:****@"))
}

func (s secretmasker) maskAwsKey() secretmasker {
	return secretmasker(awsKeyRegexp.ReplaceAllString(fmt.Sprint(s), "${1}****$2"))
}

func (s secretmasker) maskAwsToken() secretmasker {
	return secretmasker(awsTokenRegexp.ReplaceAllString(fmt.Sprint(s), "${1}XXXX$2"))
}

func (s secretmasker) maskSasToken() secretmasker {
	return secretmasker(sasTokenRegexp.ReplaceAllString(fmt.Sprint(s), "${1}****$2"))
}
func (s secretmasker) maskPrivateKey() secretmasker {
	return secretmasker(privateKeyRegexp.ReplaceAllString(fmt.Sprint(s), "-----BEGIN PRIVATE KEY-----\\\\\\\\nXXXX\\\\\\\\n-----END PRIVATE KEY-----")) // pragma: allowlist secret
}

func (s secretmasker) maskPrivateKeyData() secretmasker {
	return secretmasker(privateKeyDataRegexp.ReplaceAllString(fmt.Sprint(s), `"privateKeyData": "XXXX"`))
}

func (s secretmasker) maskClientSecret() secretmasker {
	return secretmasker(clientSecretRegexp.ReplaceAllString(fmt.Sprint(s), "$1${2}****"))
}

func (s secretmasker) maskPrivateKeyParam() secretmasker {
	return secretmasker(privateKeyParamRegexp.ReplaceAllString(fmt.Sprint(s), "privateKey=****$2"))
}

func (s secretmasker) maskJwtToken() secretmasker {
	return secretmasker(jwtTokenRegexp.ReplaceAllString(fmt.Sprint(s), "$1 ****"))
}

func (s secretmasker) String() string {
	return string(s)
}

func newSecretMasker(text string) secretmasker {
	return secretmasker(text)
}

func maskSecrets(text string) string {
	initRegexpOnce.Do(registerRegexps)

	s := newSecretMasker(text)

	return s.maskConnectionToken().
		maskPassword().
		maskDsnPassword().
		maskPrivateKeyData().
		maskPrivateKeyParam().
		maskPrivateKey().
		maskAwsToken().
		maskSasToken().
		maskAwsKey().
		maskClientSecret().
		maskJwtToken().
		String()
}
