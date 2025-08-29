package gosnowflake

import "regexp"

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
	awsKeyRegexp          = regexp.MustCompile(awsKeyPattern)
	awsTokenRegexp        = regexp.MustCompile(awsTokenPattern)
	sasTokenRegexp        = regexp.MustCompile(sasTokenPattern)
	privateKeyRegexp      = regexp.MustCompile(privateKeyPattern)
	privateKeyDataRegexp  = regexp.MustCompile(privateKeyDataPattern)
	privateKeyParamRegexp = regexp.MustCompile(privateKeyParamPattern)
	connectionTokenRegexp = regexp.MustCompile(connectionTokenPattern)
	passwordRegexp        = regexp.MustCompile(passwordPattern)
	dsnPasswordRegexp     = regexp.MustCompile(dsnPasswordPattern)
	clientSecretRegexp    = regexp.MustCompile(clientSecretPattern)
	jwtTokenRegexp        = regexp.MustCompile(jwtTokenPattern)
)

func maskConnectionToken(text string) string {
	return connectionTokenRegexp.ReplaceAllString(text, "$1${2}****")
}

func maskPassword(text string) string {
	return passwordRegexp.ReplaceAllString(text, "$1${2}****")
}

func maskDsnPassword(text string) string {
	return dsnPasswordRegexp.ReplaceAllString(text, "$1:****@")
}

func maskAwsKey(text string) string {
	return awsKeyRegexp.ReplaceAllString(text, "${1}****$2")
}

func maskAwsToken(text string) string {
	return awsTokenRegexp.ReplaceAllString(text, "${1}XXXX$2")
}

func maskSasToken(text string) string {
	return sasTokenRegexp.ReplaceAllString(text, "${1}****$2")
}
func maskPrivateKey(text string) string {
	return privateKeyRegexp.ReplaceAllString(text, "-----BEGIN PRIVATE KEY-----\\\\\\\\nXXXX\\\\\\\\n-----END PRIVATE KEY-----") // pragma: allowlist secret
}

func maskPrivateKeyData(text string) string {
	return privateKeyDataRegexp.ReplaceAllString(text, `"privateKeyData": "XXXX"`)
}

func maskClientSecret(text string) string {
	return clientSecretRegexp.ReplaceAllString(text, "$1${2}****")
}

func maskPrivateKeyParam(text string) string {
	return privateKeyParamRegexp.ReplaceAllString(text, "privateKey=****$2")
}

func maskJwtToken(text string) string {
	return jwtTokenRegexp.ReplaceAllString(text, "$1 ****")
}

func maskSecrets(text string) string {
	return maskConnectionToken(
		maskPassword(
			maskDsnPassword(
				maskPrivateKeyData(
					maskPrivateKeyParam(
						maskPrivateKey(
							maskAwsToken(
								maskSasToken(
									maskAwsKey(
										maskClientSecret(
											maskJwtToken(
												text)))))))))))
}
