package gosnowflake

import (
	"regexp",
	"net/url"
)

var (
	matcher, _ = regexp.Compile("^http(s?)\\:\\/\\/[0-9a-zA-Z]([-.\\w]*[0-9a-zA-Z@:])*(:(0-9)*)*(\\/?)([a-zA-Z0-9\\-\\.\\?\\,\\&\\(\\)\\/\\\\\\+&%\\$#_=@]*)?$")
)

func isValidURL(targetURL string) bool {
	if !matcher.MatchString(targetURL) {
		logger.infof(" The provided SSO URL is invalid")
		return false
	}
	return true
}

func urlEncode(targetString string) string {
	return url.PathEscape(targetString)
}
