package gosnowflake

import (
	"net/url"
	"regexp"
)

var (
	matcher, _ = regexp.Compile(`^http(s?)\:\/\/[0-9a-zA-Z]([-.\w]*[0-9a-zA-Z@:])*(:(0-9)*)*(\/?)([a-zA-Z0-9\-\.\?\,\&\(\)\/\\\+&%\$#_=@]*)?$`)
)

func isValidURL(targetURL string) bool {
	if !matcher.MatchString(targetURL) {
		logger.Infof(" The provided URL is not a valid URL - " + targetURL)
		return false
	}
	return true
}

func urlEncode(targetString string) string {
	return url.PathEscape(targetString)
}
