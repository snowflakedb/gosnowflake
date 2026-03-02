package config

// OCSPFailOpenMode is OCSP fail open mode. OCSPFailOpenTrue by default and may
// set to ocspModeFailClosed for fail closed mode
type OCSPFailOpenMode uint32

const (
	// OCSPFailOpenNotSet represents OCSP fail open mode is not set, which is the default value.
	OCSPFailOpenNotSet OCSPFailOpenMode = iota
	// OCSPFailOpenTrue represents OCSP fail open mode.
	OCSPFailOpenTrue
	// OCSPFailOpenFalse represents OCSP fail closed mode.
	OCSPFailOpenFalse
)

const (
	ocspModeFailOpen   = "FAIL_OPEN"
	ocspModeFailClosed = "FAIL_CLOSED"
	ocspModeDisabled   = "INSECURE"
)

// OcspMode returns the OCSP mode in string INSECURE, FAIL_OPEN, FAIL_CLOSED
func OcspMode(c *Config) string {
	if c.DisableOCSPChecks {
		return ocspModeDisabled
	} else if c.OCSPFailOpen == OCSPFailOpenNotSet || c.OCSPFailOpen == OCSPFailOpenTrue {
		// by default or set to true
		return ocspModeFailOpen
	}
	return ocspModeFailClosed
}
