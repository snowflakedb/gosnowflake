// Copyright (c) 2021-2022 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"strings"
	"testing"
)

const (
	longToken = "_Y1ZNETTn5/qfUWj3Jedby7gipDzQs=UKyJH9DS=nFzzWnfZKGV+C7GopWC" + // pragma: allowlist secret
		"GD4LjOLLFZKOE26LXHDt3pTi4iI1qwKuSpf/FmClCMBSissVsU3Ei590FP0lPQQhcSG" + // pragma: allowlist secret
		"cDu69ZL_1X6e9h5z62t/iY7ZkII28n2qU=nrBJUgPRCIbtJQkVJXIuOHjX4G5yUEKjZ" + // pragma: allowlist secret
		"BAx4w6=_lqtt67bIA=o7D=oUSjfywsRFoloNIkBPXCwFTv+1RVUHgVA2g8A9Lw5XdJY" + // pragma: allowlist secret
		"uI8vhg=f0bKSq7AhQ2Bh"
	randomPassword = `Fh[+2J~AcqeqW%?`
)

func TestMaskToken(t *testing.T) {
	if text := maskSecrets("Token =" + longToken); strings.Compare(text, "Token =****") != 0 {
		t.Errorf("mask unsuccessful. expected: Token=****, got: %v", text)
	}
	if text := maskSecrets("idToken : " + longToken); strings.Compare(text, "idToken : ****") != 0 {
		t.Errorf("mask unsuccessful. expected: idToken : ****, got: %v", text)
	}
	if text := maskSecrets("sessionToken : " + longToken); strings.Compare(text, "sessionToken : ****") != 0 {
		t.Errorf("mask unsuccessful. expected: sessionToken : ****, got: %v", text)
	}
	if text := maskSecrets("masterToken : " + longToken); strings.Compare(text, "masterToken : ****") != 0 {
		t.Errorf("mask unsuccessful. expected: masterToken : ****, got: %v", text)
	}
	if text := maskSecrets("programmaticAccessToken : " + longToken); strings.Compare(text, "programmaticAccessToken : ****") != 0 {
		t.Errorf("mask unsuccessful. expected: programmaticAccessToken : ****, got: %v", text)
	}
	if text := maskSecrets("programmatic_access_token : " + longToken); strings.Compare(text, "programmatic_access_token : ****") != 0 {
		t.Errorf("mask unsuccessful. expected: programmatic_access_token : ****, got: %v", text)
	}

	falsePositiveToken := "2020-04-30 23:06:04,069 - MainThread auth.py:397" +
		" - write_temporary_credential() - DEBUG - no ID token is given when " +
		"try to store temporary credential"
	if text := maskSecrets(falsePositiveToken); strings.Compare(text, falsePositiveToken) != 0 {
		t.Errorf("mask token %v should not have changed value. got: %v", falsePositiveToken, text)
	}
}

func TestMaskPassword(t *testing.T) {
	if text := maskSecrets("password:" + randomPassword); strings.Compare(text, "password:****") != 0 {
		t.Errorf("mask unsuccessful. expected: password:****, got: %v", text)
	}
	if text := maskSecrets("PASSWORD:" + randomPassword); strings.Compare(text, "PASSWORD:****") != 0 {
		t.Errorf("mask unsuccessful. expected: PASSWORD:****, got: %v", text)
	}
	if text := maskSecrets("PaSsWoRd:" + randomPassword); strings.Compare(text, "PaSsWoRd:****") != 0 {
		t.Errorf("mask unsuccessful. expected: PaSsWoRd:****, got: %v", text)
	}
	if text := maskSecrets("password = " + randomPassword); strings.Compare(text, "password = ****") != 0 {
		t.Errorf("mask unsuccessful. expected: password = ****, got: %v", text)
	}
	if text := maskSecrets("pwd:" + randomPassword); strings.Compare(text, "pwd:****") != 0 {
		t.Errorf("mask unsuccessful. expected: pwd:****, got: %v", text)
	}
}

func TestTokenPassword(t *testing.T) {
	text := maskSecrets("token=" + longToken + " foo bar baz " + "password:" + randomPassword)
	expected := "token=**** foo bar baz password:****"
	if strings.Compare(text, expected) != 0 {
		t.Errorf("mask unsuccessful. expected: %v, got: %v", expected, text)
	}
	text = maskSecrets("PWD = " + randomPassword + " blah blah blah " + "TOKEN:" + longToken)
	expected = "PWD = **** blah blah blah TOKEN:****"
	if strings.Compare(text, expected) != 0 {
		t.Errorf("mask unsuccessful. expected: %v, got: %v", expected, text)
	}
}
