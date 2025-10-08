#!/bin/bash -e

set -o pipefail

export AUTH_PARAMETER_FILE=./.github/workflows/parameters_aws_auth_tests.json
eval $(jq -r '.authtestparams | to_entries | map("export \(.key)=\(.value|tostring)")|.[]' $AUTH_PARAMETER_FILE)

export SNOWFLAKE_AUTH_TEST_PRIVATE_KEY_PATH=./.github/workflows/rsa_keys/rsa_key.p8
export SNOWFLAKE_AUTH_TEST_INVALID_PRIVATE_KEY_PATH=./.github/workflows/rsa_keys/rsa_key_invalid.p8
export RUN_AUTH_TESTS=true

export AUTHENTICATION_TESTS_ENV="docker"

export RUN_AUTH_TESTS=true
export AUTHENTICATION_TESTS_ENV="docker"

go test -v -run TestExternalBrowser*
go test -v -run TestClientStoreCredentials
go test -v -run TestOkta*
go test -v -run TestOauth*
go test -v -run TestKeypair*
go test -v -run TestEndToEndPat*
go test -v -run TestMfaSuccessful