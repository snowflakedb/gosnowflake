#!/bin/bash -e

set -o pipefail

export AUTH_PARAMETER_FILE=./.github/workflows/parameters_aws_auth_tests.json
eval $(jq -r '.authtestparams | to_entries | map("export \(.key)=\(.value|tostring)")|.[]' $AUTH_PARAMETER_FILE)

go test -run TestExternalBrowser*
go test -run TestClientStoreCredentials
