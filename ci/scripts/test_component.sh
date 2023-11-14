#!/bin/bash
#
# Build and Test Golang driver
#
set -e
set -o pipefail
CI_SCRIPTS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
TOPDIR=$(cd $CI_SCRIPTS_DIR/../.. && pwd)
eval $(jq -r '.testconnection | to_entries | map("export \(.key)=\(.value|tostring)")|.[]' $TOPDIR/parameters.json)
if [[ -n "$GITHUB_WORKFLOW" ]]; then
	export SNOWFLAKE_TEST_PRIVATE_KEY=$TOPDIR/rsa-2048-private-key.p8
fi

# TestCreateCredentialCache has a weird setup where if this
# file exists, the test fails. Instead of fixing the test
# and risking conflicts resolutions in the future, rm the 
# file before we run the tests.
rm -f ${HOME}/.cache/snowflake/temporary_credential.json

env | grep SNOWFLAKE | grep -v PASS | sort
cd $TOPDIR
go test -timeout 30m -race -coverprofile=coverage.txt -covermode=atomic -v . 
