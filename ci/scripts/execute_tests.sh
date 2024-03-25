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
env | grep SNOWFLAKE | grep -v PASS | sort
cd $TOPDIR
if [[ -n "$JENKINS_HOME" ]]; then
  export WORKSPACE=${WORKSPACE:-/mnt/workspace}
  go install github.com/jstemmer/go-junit-report/v2@latest
  go test -timeout 50m -race -v . | go-junit-report -iocopy -out $WORKSPACE/junit-go.xml
else
  go test -timeout 50m -race -coverprofile=coverage.txt -covermode=atomic -v .
fi
