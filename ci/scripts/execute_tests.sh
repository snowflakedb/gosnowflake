#!/bin/bash -x
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
env | grep SNOWFLAKE | grep -v PASS | grep -v SECRET | sort
cd $TOPDIR
go install github.com/jstemmer/go-junit-report/v2@latest

if [[ "$TEST_GROUP" == "groupAH" ]]; then
  GO_TEST_PARAMS="-run Test[A-H] $GO_TEST_PARAMS"
elif [[ "$TEST_GROUP" == "groupIP" ]]; then
  GO_TEST_PARAMS="-run Test[I-P] $GO_TEST_PARAMS"
elif [[ "$TEST_GROUP" == "groupQZ" ]]; then
  GO_TEST_PARAMS="-run Test[Q-Z] $GO_TEST_PARAMS"
fi

if [[ -n "$JENKINS_HOME" ]]; then
  export WORKSPACE=${WORKSPACE:-/mnt/workspace}
  go test $GO_TEST_PARAMS -timeout 120m -race -v . | /home/user/go/bin/go-junit-report -iocopy -out $WORKSPACE/junit-go.xml
else
  go test $GO_TEST_PARAMS -timeout 120m -race -coverprofile=coverage.txt -covermode=atomic -v . | tee test-output.txt
  cat test-output.txt | go-junit-report > test-report.junit.xml
fi