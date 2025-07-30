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
	export SNOWFLAKE_TEST_PRIVATE_KEY_RSA_2048=$TOPDIR/rsa-2048-private-key.p8
fi
env | grep SNOWFLAKE | grep -v PASS | grep -v SECRET | sort
cd $TOPDIR
go install github.com/jstemmer/go-junit-report/v2@latest

if [[ "$HOME_EMPTY" == "yes" ]] ; then
  export GOCACHE=$HOME/go-build
  export GOMODCACHE=$HOME/go-modules
  export HOME=
fi
if [[ -n "$JENKINS_HOME" ]]; then
  export WORKSPACE=${WORKSPACE:-/mnt/workspace}
  go test $GO_TEST_PARAMS -timeout 120m -race -v . | /home/user/go/bin/go-junit-report -iocopy -out $WORKSPACE/junit-go.xml
else
  go test $GO_TEST_PARAMS -timeout 120m -race -coverprofile=coverage.txt -covermode=atomic -v . | tee test-output.txt
  cat test-output.txt | go-junit-report > test-report.junit.xml
fi