#!/bin/bash
#
# Build and Test Golang driver
#
set -e
set -o pipefail
CI_SCRIPTS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
TOPDIR=$(cd $CI_SCRIPTS_DIR/../.. && pwd)
eval $(jq -r '.testconnection | to_entries | map("export \(.key)=\(.value|tostring)")|.[]' $TOPDIR/parameters.json)
env | grep SNOWFLAKE | grep -v PASS | grep -v SECRET | sort
cd $TOPDIR
go install github.com/jstemmer/go-junit-report/v2@latest

if [[ "$HOME_EMPTY" == "yes" ]] ; then
  export GOCACHE=$HOME/go-build
  export GOMODCACHE=$HOME/go-modules
  export HOME=
fi

if [[ "$SEQUENTIAL_TESTS" == "true" ]] ; then
  # Test each package separately to avoid buffering (slower but real-time output)
  PACKAGES=$(go list ./...)

  if [[ -n "$JENKINS_HOME" ]]; then
    export WORKSPACE=${WORKSPACE:-/mnt/workspace}
    (
      for pkg in $PACKAGES; do
        # Convert full package path to relative path
        pkg_path=$(echo $pkg | sed "s|^github.com/snowflakedb/gosnowflake/v2||" | sed "s|^/||")
        if [[ -z "$pkg_path" ]]; then
          pkg_path="."
        else
          pkg_path="./$pkg_path"
        fi
        echo "=== Testing package: $pkg_path ===" >&2
        go test $GO_TEST_PARAMS -timeout 90m -race -v "$pkg_path"
      done
    ) | /home/user/go/bin/go-junit-report -iocopy -out $WORKSPACE/junit-go.xml
  else
    set +e
    FAILED=0
    (
      for pkg in $PACKAGES; do
        pkg_path=$(echo $pkg | sed "s|^github.com/snowflakedb/gosnowflake/v2||" | sed "s|^/||")
        if [[ -z "$pkg_path" ]]; then
          pkg_path="."
        else
          pkg_path="./$pkg_path"
        fi
        echo "=== Testing package: $pkg_path ===" >&2
        # Note: -coverprofile only works with single package, use -coverpkg for multiple
        go test $GO_TEST_PARAMS -timeout 90m -race -coverprofile="${pkg_path//\//_}_coverage.txt" -covermode=atomic -v "$pkg_path"
        if [[ $? -ne 0 ]]; then
          FAILED=1
          echo "[ERROR] Package $pkg_path tests failed" >&2
        fi
      done
      # Merge coverage files
      go install github.com/wadey/gocovmerge@latest
      gocovmerge *_coverage.txt > coverage.txt
      rm -f *_coverage.txt
      exit $FAILED
    ) | tee test-output.txt
    TEST_EXIT_CODE=${PIPESTATUS[0]}
    cat test-output.txt | go-junit-report > test-report.junit.xml
    exit $TEST_EXIT_CODE
  fi
else
  # Test all packages with ./... (parallel, faster, but buffered per package)
  if [[ -n "$JENKINS_HOME" ]]; then
    export WORKSPACE=${WORKSPACE:-/mnt/workspace}
    go test $GO_TEST_PARAMS -timeout 90m -race -v ./... | /home/user/go/bin/go-junit-report -iocopy -out $WORKSPACE/junit-go.xml
  else
    set +e
    go test $GO_TEST_PARAMS -timeout 90m -race -coverprofile=coverage.txt -covermode=atomic -v ./... | tee test-output.txt
    TEST_EXIT_CODE=$?
    cat test-output.txt | go-junit-report > test-report.junit.xml
    exit $TEST_EXIT_CODE
  fi
fi