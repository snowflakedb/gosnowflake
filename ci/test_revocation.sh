#!/bin/bash
#
# Test certificate revocation validation using the revocation-validation framework.
#

set -o pipefail

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
DRIVER_DIR="$( dirname "${THIS_DIR}")"
WORKSPACE=${WORKSPACE:-${DRIVER_DIR}}

echo "[Info] Starting revocation validation tests"
echo "[Info] Go driver path: $DRIVER_DIR"

set -e

# Clone revocation-validation framework
REVOCATION_DIR="/tmp/revocation-validation"
REVOCATION_BRANCH="${REVOCATION_BRANCH:-main}"

rm -rf "$REVOCATION_DIR"
mkdir -p "$REVOCATION_DIR"
wget -O - "https://artifactory.ci1.us-west-2.aws-dev.app.snowflake.com/artifactory/development-github-virtual/snowflake-eng/revocation-validation/archive/refs/heads/${REVOCATION_BRANCH}.tar.gz" | tar -xz --strip-components=1 -C "$REVOCATION_DIR"

cd "$REVOCATION_DIR"

# Point the framework at the local Go driver checkout
go mod edit -replace "github.com/snowflakedb/gosnowflake/v2=${DRIVER_DIR}"
go mod tidy
echo "[Info] Replaced gosnowflake module with local checkout: $DRIVER_DIR"

echo "[Info] Running tests with Go $(go version | grep -oE 'go[0-9]+\.[0-9]+')..."

go run . \
    --client snowflake \
    --output "${WORKSPACE}/revocation-results.json" \
    --output-html "${WORKSPACE}/revocation-report.html" \
    --log-level debug

EXIT_CODE=$?

if [ -f "${WORKSPACE}/revocation-results.json" ]; then
    echo "[Info] Results: ${WORKSPACE}/revocation-results.json"
fi
if [ -f "${WORKSPACE}/revocation-report.html" ]; then
    echo "[Info] Report: ${WORKSPACE}/revocation-report.html"
fi

exit $EXIT_CODE
