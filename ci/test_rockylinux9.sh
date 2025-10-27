#!/bin/bash -e
#
# Test GoSnowflake driver in Rocky Linux 9
# NOTES:
#   - Go version to be tested should be passed in as the first argument, e.g: "1.24". If omitted 1.24 will be assumed.
#   - This is the script that test_rockylinux9_docker.sh runs inside of the docker container


GO_VERSION="${1:-1.24}"
THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
CONNECTOR_DIR="$( dirname "${THIS_DIR}")"

# Validate prerequisites
if [[ ! -f "${CONNECTOR_DIR}/parameters.json" ]]; then
    echo "[ERROR] parameters.json not found - connection parameters must be decrypted first"
    exit 1
fi

if [[ ! -f "${CONNECTOR_DIR}/.github/workflows/parameters/public/rsa_key_golang.p8" ]]; then
    echo "[ERROR] Private key not found - must be decrypted first"  
    exit 1
fi

# Setup Go environment
echo "[Info] Using Go ${GO_VERSION}"

if ! command -v go${GO_VERSION} &> /dev/null; then
    echo "[ERROR] Go ${GO_VERSION} not found!"
    exit 1
fi

# Make the specified Go version the default 'go' command for make test
case "$GO_VERSION" in
    "1.23") export GOROOT="/usr/local/go1.23.4" ;;
    "1.24") export GOROOT="/usr/local/go1.24.2" ;;
    "1.25") export GOROOT="/usr/local/go1.25.0" ;;
    *) echo "[ERROR] Unsupported Go version: $GO_VERSION"; exit 1 ;;
esac
export PATH="${GOROOT}/bin:$PATH"
export GOPATH="/home/user/go"
export PATH="$GOPATH/bin:$PATH"

echo "[Info] Go ${GO_VERSION} version: $(go version)"

cd $CONNECTOR_DIR

echo "[Info] Downloading Go modules"
go mod download

# Load connection parameters
eval $(jq -r '.testconnection | to_entries | map("export \(.key)=\(.value|tostring)")|.[]' ${CONNECTOR_DIR}/parameters.json)
export SNOWFLAKE_TEST_PRIVATE_KEY="${CONNECTOR_DIR}/.github/workflows/parameters/public/rsa_key_golang.p8"

# Start WireMock  
${CONNECTOR_DIR}/ci/scripts/run_wiremock.sh &

# Run tests using make test
cd ${CONNECTOR_DIR}
make test
