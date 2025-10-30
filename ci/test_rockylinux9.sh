#!/bin/bash -e
#
# Test GoSnowflake driver in Rocky Linux 9
# NOTES:
#   - Go version MUST be passed in as the first argument, e.g: "1.24.2"
#   - This is the script that test_rockylinux9_docker.sh runs inside of the docker container

if [[ -z "${1}" ]]; then
    echo "[ERROR] Go version is required as first argument (e.g., '1.24.2')"
    echo "Usage: $0 <go_version>"
    exit 1
fi

GO_VERSION="${1}"
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

# Extract short version for wrapper script
GO_VERSION_SHORT=$(echo ${GO_VERSION} | cut -d. -f1,2)

if ! command -v go${GO_VERSION_SHORT} &> /dev/null; then
    echo "[ERROR] Go ${GO_VERSION_SHORT} not found!"
    exit 1
fi

# Set GOROOT to short version directory (e.g., /usr/local/go1.24)  
export GOROOT="/usr/local/go${GO_VERSION_SHORT}"
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
