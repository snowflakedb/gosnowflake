#!/bin/bash -e
# Test GoSnowflake driver in Rocky Linux 9 Docker
# NOTES:
#   - Go version MUST be specified as first argument
#   - Usage: ./test_rockylinux9_docker.sh "1.24.2"

set -o pipefail

if [[ -z "${1}" ]]; then
    echo "[ERROR] Go version is required as first argument (e.g., '1.24.2')"
    echo "Usage: $0 <go_version>"
    exit 1
fi

GO_ENV=${1}

# Set constants
THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
CONNECTOR_DIR="$( dirname "${THIS_DIR}")"
WORKSPACE=${WORKSPACE:-${CONNECTOR_DIR}}

# TODO: Uncomment when set_base_image.sh is created for Go
# source $THIS_DIR/set_base_image.sh

cd $THIS_DIR/docker/rockylinux9

CONTAINER_NAME=test_gosnowflake_rockylinux9

echo "[Info] Building docker image for Rocky Linux 9 with Go ${GO_ENV}"

# Get current user/group IDs to match host permissions
USER_ID=$(id -u)
GROUP_ID=$(id -g)

docker build --pull -t ${CONTAINER_NAME}:1.0 \
    --build-arg BASE_IMAGE=rockylinux:9 \
    --build-arg GO_VERSION=$GO_ENV \
    --build-arg USER_ID=$USER_ID \
    --build-arg GROUP_ID=$GROUP_ID \
    . -f Dockerfile

# Use setup_connection_parameters.sh like native jobs (outside container)
if [[ "$GITHUB_ACTIONS" == "true" ]]; then
    source ${CONNECTOR_DIR}/ci/scripts/setup_connection_parameters.sh
fi

docker run --network=host \
    -e TERM=vt102 \
    -e JENKINS_HOME \
    -e GITHUB_ACTIONS \
    -e CLOUD_PROVIDER \
    -e GO_TEST_PARAMS \
    -e WIREMOCK_PORT \
    -e WIREMOCK_HTTPS_PORT \
    --mount type=bind,source="${CONNECTOR_DIR}",target=/home/user/gosnowflake \
    ${CONTAINER_NAME}:1.0 \
    ci/test_rockylinux9.sh ${GO_ENV}
