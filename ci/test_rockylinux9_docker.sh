#!/bin/bash -e
# Test GoSnowflake driver in Rocky Linux 9 Docker
# NOTES:
#   - By default this script runs Go 1.24 tests
#   - To test specific version(s) pass in versions like: `./test_rockylinux9_docker.sh "1.23"`

set -o pipefail

# In case this is ran from dev-vm
GO_ENV=${1:-1.24}

# Set constants
THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
CONNECTOR_DIR="$( dirname "${THIS_DIR}")"
WORKSPACE=${WORKSPACE:-${CONNECTOR_DIR}}

# TODO: Uncomment when set_base_image.sh is created for Go
# source $THIS_DIR/set_base_image.sh

cd $THIS_DIR/docker/rockylinux9

CONTAINER_NAME=test_gosnowflake_rockylinux9

echo "[Info] Building docker image for Rocky Linux 9"
BASE_IMAGE=${BASE_IMAGE_ROCKYLINUX9:-rockylinux:9}
GOSU_URL=https://github.com/tianon/gosu/releases/download/1.14/gosu-amd64

docker build --pull -t ${CONTAINER_NAME}:1.0 --build-arg BASE_IMAGE=$BASE_IMAGE --build-arg GOSU_URL="$GOSU_URL" . -f Dockerfile

# Use setup_connection_parameters.sh like native jobs (outside container)
if [[ "$GITHUB_ACTIONS" == "true" ]]; then
    source ${CONNECTOR_DIR}/ci/scripts/setup_connection_parameters.sh
fi

user_id=$(id -u ${USER})
docker run --network=host \
    -e TERM=vt102 \
    -e LOCAL_USER_ID=${user_id} \
    -e JENKINS_HOME \
    -e GITHUB_ACTIONS \
    -e CLOUD_PROVIDER \
    -e GO_TEST_PARAMS \
    -e WIREMOCK_PORT \
    -e WIREMOCK_HTTPS_PORT \
    --mount type=bind,source="${CONNECTOR_DIR}",target=/home/user/gosnowflake \
    ${CONTAINER_NAME}:1.0 \
    /home/user/gosnowflake/ci/test_rockylinux9.sh ${GO_ENV}
