#!/bin/bash
#
# Format, lint and WhiteSource scan Golang driver
#
set -e
set -o pipefail

CI_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [[ -n "$JENKINS_HOME" ]]; then
  for name in "${!BUILD_IMAGE_NAMES[@]}"; do
    echo "[INFO] Building $DRIVER_NAME on $name"
    docker pull "${BUILD_IMAGE_NAMES[$name]}"
    docker run \
        -v $(cd $THIS_DIR/.. && pwd):/mnt/host \
        -v $WORKSPACE:/mnt/workspace \
        -e LOCAL_USER_ID=$(id -u $USER) \
        -e GIT_URL \
        -e GIT_BRANCH \
        -e GIT_COMMIT \
        -e AWS_ACCESS_KEY_ID \
        -e AWS_SECRET_ACCESS_KEY \
        -e GITHUB_ACTIONS \
        -e GITHUB_SHA \
        -e GITHUB_REF \
        -e GITHUB_EVENT_NAME \
        "${BUILD_IMAGE_NAMES[$name]}" \
        "/mnt/host/ci/scripts/build_component.sh"
  done
else
  cd $CI_DIR/..
  make fmt lint
fi
