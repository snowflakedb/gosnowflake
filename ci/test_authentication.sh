#!/bin/bash -e

set -o pipefail

CI_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [[ -n "$JENKINS_HOME" ]]; then
  ROOT_DIR="$(cd "${CI_DIR}/.." && pwd)"
  export WORKSPACE=${WORKSPACE:-/tmp}

  source $CI_DIR/_init.sh
  source $CI_DIR/scripts/login_internal_docker.sh

  echo "Use /sbin/ip"
  IP_ADDR=$(/sbin/ip -4 addr show scope global dev eth0 | grep inet | awk '{print $2}' | cut -d / -f 1)

  declare -A TARGET_TEST_IMAGES
  if [[ -n "$TARGET_DOCKER_TEST_IMAGE" ]]; then
      echo "[INFO] TARGET_DOCKER_TEST_IMAGE: $TARGET_DOCKER_TEST_IMAGE"
      IMAGE_NAME=${TEST_IMAGE_NAMES[$TARGET_DOCKER_TEST_IMAGE]}
      if [[ -z "$IMAGE_NAME" ]]; then
          echo "[ERROR] The target platform $TARGET_DOCKER_TEST_IMAGE doesn't exist. Check $CI_DIR/_init.sh"
          exit 1
      fi
      TARGET_TEST_IMAGES=([$TARGET_DOCKER_TEST_IMAGE]=$IMAGE_NAME)
  else
      echo "[ERROR] Set TARGET_DOCKER_TEST_IMAGE to the docker image name to run the test"
      for name in "${!TEST_IMAGE_NAMES[@]}"; do
          echo "  " $name
      done
      exit 2
  fi
fi

gpg --quiet --batch --yes --decrypt --passphrase="$PARAMETERS_SECRET" --output $THIS_DIR/../.github/workflows/parameters_aws_auth_tests.json "$THIS_DIR/../.github/workflows/parameters_aws_auth_tests.json.gpg"

docker run \
  -v $(cd $THIS_DIR/.. && pwd):/mnt/host \
  -v $WORKSPACE:/mnt/workspace \
  --rm \
  nexus.int.snowflakecomputing.com:8086/docker/snowdrivers-test-external-browser-golang:2 \
  "/mnt/host/ci/container/test_authentication.sh"
