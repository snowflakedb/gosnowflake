#!/bin/bash
#
# Test Golang driver
#
set -e
set -o pipefail

CI_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

curl -O https://repo1.maven.org/maven2/org/wiremock/wiremock-standalone/3.11.0/wiremock-standalone-3.11.0.jar
java -jar wiremock-standalone-3.11.0.jar --port $WIREMOCK_PORT &

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

  for name in "${!TARGET_TEST_IMAGES[@]}"; do
      echo "[INFO] Testing $DRIVER_NAME on $name"
      docker container run \
          --rm \
          --add-host=snowflake.reg.local:${IP_ADDR} \
          --add-host=s3testaccount.reg.local:${IP_ADDR} \
          -v $ROOT_DIR:/mnt/host \
          -v $WORKSPACE:/mnt/workspace \
          -e LOCAL_USER_ID=$(id -u ${USER}) \
          -e GIT_COMMIT \
          -e GIT_BRANCH \
          -e GIT_URL \
          -e AWS_ACCESS_KEY_ID \
          -e AWS_SECRET_ACCESS_KEY \
          -e GITHUB_ACTIONS \
          -e GITHUB_SHA \
          -e GITHUB_REF \
          -e RUNNER_TRACKING_ID \
          -e JOB_NAME \
          -e BUILD_NUMBER \
          -e JENKINS_HOME \
          ${TEST_IMAGE_NAMES[$name]} \
          /mnt/host/ci/container/test_component.sh
          echo "[INFO] Test Results: $WORKSPACE/junit.xml"
  done
else
  source $CI_DIR/scripts/setup_connection_parameters.sh
  cd $CI_DIR/..
  make test
fi
