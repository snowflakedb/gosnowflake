#!/bin/bash -e

set -o pipefail
export THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export WORKSPACE=${WORKSPACE:-/tmp}

CI_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [[ -n "$JENKINS_HOME" ]]; then
  source $CI_DIR/_init.sh
  source $CI_DIR/scripts/login_internal_docker.sh
fi
gpg --quiet --batch --yes --decrypt --passphrase="$PARAMETERS_SECRET" --output $THIS_DIR/../.github/workflows/parameters_aws_auth_tests.json "$THIS_DIR/../.github/workflows/parameters_aws_auth_tests.json.gpg"

docker run \
  -v $(cd $THIS_DIR/.. && pwd):/mnt/host \
  -v $WORKSPACE:/mnt/workspace \
  --rm \
  nexus.int.snowflakecomputing.com:8086/docker/snowdrivers-test-external-browser-golang:2 \
  "/mnt/host/ci/container/test_authentication.sh"
