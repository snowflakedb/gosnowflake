#!/usr/bin/env bash -e
#
# Build Docker images
#
set -o pipefail
THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $THIS_DIR/../_init.sh

for name in "${!TEST_IMAGE_NAMES[@]}"; do
    docker build \
        --platform linux/amd64 \
        --file $THIS_DIR/Dockerfile \
        --label snowflake \
        --label $DRIVER_NAME \
        --tag ${TEST_IMAGE_NAMES[$name]} .
done
