#!/usr/bin/env -e

export PLATFORM=$(echo $(uname) | tr '[:upper:]' '[:lower:]')
# Use the internal Docker Registry
export INTERNAL_REPO=nexus.int.snowflakecomputing.com:8086
export DOCKER_REGISTRY_NAME=$INTERNAL_REPO/docker
export WORKSPACE=${WORKSPACE:-/tmp}

export DRIVER_NAME=go

TEST_IMAGE_VERSION=1
declare -A TEST_IMAGE_NAMES=(
    [$DRIVER_NAME-chainguard-go1_24]=$DOCKER_REGISTRY_NAME/client-$DRIVER_NAME-chainguard-go1.24-test:$TEST_IMAGE_VERSION
)
export TEST_IMAGE_NAMES
