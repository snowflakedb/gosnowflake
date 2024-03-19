#!/bin/bash -e

export PLATFORM=$(echo $(uname) | tr '[:upper:]' '[:lower:]')
# Use the internal Docker Registry
export INTERNAL_REPO=nexus.int.snowflakecomputing.com:8086
export DOCKER_REGISTRY_NAME=$INTERNAL_REPO/docker
export WORKSPACE=${WORKSPACE:-/tmp}

export DRIVER_NAME=go

# Build images
BUILD_IMAGE_VERSION=1

# Test Images
TEST_IMAGE_VERSION=1

declare -A BUILD_IMAGE_NAMES=(
    [$DRIVER_NAME-centos7-go1.21]=$DOCKER_REGISTRY_NAME/client-$DRIVER_NAME-centos7-go1.21-build:$BUILD_IMAGE_VERSION
)
export BUILD_IMAGE_NAMES

declare -A TEST_IMAGE_NAMES=(
    [$DRIVER_NAME-centos7-go1.21]=$DOCKER_REGISTRY_NAME/client-$DRIVER_NAME-centos7-go1.21-test:$TEST_IMAGE_VERSION
)
export TEST_IMAGE_NAMES
