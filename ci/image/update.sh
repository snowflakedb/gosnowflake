#!/usr/bin/env bash -e
#
# Build Docker images
#
set -o pipefail
THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $THIS_DIR/../_init.sh

source $THIS_DIR/../scripts/login_internal_docker.sh

for image in $(docker images --format "{{.ID}},{{.Repository}}:{{.Tag}}" | grep "nexus.int.snowflakecomputing.com" | grep "client-$DRIVER_NAME"); do
    target_id=$(echo $image | awk -F, '{print $1}')
    target_name=$(echo $image | awk -F, '{print $2}')
    for name in "${!TEST_IMAGE_NAMES[@]}"; do
        if [[ "$target_name" == "${TEST_IMAGE_NAMES[$name]}" ]]; then
            echo $name
            docker_hub_image_name=$(echo ${TEST_IMAGE_NAMES[$name]/$DOCKER_REGISTRY_NAME/snowflakedb})
            set -x
            docker tag $target_id $docker_hub_image_name
            set +x
            docker push "${TEST_IMAGE_NAMES[$name]}"
        fi
    done
done
