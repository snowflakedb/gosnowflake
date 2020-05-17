#!/bin/bash
#
# Test Golang driver
#
set -e
set -o pipefail

CI_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $CI_DIR/scripts/setup.sh
cd $CI_DIR/..
make test
