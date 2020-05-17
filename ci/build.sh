#!/bin/bash
#
# Build and Test Golang driver
#
set -e
set -o pipefail

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $THIS_DIR/scripts/setup.sh
cd $THIS_DIR/..
make fmt lint wss test
