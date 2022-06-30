#!/bin/bash
#
# Format, lint scan Golang driver
#
set -e
set -o pipefail

CI_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $CI_DIR/..
make fmt lint
