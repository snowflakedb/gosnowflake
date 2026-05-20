#!/bin/bash
#
# Format, lint and WhiteSource scan Golang driver
#
set -e
set -o pipefail

CI_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $CI_DIR/..
make fmt lint
# make lint runs go mod tidy which may update go.mod/go.sum; restore them so
# that the subsequent gofix.sh dirty-tree check does not fail.
git checkout go.mod go.sum
