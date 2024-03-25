#!/bin/bash

set -e
set -o pipefail

CI_SCRIPTS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
TOPDIR=$(cd $CI_SCRIPTS_DIR/../.. && pwd)

cd $TOPDIR
cp parameters.json.local parameters.json
make test
