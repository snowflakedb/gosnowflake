#!/bin/bash
#
# Build and Test Golang driver
#
set -e
set -o pipefail
CI_SCRIPTS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
TOPDIR=$(cd $CI_SCRIPTS_DIR/../.. && pwd)
eval $(jq -r '.testconnection | to_entries | map("export \(.key)=\(.value|tostring)")|.[]' $TOPDIR/parameters.json)
if [[ -n "$GITHUB_WORKFLOW" ]]; then
	export SNOWFLAKE_TEST_PRIVATE_KEY=$TOPDIR/rsa-2048-private-key.p8
fi
env | grep SNOWFLAKE | grep -v PASS | sort
cd $TOPDIR
go test -timeout 30m -tags=sfdebug -race $COVFLAGS -v . # -stderrthreshold=INFO -vmodule=*=2 or -log_dir=$TOPDIR -vmodule=connection=2,driver=2
