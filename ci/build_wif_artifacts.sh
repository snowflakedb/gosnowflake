#!/bin/bash -e
#
# Builds the WIF e2e test binary on the Jenkins node. The resulting
# binary is staged as ci/wif/artifacts/wif_tests.tar.gz so the outer
# ci/test_wif.sh can scp it to the bare WIF cloud VMs and run it there.
#
set -o pipefail

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
REPO_ROOT="$( cd "$THIS_DIR/.." && pwd )"
ARTIFACT_DIR="$THIS_DIR/wif/artifacts"

cd "$REPO_ROOT"
rm -rf "$ARTIFACT_DIR"
mkdir -p "$ARTIFACT_DIR"

if ! command -v go >/dev/null 2>&1; then
  echo "ERROR: go toolchain not found on Jenkins node; cannot compile WIF tests." >&2
  exit 1
fi

echo "Using $(go version)"
echo "Compiling TestWorkloadIdentityAuthOnCloudVM..."
SKIP_SETUP=true go test -c -o "$ARTIFACT_DIR/wif_e2e.test" -run TestWorkloadIdentityAuthOnCloudVM

if [[ ! -f "$ARTIFACT_DIR/wif_e2e.test" ]]; then
  echo "ERROR: go test -c did not produce $ARTIFACT_DIR/wif_e2e.test" >&2
  exit 1
fi
chmod +x "$ARTIFACT_DIR/wif_e2e.test"

echo "Creating wif_tests.tar.gz..."
tar czf "$ARTIFACT_DIR/wif_tests.tar.gz" -C "$ARTIFACT_DIR" wif_e2e.test
rm -f "$ARTIFACT_DIR/wif_e2e.test"

echo "WIF artifacts staged in $ARTIFACT_DIR:"
ls -la "$ARTIFACT_DIR"
