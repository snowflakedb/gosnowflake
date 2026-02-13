#!/bin/bash -e
#
# Login the Internal Docker Registry
#
if [[ -z "$GITHUB_ACTIONS" ]]; then
    echo "[INFO] Login the internal Docker Registry"
    if ! docker login $INTERNAL_REPO; then
        echo "[ERROR] Failed to connect to the Artifactory server. Ensure 'sf artifact oci auth' has been run."
        exit 1
    fi
else
    echo "[INFO] No login the internal Docker Registry"
fi
