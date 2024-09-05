#!/bin/bash -e
#
# Login the Internal Docker Registry
#
if [[ -z "$GITHUB_ACTIONS" ]]; then
    echo "[INFO] Login the internal Docker Resistry"
    NEXUS_USER=${USERNAME:-jenkins}
    if [[ -z "$NEXUS_PASSWORD" ]]; then
        echo "[ERROR] Set NEXUS_PASSWORD to your LDAP password to access the internal repository!"
        exit 1
    fi
    if ! docker login --username "$NEXUS_USER" --password "$NEXUS_PASSWORD" $INTERNAL_REPO; then
        echo "[ERROR] Failed to connect to the nexus server. Verify the environment variable NEXUS_PASSWORD is set correctly for NEXUS_USER: $NEXUS_USER"
        exit 1
    fi
else
    echo "[INFO] No login the internal Docker Registry"
fi
