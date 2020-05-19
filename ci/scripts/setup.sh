#!/bin/bash -e
#
# Set connection parameters
#
CI_SCRIPTS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
if [[ "$CLOUD_PROVIDER" == "AZURE" ]]; then
    PARAMETER_FILE=parameters_azure.json.gpg
elif [[ "$CLOUD_PROVIDER" == "GCP" ]]; then
    PARAMETER_FILE=parameters_gcp.json.gpg
else
    PARAMETER_FILE=parameters_aws.json.gpg
fi
gpg --quiet --batch --yes --decrypt --passphrase="$PARAMETERS_SECRET" --output $CI_SCRIPTS_DIR/../../parameters.json $CI_SCRIPTS_DIR/../../.github/workflows/$PARAMETER_FILE
gpg --quiet --batch --yes --decrypt --passphrase="$PARAMETERS_SECRET" --output $CI_SCRIPTS_DIR/../../rsa-2048-private-key.p8 $CI_SCRIPTS_DIR/../../.github/workflows/rsa-2048-private-key.p8.gpg
