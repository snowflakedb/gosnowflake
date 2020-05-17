#!/bin/bash -e
CI_SCRIPTS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
if [[ "$CLOUD_PROVIDER" == "AZURE" ]]; then
    PARAMETER_FILE=parameters_azure.json.gpg
elif [[ "$CLOUD_PROVIDER" == "GCP" ]]; then
    PARAMETER_FILE=parameters_gcp.json.gpg
else
    PARAMETER_FILE=parameters_aws.json.gpg
fi
gpg --quiet --batch --yes --decrypt --passphrase="$PARAMETERS_SECRET" --output $CI_SCRIPTS_DIR/../../parameters.json $CI_SCRIPTS_DIR/../../.github/workflows/$PARAMETER_FILE
# TODO: openssl aes-256-cbc -k "$super_secret_password" -in rsa-2048-private-key.p8.enc -out rsa-2048-private-key.p8 -d
