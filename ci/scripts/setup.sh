#!/bin/bash -e
if [[ -n "$SNOWFLAKE_AZURE" ]]; then
    gpg --quiet --batch --yes --decrypt --passphrase="$PARAMETERS_SECRET" --output parameters.json .github/workflows/parameters_azure.json.gpg
elif [[ -n "$SNOWFLAKE_GCP" ]]; then
    gpg --quiet --batch --yes --decrypt --passphrase="$PARAMETERS_SECRET" --output parameters.json .github/workflows/parameters_gcp.json.gpg
else
    gpg --quiet --batch --yes --decrypt --passphrase="$PARAMETERS_SECRET" --output parameters.json .github/workflows/parameters_aws.json.gpg
fi
# TODO: openssl aes-256-cbc -k "$super_secret_password" -in rsa-2048-private-key.p8.enc -out rsa-2048-private-key.p8 -d
