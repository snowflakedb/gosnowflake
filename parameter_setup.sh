#!/bin/bash -ex
if [[ -n "$SNOWFLAKE_AZURE" ]]; then
    openssl aes-256-cbc -k "$super_azure_secret_password" -in parameters_azure.json.enc -out parameters.json -d
else
    openssl aes-256-cbc -k "$super_secret_password" -in parameters.json.enc -out parameters.json -d
fi
