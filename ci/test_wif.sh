#!/bin/bash -e
#
# WIF e2e test orchestrator. Runs on the Jenkins node.
#
# Strategy: the test binary is prebuilt on the Jenkins node by
# ci/build_wif_artifacts.sh and this script ships it to each WIF VM
# and runs it there inside a Docker container. The container inherits
# the VM's cloud identity via IMDS, which is what the WIF flow attests
# against.
#
# Prerequisites (run before this script):
#   * ci/build_wif_artifacts.sh has populated ci/wif/artifacts/
#   * PARAMETERS_SECRET is exported
#
set -o pipefail

export THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export RSA_KEY_PATH_AWS_AZURE="$THIS_DIR/wif/parameters/rsa_wif_aws_azure"
export RSA_KEY_PATH_GCP="$THIS_DIR/wif/parameters/rsa_wif_gcp"
export PARAMETERS_FILE_PATH="$THIS_DIR/wif/parameters/parameters_wif.json"
export ARTIFACT_DIR="$THIS_DIR/wif/artifacts"

# while we scp test binary to the target vm and technically don't need 
# Go, it still needs to have a few prerequisites bundled. This  
# go-specific image has everything needed.
RUNTIME_IMAGE="snowflakedb/client-go-chainguard-go1.24-test:1"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")

run_wif_tests() {
  local provider="$1"
  local host="$2"
  local snowflake_host="$3"
  local rsa_key_path="$4"
  local snowflake_user="$5"
  local impersonation_path="$6"
  local snowflake_user_for_impersonation="$7"

  local remote_dir="wif_${provider}_${TIMESTAMP}"
  local ssh_opts=(-i "$rsa_key_path" -o IdentitiesOnly=yes -o StrictHostKeyChecking=no -p 443 -o UserKnownHostsFile=/dev/null)
  local scp_opts=(-P 443 -i "$rsa_key_path" -o IdentitiesOnly=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null)

  echo "==================================================================="
  echo "WIF tests: ${provider}  (host=${host}, remote_dir=${remote_dir})"
  echo "==================================================================="

  # NOTE: /home/user is the only dir we can write to (SNOW-2231498 to improve WORKDIR)
  ssh "${ssh_opts[@]}" "$host" "mkdir -p \"$remote_dir\"" || {
    echo "ERROR: failed to create remote dir '$remote_dir' on $host" >&2
    return 1
  }

  scp "${scp_opts[@]}" "$ARTIFACT_DIR/wif_tests.tar.gz" "$host:$remote_dir/wif_tests.tar.gz" || {
    echo "ERROR: failed to scp artifact to $host:$remote_dir/" >&2
    return 1
  }

  ssh "${ssh_opts[@]}" "$host" \
    env REMOTE_DIR="$remote_dir" \
        RUNTIME_IMAGE="$RUNTIME_IMAGE" \
        SNOWFLAKE_TEST_WIF_PROVIDER="$provider" \
        SNOWFLAKE_TEST_WIF_HOST="$snowflake_host" \
        SNOWFLAKE_TEST_WIF_ACCOUNT="$SNOWFLAKE_TEST_WIF_ACCOUNT" \
        SNOWFLAKE_TEST_WIF_USERNAME="$snowflake_user" \
        SNOWFLAKE_TEST_WIF_IMPERSONATION_PATH="$impersonation_path" \
        SNOWFLAKE_TEST_WIF_USERNAME_IMPERSONATION="$snowflake_user_for_impersonation" \
        SKIP_SETUP=true \
        bash <<'EOF'
    set -e
    set -o pipefail
    mkdir -p "$HOME/$REMOTE_DIR/tests"
    tar xzf "$HOME/$REMOTE_DIR/wif_tests.tar.gz" -C "$HOME/$REMOTE_DIR/tests"
    chmod +x "$HOME/$REMOTE_DIR/tests/wif_e2e.test"
    docker run \
      --rm \
      --cpus=1 \
      -m 2g \
      -v "$HOME/$REMOTE_DIR/tests":/home/user/tests \
      -w /home/user \
      -e SKIP_SETUP \
      -e SNOWFLAKE_TEST_WIF_PROVIDER \
      -e SNOWFLAKE_TEST_WIF_HOST \
      -e SNOWFLAKE_TEST_WIF_ACCOUNT \
      -e SNOWFLAKE_TEST_WIF_USERNAME \
      -e SNOWFLAKE_TEST_WIF_IMPERSONATION_PATH \
      -e SNOWFLAKE_TEST_WIF_USERNAME_IMPERSONATION \
      "$RUNTIME_IMAGE" \
      /home/user/tests/wif_e2e.test -test.v -test.run TestWorkloadIdentityAuthOnCloudVM
EOF
}

run_tests_and_set_result() {
  local provider="$1"
  local host="$2"
  local snowflake_host="$3"
  local rsa_key_path="$4"
  local snowflake_user="$5"
  local impersonation_path="$6"
  local snowflake_user_for_impersonation="$7"

  run_wif_tests "$provider" "$host" "$snowflake_host" "$rsa_key_path" \
    "$snowflake_user" "$impersonation_path" "$snowflake_user_for_impersonation"
  local status=$?

  if [[ $status -ne 0 ]]; then
    echo "$provider tests failed with exit status: $status"
    EXIT_STATUS=1
  else
    echo "$provider tests passed"
  fi

  ssh -i "$rsa_key_path" -o IdentitiesOnly=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -p 443 "$host" \
    "rm -rf \"wif_${provider}_${TIMESTAMP}\"" || true
}

setup_parameters() {
  source "$THIS_DIR/scripts/setup_gpg.sh"
  gpg --quiet --batch --yes --decrypt --passphrase="$PARAMETERS_SECRET" --output "$RSA_KEY_PATH_AWS_AZURE" "${RSA_KEY_PATH_AWS_AZURE}.gpg"
  gpg --quiet --batch --yes --decrypt --passphrase="$PARAMETERS_SECRET" --output "$RSA_KEY_PATH_GCP" "${RSA_KEY_PATH_GCP}.gpg"
  chmod 600 "$RSA_KEY_PATH_AWS_AZURE"
  chmod 600 "$RSA_KEY_PATH_GCP"
  gpg --quiet --batch --yes --decrypt --passphrase="$PARAMETERS_SECRET" --output "$PARAMETERS_FILE_PATH" "${PARAMETERS_FILE_PATH}.gpg"
  eval $(jq -r '.wif | to_entries | map("export \(.key)=\(.value|tostring)")|.[]' $PARAMETERS_FILE_PATH)
}

if [[ ! -f "$ARTIFACT_DIR/wif_tests.tar.gz" ]]; then
  echo "ERROR: $ARTIFACT_DIR/wif_tests.tar.gz not found. Run ci/build_wif_artifacts.sh first." >&2
  exit 1
fi

setup_parameters

EXIT_STATUS=0
set +e
run_tests_and_set_result "AZURE" "$HOST_AZURE" "$SNOWFLAKE_TEST_WIF_HOST_AZURE" "$RSA_KEY_PATH_AWS_AZURE" "$SNOWFLAKE_TEST_WIF_USERNAME_AZURE"
run_tests_and_set_result "AWS" "$HOST_AWS" "$SNOWFLAKE_TEST_WIF_HOST_AWS" "$RSA_KEY_PATH_AWS_AZURE" "$SNOWFLAKE_TEST_WIF_USERNAME_AWS" "$SNOWFLAKE_TEST_WIF_IMPERSONATION_PATH_AWS" "$SNOWFLAKE_TEST_WIF_USERNAME_AWS_IMPERSONATION"
run_tests_and_set_result "GCP" "$HOST_GCP" "$SNOWFLAKE_TEST_WIF_HOST_GCP" "$RSA_KEY_PATH_GCP" "$SNOWFLAKE_TEST_WIF_USERNAME_GCP" "$SNOWFLAKE_TEST_WIF_IMPERSONATION_PATH_GCP" "$SNOWFLAKE_TEST_WIF_USERNAME_GCP_IMPERSONATION"
run_tests_and_set_result "GCP+OIDC" "$HOST_GCP" "$SNOWFLAKE_TEST_WIF_HOST_GCP" "$RSA_KEY_PATH_GCP" "$SNOWFLAKE_TEST_WIF_USERNAME_GCP_OIDC"
set -e
echo "Exit status: $EXIT_STATUS"
exit $EXIT_STATUS
