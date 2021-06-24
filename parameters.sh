#!/bin/sh
context=$1
if [ -z "$context" ]; then
    context="k8s.observe-eng.com"
fi
echo -n '{"testconnection":{'
for E in SNOWFLAKE_TEST_ACCOUNT SNOWFLAKE_TEST_DATABASE SNOWFLAKE_TEST_WAREHOUSE SNOWFLAKE_TEST_USER SNOWFLAKE_TEST_ROLE SNOWFLAKE_TEST_PASSWORD; do
	echo -n $COMMA\"$E\":\"`kubectl --context=$context -n eng get secret snowflake-sfdrivertest-credentials -o=jsonpath={.data.$E} | base64 --decode`\"
	COMMA=,
done
echo }}

