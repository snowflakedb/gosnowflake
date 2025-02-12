REM Test Golang driver

setlocal EnableDelayedExpansion

start /b python ci\scripts\hang_webserver.py 12345

curl -O https://repo1.maven.org/maven2/org/wiremock/wiremock-standalone/3.11.0/wiremock-standalone-3.11.0.jar
START /B java -jar wiremock-standalone-3.11.0.jar --port %WIREMOCK_PORT%

if "%CLOUD_PROVIDER%"=="AWS" set PARAMETER_FILENAME=parameters_aws_golang.json.gpg
if "%CLOUD_PROVIDER%"=="AZURE" set PARAMETER_FILENAME=parameters_azure_golang.json.gpg
if "%CLOUD_PROVIDER%"=="GCP" set PARAMETER_FILENAME=parameters_gcp_golang.json.gpg

if not defined PARAMETER_FILENAME (
    echo [ERROR] failed to detect CLOUD_PROVIDER: %CLOUD_PROVIDER%
    exit /b 1
)

gpg --quiet --batch --yes --decrypt --passphrase="%PARAMETERS_SECRET%" --output parameters.json .github/workflows/%PARAMETER_FILENAME%
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] failed to decrypt the test parameters 
    exit /b 1
)

gpg --quiet --batch --yes --decrypt --passphrase="%PARAMETERS_SECRET%" --output rsa-2048-private-key.p8 .github/workflows/rsa-2048-private-key.p8.gpg
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] failed to decrypt the test parameters 
    exit /b 1
)

echo @echo off>parameters.bat
jq -r ".testconnection | to_entries | map(\"set \(.key)=\(.value)\") | .[]" parameters.json >> parameters.bat
call parameters.bat
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] failed to set the test parameters
    exit /b 1
)
if defined GITHUB_WORKFLOW (
	set SNOWFLAKE_TEST_PRIVATE_KEY=%cd%/rsa-2048-private-key.p8
)

echo [INFO] Account:   %SNOWFLAKE_TEST_ACCOUNT%
echo [INFO] User   :   %SNOWFLAKE_TEST_USER%
echo [INFO] Database:  %SNOWFLAKE_TEST_DATABASE%
echo [INFO] Warehouse: %SNOWFLAKE_TEST_WAREHOUSE%
echo [INFO] Role:      %SNOWFLAKE_TEST_ROLE%

go test %GO_TEST_PARAMS% --timeout 90m --tags=sfdebug -race -coverprofile=coverage.txt -covermode=atomic -v .
