REM Test Golang driver

setlocal EnableDelayedExpansion

start /b python ci\scripts\hang_webserver.py 12345

curl -O https://repo1.maven.org/maven2/org/wiremock/wiremock-standalone/3.11.0/wiremock-standalone-3.11.0.jar
START /B java -jar wiremock-standalone-3.11.0.jar --port %WIREMOCK_PORT% -https-port %WIREMOCK_HTTPS_PORT% --https-keystore ci/scripts/wiremock.p12 --keystore-type PKCS12 --keystore-password password

if "%CLOUD_PROVIDER%"=="AWS" (
    set PARAMETER_FILENAME=parameters_aws_golang.json.gpg
    set PRIVATE_KEY=rsa_key_golang_aws.p8.gpg
) else if "%CLOUD_PROVIDER%"=="AZURE" (
    set PARAMETER_FILENAME=parameters_azure_golang.json.gpg
    set PRIVATE_KEY=rsa_key_golang_azure.p8.gpg
) else if "%CLOUD_PROVIDER%"=="GCP" (
    set PARAMETER_FILENAME=parameters_gcp_golang.json.gpg
    set PRIVATE_KEY=rsa_key_golang_gcp.p8.gpg
)

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
    echo [ERROR] failed to decrypt the rsa-2048 private key
    exit /b 1
)

REM Create directory structure for golang private key
if not exist ".github\workflows\parameters\public" mkdir ".github\workflows\parameters\public"

gpg --quiet --batch --yes --decrypt --passphrase="%GOLANG_PRIVATE_KEY_SECRET%" --output .github\workflows\parameters\public\rsa_key_golang.p8 .github\workflows\parameters\public\%PRIVATE_KEY%
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] failed to decrypt the golang private key
    exit /b 1
)

echo @echo off>parameters.bat
jq -r ".testconnection | to_entries | map(\"set \(.key)=\(.value)\") | .[]" parameters.json >> parameters.bat
call parameters.bat
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] failed to set the test parameters
    exit /b 1
)

echo [INFO] Account:   %SNOWFLAKE_TEST_ACCOUNT%
echo [INFO] User   :   %SNOWFLAKE_TEST_USER%
echo [INFO] Database:  %SNOWFLAKE_TEST_DATABASE%
echo [INFO] Warehouse: %SNOWFLAKE_TEST_WAREHOUSE%
echo [INFO] Role:      %SNOWFLAKE_TEST_ROLE%

go install github.com/jstemmer/go-junit-report/v2@latest

REM Test based on SEQUENTIAL_TESTS setting
if "%SEQUENTIAL_TESTS%"=="true" (
    REM Test each package separately to avoid buffering - real-time output but slower
    echo [INFO] Running tests sequentially for real-time output

    REM Clear any existing output file
    if exist test-output.txt del test-output.txt

    REM Track if any test failed
    set TEST_FAILED=0

    REM Loop through each package and test separately
    for /f "usebackq delims=" %%p in (`go list ./...`) do (
        set PKG=%%p
        REM Convert full package path to relative path
        set PKG_PATH=!PKG:github.com/snowflakedb/gosnowflake/v2=!
        if "!PKG_PATH!"=="" (
            set PKG_PATH=.
        ) else (
            set PKG_PATH=.!PKG_PATH!
        )

        echo === Testing package: !PKG_PATH! ===
        echo === Testing package: !PKG_PATH! === >> test-output.txt

        REM Test package and append to output (no -race on Windows ARM)
        REM Replace / with _ for coverage filename
        set COV_FILE=!PKG_PATH:/=_!_coverage.txt
        go test %GO_TEST_PARAMS% --timeout 90m -coverprofile=!COV_FILE! -covermode=atomic -v !PKG_PATH! >> test-output.txt 2>&1

        REM Track failure but continue testing other packages
        if !ERRORLEVEL! NEQ 0 (
            echo [ERROR] Package !PKG_PATH! tests failed
            set TEST_FAILED=1
        )
    )

    REM Merge coverage files
    go install github.com/wadey/gocovmerge@latest
    gocovmerge *_coverage.txt > coverage.txt
    del *_coverage.txt

    REM Set exit code based on whether any test failed
    set TEST_EXIT=!TEST_FAILED!
) else (
    REM Test all packages with ./... - parallel, faster, but buffered
    echo [INFO] Running tests in parallel
    go test %GO_TEST_PARAMS% --timeout 90m -coverprofile=coverage.txt -covermode=atomic -v ./... > test-output.txt 2>&1
    set TEST_EXIT=%ERRORLEVEL%
)

REM Display the test output
type test-output.txt

REM Generate JUnit report from the saved output
type test-output.txt | go-junit-report > test-report.junit.xml

REM End local scope and exit with the test exit code
endlocal & exit /b %TEST_EXIT%
