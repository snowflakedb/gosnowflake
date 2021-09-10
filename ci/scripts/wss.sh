#!/usr/bin/env bash
#
# Run whitesource for Golang
#
set -e
set -o pipefail
THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
[[ -z "$WHITESOURCE_API_KEY" ]] && echo "[WARNING] No WHITESOURCE_API_KEY is set. No WhiteSource scan will occurr." && exit 0

export PRODUCT_NAME=gosnowflake

export PROD_BRANCH=master
export PROD_GIT_REF=refs/heads/$PROD_BRANCH
export PROJECT_VERSION=$GITHUB_SHA

if [[ "$GITHUB_EVENT_NAME" == "pull_request" ]]; then
    echo "[INFO] Pull Request"
    IFS="/"
    read -ra GITHUB_REF_ELEMS <<<"$GITHUB_REF"
    IFS=" "
    export PROJECT_NAME=PR-${GITHUB_REF_ELEMS[2]}
elif [[ "$GITHUB_REF" == "$PROD_GIT_REF" ]]; then
    echo "[INFO] Production branch"
    export PROJECT_NAME=$PROD_BRANCH
else
    echo "[INFO] Non Production branch. Skipping wss..."
    env | grep GITHUB | sort
    export PROJECT_NAME=
fi

SCAN_DIRECTORIES=$( cd $THIS_DIR/../.. && pwd )

if [[ -n "$PROJECT_NAME" ]]; then
    rm -f wss-unified-agent.jar 
    curl -LO https://github.com/whitesource/unified-agent-distribution/releases/download/v21.7.2/wss-unified-agent.jar
fi
SCAN_CONFIG=wss-golang-agent.config
cat > $SCAN_CONFIG <<CONFIG
###############################################################
# WhiteSource Unified-Agent configuration file
###############################################################
# GO-MODULES SCAN MODE
###############################################################

apiKey=
#userKey is required if WhiteSource administrator has enabled "Enforce user level access" option
#userKey=
#requesterEmail=user@provider.com

projectName=
projectVersion=
projectToken=
#projectTag= key:value

productName=
productVersion=
productToken=

#projectPerFolder=true
#projectPerFolderIncludes=
#projectPerFolderExcludes=

#wss.connectionTimeoutMinutes=60
wss.url=https://saas.whitesourcesoftware.com/agent

############
# Policies #
############
checkPolicies=true
forceCheckAllDependencies=true
forceUpdate=true
forceUpdate.failBuildOnPolicyViolation=true
#updateInventory=false

###########
# General #
###########
#offline=false
#updateType=APPEND
#ignoreSourceFiles=true
#scanComment=
#failErrorLevel=ALL
#requireKnownSha1=false

#generateProjectDetailsJson=true
#generateScanReport=true
#scanReportTimeoutMinutes=10
#scanReportFilenameFormat=

#analyzeFrameworks=true
#analyzeFrameworksReference=

#updateEmptyProject=false

#log.files.level=
#log.files.maxFileSize=
#log.files.maxFilesCount=
#log.files.path=

########################################
# Package Manager Dependency resolvers #
########################################
resolveAllDependencies=false

go.resolveDependencies=true
go.collectDependenciesAtRuntime=true
go.dependencyManager=modules
go.ignoreSourceFiles=true
go.glide.ignoreTestPackages=true

###########################################################################################
# Includes/Excludes Glob patterns - Please use only one exclude line and one include line #
###########################################################################################
includes=**/*.go

#Exclude file extensions or specific directories by adding **/*.<extension> or **/<excluded_dir>/**
excludes=**/*sources.jar **/*javadoc.jar

case.sensitive.glob=false
followSymbolicLinks=true
CONFIG

set +e
if [[ "$PROJECT_NAME" == "$PROD_BRANCH" ]]; then
    # Prod branch
    echo "[INFO] Running wss.sh for ${PRODUCT_NAME}-${PROJECT_NAME}-${PROJECT_VERSION} under ${SCAN_DIRECTORIES}"
    java -jar wss-unified-agent.jar -apiKey ${WHITESOURCE_API_KEY} \
        -c ${SCAN_CONFIG} \
        -d ${SCAN_DIRECTORIES} \
        -product ${PRODUCT_NAME} \
        -project ${PROJECT_NAME} \
        -projectVersion ${PROJECT_VERSION} \
        -offline true
    ERR=$?
    if [[ "$ERR" != "254" && "$ERR" != "0" ]]; then
        echo "failed to run wss for $PRODUCT_VERSION_${PROJECT_VERSION} in ${PROJECT_VERSION}..."
        exit 1
    fi
    java -jar wss-unified-agent.jar -apiKey ${WHITESOURCE_API_KEY} \
       -c ${SCAN_CONFIG} \
       -product ${PRODUCT_NAME} \
       -project ${PROJECT_NAME} \
       -projectVersion baseline \
       -requestFiles whitesource/update-request.txt
    ERR=$?
    if [[ "$ERR" != "254" && "$ERR" != "0" ]]; then
        echo "failed to run wss for $PRODUCT_VERSION_${PROJECT_VERSION} in baseline"
        exit 1
    fi
    java -jar wss-unified-agent.jar -apiKey ${WHITESOURCE_API_KEY} \
        -c ${SCAN_CONFIG} \
        -product ${PRODUCT_NAME} \
        -project ${PROJECT_NAME} \
        -projectVersion ${PROJECT_VERSION} \
        -requestFiles whitesource/update-request.txt
    ERR=$?
    if [[ "$ERR" != "254" && "$ERR" != "0" ]]; then
        echo "failed to run wss for $PRODUCT_VERSION_${PROJECT_VERSION} in ${PROJECT_VERSION}"
        exit 1
    fi
elif [[ -n "$PROJECT_NAME" ]]; then
    # PR
    echo "[INFO] Running wss.sh for ${PRODUCT_NAME}-${PROJECT_NAME}-${PROJECT_VERSION} under ${SCAN_DIRECTORIES}"
    java -jar wss-unified-agent.jar -apiKey ${WHITESOURCE_API_KEY} \
        -c ${SCAN_CONFIG} \
        -d ${SCAN_DIRECTORIES} \
        -product ${PRODUCT_NAME} \
        -project ${PROJECT_NAME} \
        -projectVersion ${PROJECT_VERSION}
    ERR=$?
    if [[ "$ERR" != "254" && "$ERR" != "0" ]]; then
        echo "failed to run wss for $PRODUCT_VERSION_${PROJECT_VERSION} in ${PROJECT_VERSION}..."
        exit 1
    fi
fi
set -e
