#!/usr/bin/env bash
#
# Run whitesource for Golang
#
set -e
set -o pipefail
THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
[[ -z "$WHITESOURCE_API_KEY" ]] && echo "[WARNING] No WHITESOURCE_API_KEY is set. No WhiteSource scan will occurr." && exit 0

export PRODUCT_NAME=GolangDriver
export PROJECT_NAME=GolangDriver

DATE=$(date +'%m-%d-%Y')

SCAN_DIRECTORIES=$THIS_DIR/..

rm -f wss-unified-agent.jar 
curl -LJO https://github.com/whitesource/unified-agent-distribution/releases/latest/download/wss-unified-agent.jar

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
forceCheckAllDependencies=false
forceUpdate=false
forceUpdate.failBuildOnPolicyViolation=false
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

echo "[INFO] Running wss.sh for ${PROJECT_NAME}-${PRODUCT_NAME} under ${SCAN_DIRECTORIES}"
java -jar wss-unified-agent.jar -apiKey ${WHITESOURCE_API_KEY} \
    -c ${SCAN_CONFIG} \
    -project ${PROJECT_NAME} \
    -product ${PRODUCT_NAME} \
    -d ${SCAN_DIRECTORIES} \
    -wss.url https://saas.whitesourcesoftware.com/agent \
    -offline true

if java -jar wss-unified-agent.jar -apiKey ${WHITESOURCE_API_KEY} \
   -c ${SCAN_CONFIG} \
   -project ${PROJECT_NAME} \
   -product ${PRODUCT_NAME} \
   -projectVersion baseline \
   -requestFiles whitesource/update-request.txt \
   -wss.url https://saas.whitesourcesoftware.com/agent ; then
    echo "checkPolicies=false" >> ${SCAN_CONFIG}
    java -jar wss-unified-agent.jar -apiKey ${WHITESOURCE_API_KEY} \
        -c ${SCAN_CONFIG} \
        -project ${PROJECT_NAME} \
        -product ${PRODUCT_NAME} \
        -projectVersion ${DATE} \
        -requestFiles whitesource/update-request.txt \
        -wss.url https://saas.whitesourcesoftware.com/agent
fi
