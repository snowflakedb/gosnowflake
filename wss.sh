#!/usr/bin/env bash
#
# Run whitesource for Golang
#
THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
[[ -z "$WHITESOURCE_API_KEY" ]] && echo "[WARNING] Set WHITESOURCE_API_KEY" && exit 1

export PRODUCT_NAME=GolangDriver
export PROJECT_NAME=GolangDriver

DATE=$(date +'%m-%d-%Y')

set -x
# Never ever fail for whitesource problems
set +e

SCAN_DIRECTORIES=$THIS_DIR

rm -f wss-unified-agent.jar 
curl -LJO https://github.com/whitesource/unified-agent-distribution/releases/latest/download/wss-unified-agent.jar

WSS_CONFIG=wss-golang-agent.config
cat > $WSS_CONFIG <<CONFIG
###############################################################
# WhiteSource Unified-Agent configuration file
###############################################################
# GO-DEP SCAN MODE
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
go.dependencyManager=dep
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
    -c $WSS_CONFIG \
    -project ${PROJECT_NAME} \
    -d ${SCAN_DIRECTORIES} \
    -product ${PRODUCT_NAME} \
    -projectVersion ${DATE}

# not ever
exit 0
