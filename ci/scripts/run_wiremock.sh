#!/usr/bin/env bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

cd $SCRIPT_DIR

if [[ "$1" == "--ecdsa" || "$WIREMOCK_ENABLE_ECDSA" == "true" ]] ; then
  echo "Using ecliptic curves"
  pfxFile="$SCRIPT_DIR/wiremock-ecdsa.p12"
else
  echo "Using RSA"
  pfxFile="$SCRIPT_DIR/wiremock.p12"
fi

if [ ! -f "$SCRIPT_DIR/wiremock-standalone-3.11.0.jar" ]; then
  curl -O https://repo1.maven.org/maven2/org/wiremock/wiremock-standalone/3.11.0/wiremock-standalone-3.11.0.jar
fi

java -jar "$SCRIPT_DIR/wiremock-standalone-3.11.0.jar" --port ${WIREMOCK_PORT:=14355} --https-port ${WIREMOCK_HTTPS_PORT:=13567} --https-keystore "$pfxFile" --keystore-type PKCS12 --keystore-password password
