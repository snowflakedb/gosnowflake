# Refreshing wiremock test cert

Password for CA is `password`.

```bash
openssl x509 -req -in wiremock.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out wiremock.crt -days 365 -sha256 -extfile wiremock.v3.ext
openssl pkcs12 -export -out wiremock.p12 -inkey wiremock.key -in wiremock.crt
```

# Refreshing ECDSA cert

```bash
openssl x509 -req -in wiremock-ecdsa.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out wiremock-ecdsa.crt -days 365 -sha256 -extfile wiremock.v3.ext
openssl pkcs12 -export -inkey wiremock-ecdsa.key -in wiremock-ecdsa.crt -out wiremock-ecdsa.p12
```