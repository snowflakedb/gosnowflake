# Refreshing wiremock test cert

Password for CA is `password`.

```bash
openssl x509 -req -in wiremock.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out wiremock.crt -days 365 -sha256 -extfile wiremock.v3.ext
```