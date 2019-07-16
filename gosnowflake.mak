## Setup
setup:
	go get golang.org/x/crypto/ocsp
	go get github.com/Masterminds/glide

## Install dependencies
deps: setup
	glide install

## Update dependencies
update: setup
	glide update

# Format source codes (internally used)
cfmt: setup
	gofmt -w $$(glide nv -x)

# Lint (internally used)
clint: setup
	go vet $$(glide novendor)
	for pkg in $$(glide novendor -x); do \
		golint -set_exit_status $$pkg || exit $$?; \
	done

# Install (internally used)
cinstall:
	export GOBIN=$$GOPATH/bin; \
	go install -tags=sfdebug $(CMD_TARGET).go

# Run (internally used)
crun: install
	$(CMD_TARGET)

.PHONY: setup help cfmt clint cinstall crun
