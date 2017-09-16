## Setup
setup:
	go get github.com/Masterminds/glide
	go get github.com/golang/lint/golint
	go get github.com/Songmu/make2help/cmd/make2help

## Install
install:
	export GOBIN=$$GOPATH/bin; \
	go install -tags=sfdebug selectmany.go

## Run
run: install
	selectmany

## Lint
lint: setup
	go vet $$(glide novendor)
	for pkg in $$(glide novendor -x); do \
		golint -set_exit_status $$pkg || exit $$?; \
	done

## Format source codes using gofmt
fmt: setup
	gofmt -w $$(glide nv -x)

## Show help
help:
	@make2help $(MAKEFILE_LIST)

.PHONY: install run
