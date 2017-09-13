NAME:=gosnowflake
VERSION:=$(shell git describe --tags --abbrev=0)
REVISION:=$(shell git rev-parse --short HEAD)
COVFLAGS:=

## Run fmt, lint and test
all: fmt lint cov

## Setup
setup:
	go get github.com/Masterminds/glide
	go get github.com/golang/lint/golint
	go get github.com/Songmu/make2help/cmd/make2help

## Run tests
test: deps
	eval $$(jq -r '.testconnection | to_entries | map("export \(.key)=\(.value|tostring)")|.[]' parameters.json) && \
		go test -race $(COVFLAGS) -v . # -stderrthreshold=INFO -vmodule=*=2 or -log_dir=$(HOME) -vmodule=connection=2,driver=2

## Run Coverage tests
cov:
	make test COVFLAGS="-coverprofile=coverage.txt -covermode=atomic"

## Install dependencies
deps: setup
	glide install

## Update dependencies
update: setup
	glide update

## Lint
lint: setup
	go vet $$(glide novendor)
	for pkg in $$(glide novendor -x); do \
		golint -set_exit_status $$pkg || exit $$?; \
	done
	for c in $$(ls cmd); do \
		(cd cmd/$$c;  make lint); \
	done

## Format source codes using gofmt
fmt: setup
	gofmt -w $$(glide nv -x)
	for c in $$(ls cmd); do \
		(cd cmd/$$c;  make fmt); \
	done

## Install sample programs
install:
	for c in $$(ls cmd); do \
		(cd cmd/$$c;  GOBIN=$$GOPATH/bin go install $$c.go); \
	done

## Build fuzz tests
fuzz-build:
	for c in $$(ls | grep -E "fuzz-*"); do \
		(cd $$c; make fuzz-build); \
	done

## Run fuzz-dsn
fuzz-dsn:
	(cd fuzz-dsn; go-fuzz -bin=./dsn-fuzz.zip -workdir=.)

## Show help
help:
	@make2help $(MAKEFILE_LIST)

.PHONY: setup deps update test lint help fuzz-dsn
