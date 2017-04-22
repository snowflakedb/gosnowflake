NAME:=gosnowflake
VERSION:=$(shell git describe --tags --abbrev=0)
REVISION:=$(shell git rev-parse --short HEAD)

setup:
	go get github.com/Masterminds/glide
	go get github.com/golang/lint/golint
	go get golang.org/x/tools/cmd/goimports
	go get github.com/Songmu/make2help/cmd/make2help

test: deps
	eval $$(jq -r '.testconnection | to_entries | map("export \(.key)=\(.value|tostring)")|.[]' parameters.json) && \
		env | grep SNOWFLAKE && \
		go test -v $$(glide novendor)

deps: setup
	glide install


update: setup
	glide update

lint: setup
	go vet $$(glide novendor)
	for pkg in $$(glide novendor -x); do \
		golint -set_exit_status $$pkg || exit $$?; \
	done

fmt: setup
	goimports -w $$(glid nv -x)

help:
	@make2help $(MAKEFILE_LIST)

.PHONY: setup deps update test lint help
