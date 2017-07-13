## Setup
setup:
	go get github.com/Masterminds/glide
	go get github.com/golang/lint/golint
	go get golang.org/x/tools/cmd/goimports
	go get github.com/Songmu/make2help/cmd/make2help

test:
	go test -run none -bench . -benchtime 3s -benchmem -cpuprofile cpu.out -memprofile mem.out
	@echo "For CPU usage, run 'go tool pprof largesetresult.test cpu.out'"
	@echo "For Memory usage, run 'go tool pprof -alloc_space largesetresult.test mem.out'"

## Lint
lint: setup
	go vet $$(glide novendor)
	for pkg in $$(glide novendor -x); do \
		golint -set_exit_status $$pkg || exit $$?; \
	done

## Format source codes using goimports
fmt: setup
	goimports -w $$(glide nv -x)

## Show help
help:
	@make2help $(MAKEFILE_LIST)

.PHONY: install run
