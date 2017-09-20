## Setup
setup:
	go get github.com/Masterminds/glide
	go get github.com/golang/lint/golint
	go get github.com/Songmu/make2help/cmd/make2help
	go get honnef.co/go/tools/cmd/megacheck

## Show help
help:
	@make2help $(MAKEFILE_LIST)

# Format source codes (internally used)
cfmt: setup
	gofmt -w $$(glide nv -x)

# Lint (internally used)
clint: setup
	go vet $$(glide novendor)
	megacheck
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
