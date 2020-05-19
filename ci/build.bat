REM Format and Lint Golang driver

@echo off
setlocal EnableDelayedExpansion

echo [INFO] Download tools
where golint
IF !ERRORLEVEL! NEQ 0 go get golang.org/x/lint/golint
where make2help
IF !ERRORLEVEL! NEQ 0 go get github.com/Songmu/make2help/cmd/make2help
where staticcheck
IF !ERRORLEVEL! NEQ 0 go get honnef.co/go/tools/cmd/staticcheck

echo [INFO] Go mod
go mod tidy
go mod vendor

FOR /F "tokens=1" %%a IN ('go list ./...') DO (
    echo [INFO] Verifying %%a
    go vet %%a
    golint -set_exit_status %%a
)

