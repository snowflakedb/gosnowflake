---
trigger: glob
description: 
globs: **/*.go
---

# Go files rules

## General

1. Unless it's necessary or told otherwise, try reusing existing files, both for implementation and tests.
2. If possible, try running relevant tests.

## Tests

1. Create a test file with the name same as prod code file by default.
2. For assertions use our test helpers defined in assert_test.go.

## Logging

1. Add reasonable logging - don't repeat logs, but add them when it's meaningful.
2. Always consider log levels.