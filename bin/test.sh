#!/usr/bin/env bash

set -eu

failed="false"

whine() {
  echo "$@" >&2
}

setup() {
  if [[ -x "./bin/setup" ]]; then
    ./bin/setup
  else
    go get -t -d ./...
  fi
}

install_tools() {
  go get \
    golang.org/x/lint/golint \
    github.com/mattn/goveralls
}

test_fmt() {
  local fmtresult="$(gofmt -l . | grep -v bindata_)"

  if test -n "${fmtresult}"; then
    failed="true"
    whine Code is not gofmt-clean. gofmt output:
    whine "${fmtresult}"
  fi
}

test_lint() {
  local lintresult="$(golint ./... | grep -v bindata_)"
  if test -n "${lintresult}"; then
    failed="true"
    whine Code is not golint-clean. golint output:
    whine "${lintresult}"
  fi
}

test_vet() {
  if ! go vet ./...; then
    failed="true"
    whine go vet failed.
  fi
}

test_without_coverage() {
  if ! go test ./...; then
    failed="true"
  fi
}

setup
install_tools
test_fmt
test_lint
test_vet
test_without_coverage

if [[ "${failed}" != "false" ]]; then
  exit 1
fi
