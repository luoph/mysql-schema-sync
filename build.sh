#!/bin/bash

CGO_ENABLED=0
GOOS=linux
GOARCH=amd64

[ -d target ] && rm -rf target
mkdir -p target
go build -a -ldflags '-extldflags="-static"' -o ./target/mysql-schema-sync ./