#!/bin/sh
path=$1
if [ -z "$path" ] 
then 
    path=./...
fi
go test -v -cover -race $path