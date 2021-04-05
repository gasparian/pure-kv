#!/bin/sh
go test -v \
        -race \
        -run=XXX \
        -bench=. \
        -benchtime=0.05s \
        -benchmem \
        ${1}