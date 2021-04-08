#!/bin/sh
go test -v \
        -run=XXX \
        -bench=. \
        -benchtime=0.1s \
        -trace trace.out \
        {1}