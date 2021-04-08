#!/bin/sh
go test -v \
        -run=XXX \
        -bench=. \
        -benchtime=0.2s \
        -benchmem \
        -memprofile memprofile.out \
        -cpuprofile profile.out \
        {1}