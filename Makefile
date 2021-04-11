.SILENT:

compile:
	go fmt ./...
	go build -o pure-kv-srv

map-bench:
	go test -v \
	        -run=XXX \
	        -bench=. \
	        -benchtime=0.2s \
	        -benchmem \
	        -memprofile memprofile.out \
	        -cpuprofile profile.out \
	        ./core

map-race-test:
	go test -v \
	        -race \
	        -run=XXX \
	        -bench=. \
	        -benchtime=0.05s \
	        -benchmem \
	        ./core

test-coverage:
	go tool cover -func cover.out | grep total | awk '{print $$3}'

.ONESHELL:
.SHELLFLAGS=-e -c
test:
	path=$(path)
	if [ -z "$$path" ]
	then
	    path=./...
	fi
	go clean -testcache
	go test -v -cover -coverprofile cover.out -race $$path