.SILENT:

.DEFAULT_GOAL := build

build:
	go build -ldflags '-w -extldflags "-static"' -v -o purekv ./cmd/rpcserver

benchmark:
	go test -v \
	    -run=XXX \
	    -bench=. \
	    -benchtime=0.2s \
	    -benchmem \
	    -memprofile memprofile.prof \
	    -cpuprofile cpuprofile.prof \
		-count=1 \
		-timeout 300s \
	    ./pkg/purekv

test-coverage:
	go tool cover -func cover.out | grep total | awk '{print $$3}'

test:
	go test -v -cover -coverprofile cover.out -race -count=1 -timeout 30s ./...

install-hooks:
	cp -rf .githooks/pre-commit .git/hooks/pre-commit
