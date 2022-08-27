//go:build !test
// +build !test

package main

import (
	"flag"

	"github.com/gasparian/pure-kv/internal/server"
)

var (
	port                          = flag.Int("port", 6666, "port to listen for rpc calls")
	shards                        = flag.Int("shards", 32, "number of shards in the map")
	TTLGarbageCollectionTimeoutMs = flag.Int("ttl_garbage_timeout", 60000, "time in ms between stale keys deletion routine")
)

func main() {
	flag.Parse()
	srv := server.InitServer(
		*port,
		*shards,
		*TTLGarbageCollectionTimeoutMs,
	)
	srv.Start()
}
