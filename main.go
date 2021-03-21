package main

import (
	"flag"
	"pure-kv-go/server"
)

var (
	port               = flag.Int("port", 6666, "port to listen for rpc calls")
	persistanceTimeout = flag.Int("persist", 60, "db persistance timeout in seconds")
	dbPath             = flag.String("db_path", "/tmp/pure-kv-db", "path where serialized buckets will be stored")
)

func main() {
	flag.Parse()
	server.RunServer(
		*port,
		*persistanceTimeout,
		*dbPath,
	)
}
