package main

import (
	"flag"
	"pure-kv-go/server"
)

var (
	port = flag.Int("port", 6666, "port to listen for rpc calls")
)

func main() {
	flag.Parse()
	server.RunServer(*port)
}
