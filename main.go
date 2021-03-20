package main

import (
	"flag"
)

var (
	port     = flag.Uint("port", 1337, "port to listen or connect to for rpc calls")
	isServer = flag.Bool("server", false, "activates server mode")
)

func main() {
	// TODO: run client or server here

}
