package server

import (
	"errors"
	"log"
	"net"
	"net/rpc"
	"os"
	"pure-kv-go/core"
	"strconv"
)

// Server holds config for RPC server
type Server struct {
	Port     int
	db       *core.PureKv
	listener net.Listener
}

// Close terminates the server listener
func (s *Server) Close() error {
	if s.listener != nil {
		err := s.listener.Close()
		return err
	}
	return nil
}

// Start starts the RPC server
func (s *Server) Start() (err error) {
	if s.Port <= 0 {
		err = errors.New("port must be specified")
		return
	}
	s.db = &core.PureKv{}
	rpc.Register(s.db)
	s.listener, err = net.Listen("tcp", ":"+strconv.Itoa(s.Port))
	if err != nil {
		return
	}
	log.Println("Server started")
	rpc.Accept(s.listener)
	return
}

// RunServer runs RPC sercer and stops it
func RunServer(port, persistanceTimeout int, dbPath string) {
	server := &Server{Port: port}
	defer server.Close()

	go func() {
		core.HandleSignals()
		log.Println("Signal recieved. terminating")
		server.Close()
		os.Exit(0)
	}()

	err := server.Start()
	if err != nil {
		log.Panicln(err)
	}
}
