package server

import (
	"errors"
	"log"
	"net"
	"net/rpc"
	"os"
	"pure-kv-go/core"
	"strconv"
	"time"
)

var (
	errPortNotSpecified = errors.New("port must be specified")
)

// Server holds config for RPC server
type Server struct {
	Port               int
	PersistanceTimeout int
	DbPath             string
	db                 *core.PureKv
	listener           net.Listener
}

// CleanDir removes all contents of the directory
func (s *Server) CleanDir() {
	os.RemoveAll(s.DbPath)
	os.MkdirAll(s.DbPath, core.FileMode)
}

// InitServer creates a new instance of Server
func InitServer(port, persistanceTimeout int, dbPath string) *Server {
	srv := &Server{
		Port:               port,
		PersistanceTimeout: persistanceTimeout,
		DbPath:             dbPath,
	}
	srv.db = core.NewPureKv()
	return srv
}

// Close terminates the server listener
func (s *Server) Close() error {
	if s.listener != nil {
		err := s.listener.Close()
		return err
	}
	return nil
}

// LoadDb loads db using specified path
func (s *Server) LoadDb() {
	core.LoadDb(s.db, s.DbPath)
}

// Persist dumps db on disk periodically
func (s *Server) Persist() {
	for {
		core.DumpDb(s.db, s.DbPath)
		time.Sleep(time.Duration(s.PersistanceTimeout) * time.Second)
	}
}

// StartRPC starts a new listener and registers the RPC server
func (s *Server) StartRPC() error {
	if s.Port <= 0 {
		return errPortNotSpecified
	}
	rpc.Register(s.db)
	var err error
	s.listener, err = net.Listen("tcp", ":"+strconv.Itoa(s.Port))
	if err != nil {
		return err
	}
	log.Println("Server started")
	rpc.Accept(s.listener)
	return nil
}

// Run loads db and creates the new RPC server
func (s *Server) Run() {
	defer s.Close()

	s.LoadDb()

	go func() {
		core.HandleSignals()
		log.Println("Signal recieved. terminating")
		s.Close()
		os.Exit(0)
	}()

	s.CleanDir()
	go s.Persist()

	err := s.StartRPC()
	if err != nil {
		log.Panicln(err)
	}
}
