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

// Server holds config for RPC server
type Server struct {
	Port               int
	PersistanceTimeout int
	DbPath             string
	db                 *core.PureKv
	listener           net.Listener
}

// InitServer creates a new instance of Server
func InitServer(port, persistanceTimeout int, dbPath string) *Server {
	srv := &Server{
		Port:               port,
		PersistanceTimeout: persistanceTimeout,
		DbPath:             dbPath,
	}
	srv.db = core.InitPureKv()
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
func (s *Server) LoadDb() error {
	err := s.db.Load(s.DbPath, nil)
	if err != nil {
		return err
	}
	return nil
}

// Persist dumps db on disk periodically
func (s *Server) Persist() error {
	for {
		time.Sleep(time.Duration(s.PersistanceTimeout) * time.Second)
		err := s.db.Dump(s.DbPath, nil)
		if err != nil {
			return err
		}
	}
}

// StartRPC starts a new listener and registers the RPC server
func (s *Server) StartRPC() (err error) {
	if s.Port <= 0 {
		err = errors.New("port must be specified")
		return
	}
	rpc.Register(s.db)
	s.listener, err = net.Listen("tcp", ":"+strconv.Itoa(s.Port))
	if err != nil {
		return
	}
	log.Println("Server started")
	rpc.Accept(s.listener)
	return
}

// Run loads db and creates the new RPC server
func (s *Server) Run() {
	defer s.Close()

	err := s.LoadDb()
	if err != nil {
		log.Panicln(err)
	}

	go func() {
		core.HandleSignals()
		log.Println("Signal recieved. terminating")
		s.Close()
		os.Exit(0)
	}()

	go s.Persist() // TODO: add error handling

	err = s.StartRPC()
	if err != nil {
		log.Panicln(err)
	}
}
