package server

import (
	"github.com/gasparian/pure-kv/core"
	"log"
	"net"
	"net/rpc"
	"strconv"
	"sync/atomic"
	"time"
)

// Server holds config for RPC server
type Server struct {
	Port               int
	PersistanceTimeout int
	DbPath             string
	db                 *core.PureKv
	listener           net.Listener
	closed             uint32
}

// InitServer creates a new instance of Server
func InitServer(port, persistanceTimeout, shards int, dbPath string) *Server {
	srv := &Server{
		Port:               port,
		PersistanceTimeout: persistanceTimeout,
		DbPath:             dbPath,
	}
	srv.db = core.NewPureKv(shards)
	return srv
}

// Close terminates the server listener
func (s *Server) Close() error {
	if s.listener != nil {
		log.Println("Closing server listener")
		err := s.listener.Close()
		if err == nil {
			atomic.AddUint32(&s.closed, 1)
		}
		return err
	}
	return nil
}

// loadDb loads db using specified path
func (s *Server) loadDb() error {
	return s.db.Load(s.DbPath)
}

// persist dumps db on disk periodically
func (s *Server) persist() {
	for atomic.LoadUint32(&s.closed) == 0 {
		err := s.db.Dump(s.DbPath)
		if err != nil {
			panic(err)
		}
		time.Sleep(time.Duration(s.PersistanceTimeout) * time.Second)
	}
}

// startRPC starts a new listener and registers the RPC server
func (s *Server) startRPC() {
	atomic.StoreUint32(&s.closed, 0)
	if s.Port <= 0 {
		panic("Port must be a positive integer")
	}
	defer func() {
		time.Sleep(100 * time.Millisecond)
		if atomic.LoadUint32(&s.closed) == 0 {
			s.Close()
		}
	}()

	rpc.Register(s.db)

	var err error
	s.listener, err = net.Listen("tcp", ":"+strconv.Itoa(s.Port))
	if err != nil {
		panic(err)
	}

	log.Println("Starting server")
	rpc.Accept(s.listener)
}

// Run loads db and creates the new RPC server
func (s *Server) Run() {
	s.loadDb()
	go s.persist()
	s.startRPC()
}
