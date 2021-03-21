package server

import (
	"errors"
	"io/ioutil"
	"log"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
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

// Dump serilizes buckets and write to disk in parallel
func (s *Server) Dump(path string) error {
	s.db.RLock()
	defer s.db.RUnlock()

	for k, v := range s.db.Buckets {
		// TODO: add error handling
		go v.SaveBucket(filepath.Join(path, k))
	}
	return nil
}

// Load loads buckets from disk by given dir. path
func (s *Server) Load(path string) error {
	s.db.Lock()
	defer s.db.Unlock()

	files, err := ioutil.ReadDir(path)
	if err != nil {
		return err
	}
	for _, file := range files {
		if !file.IsDir() {
			// TODO: add error handling
			go func() {
				fname := file.Name()
				tempBucket := make(core.MapInstance)
				err = tempBucket.LoadBucket(filepath.Join(path, fname))
				s.db.Buckets[fname] = tempBucket
			}()
		}
	}
	return nil
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
