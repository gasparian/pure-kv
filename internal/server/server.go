package server

import (
	"encoding/binary"
	"log"
	"net"
	"net/rpc"
	"strconv"
	"sync"

	"github.com/gasparian/pure-kv/pkg/purekv"
)

// Shortcuts for RPC methods
const (
	Size   = "PureKvRPC.Size"
	Del    = "PureKvRPC.Del"
	DelAll = "PureKvRPC.DelAll"
	Set    = "PureKvRPC.Set"
	Has    = "PureKvRPC.Has"
	Get    = "PureKvRPC.Get"
)

// PayLoad holds record and optional status
type PayLoad struct {
	Ok    bool
	TTL   int
	Key   string
	Value []byte
}

// PureKvRPC main structure for holding maps and key iterators
type PureKvRPC struct {
	mx                            sync.RWMutex
	shardsNumber                  int
	ttlGarbageCollectionTimeoutMs int
	store                         *purekv.Store
}

// NewPureKv instantiates the new PureKvRPC object
func NewPureKvRPC(shardsNumber, TTLGarbageCollectionTimeoutMs int) *PureKvRPC {
	return &PureKvRPC{
		shardsNumber:                  shardsNumber,
		ttlGarbageCollectionTimeoutMs: TTLGarbageCollectionTimeoutMs,
		store:                         purekv.NewStore(shardsNumber, TTLGarbageCollectionTimeoutMs),
	}
}

// Size calculates total number of instances in the map
func (kv *PureKvRPC) Size(req PayLoad, res *PayLoad) error {
	val := kv.store.Size()
	bs := make([]byte, 8)
	binary.LittleEndian.PutUint64(bs, val)
	res.Value = bs
	res.Ok = true
	return nil
}

// Del drops any record from map by key
func (kv *PureKvRPC) Del(req PayLoad, res *PayLoad) error {
	kv.store.Del(req.Key)
	res.Ok = true
	return nil
}

// DestroyAll resets the entire store
func (kv *PureKvRPC) DelAll(req PayLoad, res *PayLoad) error {
	kv.mx.Lock()
	defer kv.mx.Unlock()
	kv.store = purekv.NewStore(kv.shardsNumber, kv.ttlGarbageCollectionTimeoutMs)
	res.Ok = true
	return nil
}

// Set just creates the new key value pair
func (kv *PureKvRPC) Set(req PayLoad, res *PayLoad) error {
	kv.store.Set(req.Key, req.Value, req.TTL)
	res.Ok = true
	return nil
}

// Has checks that key presented in store
func (kv *PureKvRPC) Has(req PayLoad, res *PayLoad) error {
	ok := kv.store.Has(req.Key)
	res.Ok = ok
	return nil
}

// Get returns value by key from one of the maps
func (kv *PureKvRPC) Get(req PayLoad, res *PayLoad) error {
	val, ok := kv.store.Get(req.Key)
	res.Value = val
	res.Ok = ok
	return nil
}

// Dump ...
func (kv *PureKvRPC) Dump(path string) error {
	kv.mx.RLock()
	defer kv.mx.RUnlock()
	return kv.store.Dump(path)
}

// Load ...
func (kv *PureKvRPC) Load(path string) error {
	kv.mx.Lock()
	defer kv.mx.Unlock()
	var err error
	kv.store, err = purekv.Load(path)
	return err
}

// Server holds config for RPC server
type Server struct {
	Port     int
	db       *PureKvRPC
	listener net.Listener
}

// InitServer creates a new instance of Server
func InitServer(port, shards, TTLGarbageCollectionTimeoutMs int) *Server {
	srv := &Server{
		Port: port,
		db: NewPureKvRPC(
			shards,
			TTLGarbageCollectionTimeoutMs,
		),
	}
	return srv
}

// Stop terminates server listener and close the store
func (s *Server) Stop() error {
	if s.listener != nil {
		log.Println("Closing server listener")
		err := s.listener.Close()
		if err != nil {
			return err
		}
	}
	s.db.store.Close()
	return nil
}

// startRPC starts a new listener and registers the RPC server
func (s *Server) Start() {
	if s.Port <= 0 {
		panic("Port must be a positive integer")
	}

	rpc.Register(s.db)
	var err error
	s.listener, err = net.Listen("tcp", ":"+strconv.Itoa(s.Port))
	if err != nil {
		panic(err)
	}

	log.Println("Starting server")
	rpc.Accept(s.listener)
}
