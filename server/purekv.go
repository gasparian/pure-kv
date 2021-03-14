package server

import (
	"errors"
	"pure-kv-go/core"
	"sync"
)

type MapInstance struct {
	Map        map[uint64][]byte
	MapPointer interface{} // used by NEXT
}

type PureKv struct {
	sync.RWMutex
	Maps map[string]MapInstance
}

func (kv *PureKv) Destroy(req core.Request, res *core.Response) error {
	kv.Lock()
	defer kv.Unlock()
	res.Ok = false
	if len(req.MapKey) == 0 {
		return errors.New("Map key must be defined")
	}
	_, ok := kv.Maps[req.MapKey]
	if ok {
		go delete(kv.Maps, req.MapKey)
	}
	res.Ok = true
	return nil
}

func (kv *PureKv) Del(req core.Request, res *core.Response) error {
	kv.Lock()
	defer kv.Unlock()
	res.Ok = false
	if len(req.MapKey) == 0 {
		return errors.New("Map key must be defined")
	}
	key := GetKey(req)
	_, ok := kv.Maps[req.MapKey].Map[key]
	if ok {
		go delete(kv.Maps[req.MapKey].Map, key)
	}
	res.Ok = true
	return nil
}

func (kv *PureKv) Set(req core.Request, res *core.Response) error {
	kv.Lock()
	defer kv.Unlock()
	res.Ok = false
	if len(req.MapKey) == 0 {
		return errors.New("Map key must be defined")
	}
	if len(req.Value) > 0 {
		key := GetKey(req)
		kv.Maps[req.MapKey].Map[key] = req.Value
		res.Ok = true
	} else {
		return errors.New("Both key and value must be defined")
	}
	return nil
}

func (kv *PureKv) Get(req core.Request, res *core.Response) error {
	kv.RLock()
	defer kv.RUnlock()
	res.Ok = false
	if len(req.MapKey) == 0 {
		return errors.New("Map key must be defined")
	}
	key := GetKey(req)
	val, ok := kv.Maps[req.MapKey].Map[key]
	if ok {
		res.Body = val
	}
	res.Ok = true
	return nil
}

// TODO:
func (kv *PureKv) Next(req core.Request, res *core.Response) error {
	kv.RLock()
	defer kv.RUnlock()
	res.Ok = false
	if len(req.MapKey) == 0 {
		return errors.New("Map key must be defined")
	}
	key := GetKey(req)
	val, ok := kv.Maps[req.MapKey].Map[key]
	if ok {
		res.Body = val
	}
	res.Ok = true
	return nil
}
