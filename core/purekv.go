package core

import (
	"errors"
	"sync"
)

type mapInstance map[string][]byte

// PureKv main structure for holding maps and key iterators
type PureKv struct {
	sync.RWMutex
	Iterators map[string]chan string
	Maps      map[string]mapInstance
}

func mapIterator(m mapInstance) chan string {
	c := make(chan string)
	go func() {
		for k := range m {
			c <- k
		}
		close(c)
	}()
	return c
}

// Create instantiates a new map
func (kv *PureKv) Create(req Request, res *Response) error {
	kv.Lock()
	defer kv.Unlock()
	if len(req.MapKey) == 0 {
		return errors.New("Map key must be defined")
	}
	kv.Maps[req.MapKey] = make(mapInstance)
	return nil
}

// Destroy drops the entire map by key
func (kv *PureKv) Destroy(req Request, res *Response) error {
	kv.Lock()
	if len(req.MapKey) == 0 {
		return errors.New("Map key must be defined")
	}
	_, ok := kv.Maps[req.MapKey]
	if ok {
		go func() {
			delete(kv.Maps, req.MapKey)
			kv.Unlock()
		}()
	} else {
		kv.Unlock()
	}
	res.Ok = true
	return nil
}

// Del drops any record from map by keys
func (kv *PureKv) Del(req Request, res *Response) error {
	kv.Lock()
	if len(req.MapKey) == 0 {
		return errors.New("Map key must be defined")
	}
	_, ok := kv.Maps[req.MapKey]
	if !ok {
		return nil
	}
	_, ok = kv.Maps[req.MapKey][req.Key]
	if ok {
		go func() {
			delete(kv.Maps[req.MapKey], req.Key)
			kv.Unlock()
		}()
	} else {
		kv.Unlock()
	}
	res.Ok = true
	return nil
}

// Set just creates the new key value pair
func (kv *PureKv) Set(req Request, res *Response) error {
	kv.Lock()
	defer kv.Unlock()
	if len(req.MapKey) == 0 {
		return errors.New("Map key must be defined")
	}
	if len(req.Value) > 0 {
		_, ok := kv.Maps[req.MapKey]
		if !ok {
			return errors.New("Map cannot be found")
		}
		kv.Maps[req.MapKey][req.Key] = req.Value
		res.Ok = true
	} else {
		return errors.New("Both key and value must be defined")
	}
	return nil
}

// Get returns value by key from one of the maps
func (kv *PureKv) Get(req Request, res *Response) error {
	kv.RLock()
	defer kv.RUnlock()
	if len(req.MapKey) == 0 {
		return errors.New("Map key must be defined")
	}
	_, ok := kv.Maps[req.MapKey]
	if !ok {
		return nil
	}
	val, ok := kv.Maps[req.MapKey][req.Key]
	if ok {
		res.Value = val
		res.Ok = true
	}
	return nil
}

// MakeIterator creates the new map iterator based on channel
func (kv *PureKv) MakeIterator(req Request, res *Response) error {
	kv.Lock()
	defer kv.Unlock()
	if len(req.MapKey) == 0 {
		return errors.New("Map key must be defined")
	}
	_, ok := kv.Maps[req.MapKey]
	if !ok {
		return errors.New("Map cannot be found")
	}
	kv.Iterators[req.MapKey] = mapIterator(kv.Maps[req.MapKey])
	res.Ok = true
	return nil
}

// Next returns the next key-value pait according to the iterator state
func (kv *PureKv) Next(req Request, res *Response) error {
	kv.Lock()
	if len(req.MapKey) == 0 {
		return errors.New("Map key must be defined")
	}
	_, ok := kv.Maps[req.MapKey]
	if !ok {
		return errors.New("Map cannot be found")
	}
	_, ok = kv.Iterators[req.MapKey]
	if ok {
		key, ok := <-kv.Iterators[req.MapKey]
		if !ok {
			go func() {
				delete(kv.Iterators, req.MapKey)
				kv.Unlock()
			}()
			return nil
		} else {
			kv.Unlock()
		}
		res.Key = key
		res.Value = kv.Maps[req.MapKey][key]
		res.Ok = true
	} else {
		kv.Unlock()
	}
	return nil
}
