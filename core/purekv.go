package core

import (
	"errors"
	"sync"
)

var (
	errBucketKeyMustBeDefined = errors.New("Bucket key must be defined")
	errBucketCantBeFound      = errors.New("Bucket can't be found")
	errKeyCantBeFound         = errors.New("Key can't be found")
)

type mapIterators struct {
	sync.RWMutex
	Items map[string]chan string
}

// PureKv main structure for holding maps and key iterators
type PureKv struct {
	Iterators *mapIterators
	Buckets   ConcurrentMap
}

// NewPureKv instantiates the new PureKv object
func NewPureKv(shardsNumber int) *PureKv {
	return &PureKv{
		Iterators: &mapIterators{Items: make(map[string]chan string)},
		Buckets:   NewMap(shardsNumber),
	}
}

// Create instantiates the new map
func (kv *PureKv) Create(req Request, res *Response) error {
	if len(req.Bucket) == 0 {
		return errBucketKeyMustBeDefined
	}
	buckets := kv.Buckets
	buckets.SetBucket(req.Bucket)
	res.Ok = true
	return nil
}

// Destroy drops the entire bucket
func (kv *PureKv) Destroy(req Request, res *Response) error {
	if len(req.Bucket) == 0 {
		return errBucketKeyMustBeDefined
	}
	buckets := kv.Buckets
	iterators := kv.Iterators
	go func() {
		buckets.DelBucket(req.Bucket)
	}()
	go func() {
		iterators.Lock()
		delete(iterators.Items, req.Bucket)
		iterators.Unlock()
	}()
	res.Ok = true
	return nil
}

// Del drops any record from map by keys
func (kv *PureKv) Del(req Request, res *Response) error {
	if len(req.Bucket) == 0 {
		return errBucketKeyMustBeDefined
	}
	buckets := kv.Buckets
	go func() {
		buckets.Del(req.Bucket, req.Key)
	}()
	res.Ok = true
	return nil
}

// Set just creates the new key value pair
func (kv *PureKv) Set(req Request, res *Response) error {
	if len(req.Bucket) == 0 {
		return errBucketKeyMustBeDefined
	}
	buckets := kv.Buckets
	buckets.Set(req.Bucket, req.Key, req.Value)
	res.Ok = true
	return nil
}

// Get returns value by key from one of the maps
func (kv *PureKv) Get(req Request, res *Response) error {
	if len(req.Bucket) == 0 {
		return errBucketKeyMustBeDefined
	}
	buckets := kv.Buckets
	val, ok := buckets.Get(req.Bucket, req.Key)
	if !ok {
		return errBucketCantBeFound
	}
	res.Value = val
	res.Ok = true
	return nil
}

// MakeIterator creates the new map iterator based on channel
func (kv *PureKv) MakeIterator(req Request, res *Response) error {
	if len(req.Bucket) == 0 {
		return errBucketKeyMustBeDefined
	}
	buckets := kv.Buckets
	has := buckets.HasBucket(req.Bucket)
	if !has {
		return errBucketCantBeFound
	}
	iterators := kv.Iterators
	iterators.Lock()
	iterators.Items[req.Bucket] = buckets.MapKeysIterator(req.Bucket)
	iterators.Unlock()
	res.Ok = true
	return nil
}

// Next returns the next key-value pair according to the iterator state
func (kv *PureKv) Next(req Request, res *Response) error {
	if len(req.Bucket) == 0 {
		return errBucketKeyMustBeDefined
	}
	buckets := kv.Buckets
	iterators := kv.Iterators
	has := buckets.HasBucket(req.Bucket)
	if !has {
		return errBucketCantBeFound
	}
	iterators.Lock()
	key, open := <-iterators.Items[req.Bucket]
	if !open {
		go func() {
			delete(iterators.Items, req.Bucket)
			iterators.Unlock()
		}()
		return nil
	}
	iterators.Unlock()
	res.Key = key
	var ok bool
	res.Value, ok = buckets.Get(req.Bucket, key)
	if !ok {
		return errKeyCantBeFound
	}
	res.Ok = true
	return nil
}
