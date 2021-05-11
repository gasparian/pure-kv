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
	items map[string]chan string
}

// PureKv main structure for holding maps and key iterators
type PureKv struct {
	mx           sync.Mutex
	shardsNumber int
	iterators    *mapIterators
	buckets      ConcurrentMap
}

// NewPureKv instantiates the new PureKv object
func NewPureKv(shardsNumber int) *PureKv {
	return &PureKv{
		shardsNumber: shardsNumber,
		iterators:    &mapIterators{items: make(map[string]chan string)},
		buckets:      NewMap(shardsNumber),
	}
}

// Size calcaulates total number of instances in the map
func (kv *PureKv) Size(req Request, res *Response) error {
	val, err := kv.buckets.Size(req.Bucket)
	if err != nil {
		return err
	}
	res.Value = val
	res.Ok = true
	return nil
}

// Create instantiates the new map (bucket)
func (kv *PureKv) Create(req Request, res *Response) error {
	if len(req.Bucket) == 0 {
		return errBucketKeyMustBeDefined
	}
	buckets := kv.buckets
	has := buckets.HasBucket(req.Bucket)
	if !has {
		buckets.SetBucket(req.Bucket)
	}
	res.Ok = true
	return nil
}

// Destroy drops the bucket
func (kv *PureKv) Destroy(req Request, res *Response) error {
	if len(req.Bucket) == 0 {
		return errBucketKeyMustBeDefined
	}
	buckets := kv.buckets
	iterators := kv.iterators
	go func() {
		buckets.DelBucket(req.Bucket)
	}()
	go func() {
		iterators.Lock()
		delete(iterators.items, req.Bucket)
		iterators.Unlock()
	}()
	res.Ok = true
	return nil
}

// DestroyAll drops the entire db
func (kv *PureKv) DestroyAll(req Request, res *Response) error {
	kv.mx.Lock()
	kv.iterators = &mapIterators{items: make(map[string]chan string)}
	kv.buckets = NewMap(kv.shardsNumber)
	kv.mx.Unlock()
	res.Ok = true
	return nil
}

// Del drops any record from map by keys
func (kv *PureKv) Del(req Request, res *Response) error {
	if len(req.Bucket) == 0 {
		return errBucketKeyMustBeDefined
	}
	buckets := kv.buckets
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
	buckets := kv.buckets
	buckets.Set(req.Bucket, req.Key, req.Value)
	res.Ok = true
	return nil
}

// Get returns value by key from one of the maps
func (kv *PureKv) Get(req Request, res *Response) error {
	if len(req.Bucket) == 0 {
		return errBucketKeyMustBeDefined
	}
	buckets := kv.buckets
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
	buckets := kv.buckets
	has := buckets.HasBucket(req.Bucket)
	if !has {
		return errBucketCantBeFound
	}
	iterators := kv.iterators
	iterators.Lock()
	iterators.items[req.Bucket] = buckets.MapKeysIterator(req.Bucket)
	iterators.Unlock()
	res.Ok = true
	return nil
}

// Next returns the next key-value pair according to the iterator state
func (kv *PureKv) Next(req Request, res *Response) error {
	if len(req.Bucket) == 0 {
		return errBucketKeyMustBeDefined
	}
	buckets := kv.buckets
	iterators := kv.iterators
	has := buckets.HasBucket(req.Bucket)
	if !has {
		return errBucketCantBeFound
	}
	iterators.Lock()
	key, open := <-iterators.items[req.Bucket]
	if !open {
		go func() {
			delete(iterators.items, req.Bucket)
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

// Dump ...
func (kv *PureKv) Dump(dbPath string) error {
	return kv.buckets.Dump(dbPath)
}

// Load ...
func (kv *PureKv) Load(dbPath string) error {
	return kv.buckets.Load(dbPath)
}
