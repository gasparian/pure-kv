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

type bucketsMap struct {
	sync.RWMutex
	Items map[string]BucketInstance
}

// PureKv main structure for holding maps and key iterators
type PureKv struct {
	Iterators *mapIterators
	Buckets   *bucketsMap
}

// NewPureKv instantiates the new PureKv object
func NewPureKv() *PureKv {
	return &PureKv{
		Iterators: &mapIterators{Items: make(map[string]chan string)},
		Buckets:   &bucketsMap{Items: make(map[string]BucketInstance)},
	}
}

// Create instantiates the new map
func (kv *PureKv) Create(req Request, res *Response) error {
	if len(req.Bucket) == 0 {
		return errBucketKeyMustBeDefined
	}
	buckets := kv.Buckets
	buckets.Lock()
	buckets.Items[req.Bucket] = NewBucket()
	buckets.Unlock()
	return nil
}

// Destroy drops the entire map by key
func (kv *PureKv) Destroy(req Request, res *Response) error {
	if len(req.Bucket) == 0 {
		return errBucketKeyMustBeDefined
	}
	buckets := kv.Buckets
	iterators := kv.Iterators
	go func() {
		buckets.Lock()
		delete(buckets.Items, req.Bucket)
		buckets.Unlock()
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
	buckets.RLock()
	bucket, ok := buckets.Items[req.Bucket]
	buckets.RUnlock()
	if !ok {
		return errBucketCantBeFound
	}
	go func() {
		bucket.Del(req.Key)
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
	buckets.RLock()
	bucket, ok := buckets.Items[req.Bucket]
	buckets.RUnlock()
	if !ok {
		return errBucketCantBeFound
	}
	bucket.Set(req.Key, req.Value)
	res.Ok = true
	return nil
}

// Get returns value by key from one of the maps
func (kv *PureKv) Get(req Request, res *Response) error {
	if len(req.Bucket) == 0 {
		return errBucketKeyMustBeDefined
	}
	buckets := kv.Buckets
	buckets.RLock()
	bucket, ok := buckets.Items[req.Bucket]
	buckets.RUnlock()
	if !ok {
		return errBucketCantBeFound
	}
	val, ok := bucket.Get(req.Key)
	if ok {
		res.Value = val
		res.Ok = true
	}
	return nil
}

// MakeIterator creates the new map iterator based on channel
func (kv *PureKv) MakeIterator(req Request, res *Response) error {
	if len(req.Bucket) == 0 {
		return errBucketKeyMustBeDefined
	}
	buckets := kv.Buckets
	iterators := kv.Iterators
	buckets.RLock()
	bucket, ok := buckets.Items[req.Bucket]
	buckets.RUnlock()
	if !ok {
		return errBucketCantBeFound
	}
	iterators.Lock()
	iterators.Items[req.Bucket] = bucket.MapKeysIterator()
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
	buckets.RLock()
	bucket, ok := buckets.Items[req.Bucket]
	buckets.RUnlock()
	if !ok {
		return errBucketCantBeFound
	}
	iterators.Lock()
	key, ok := <-iterators.Items[req.Bucket]
	if !ok {
		go func() {
			delete(iterators.Items, req.Bucket)
			iterators.Unlock()
		}()
		return nil
	}
	iterators.Unlock()
	res.Key = key
	res.Value, ok = bucket.Get(key)
	if !ok {
		return errKeyCantBeFound
	}
	res.Ok = true
	return nil
}
