package core

import (
	"bytes"
	"encoding/gob"
	"errors"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sync"
)

// BucketInstance holds the bucket data itself
type BucketInstance map[string][]byte

// SerializeBucket encodes bucket as the byte array
func (m *BucketInstance) SerializeBucket() ([]byte, error) {
	buf := &bytes.Buffer{}
	enc := gob.NewEncoder(buf)
	err := enc.Encode(m)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// SaveBucket dumps bucket to disk
func (m *BucketInstance) SaveBucket(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	dump, err := m.SerializeBucket()
	if err != nil {
		return err
	}
	if _, err := f.Write(dump); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	return nil
}

// Deserialize converts byte array to bucket
func (m *BucketInstance) Deserialize(inp []byte) error {
	buf := &bytes.Buffer{}
	buf.Write(inp)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(m)
	if err != nil {
		return err
	}
	return nil
}

// LoadBucket loads byte array from file
func (m *BucketInstance) LoadBucket(path string) error {
	buf := &bytes.Buffer{}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	_, err = io.Copy(buf, f)
	if err != nil {
		return err
	}
	f.Close()
	err = m.Deserialize(buf.Bytes())
	if err != nil {
		return err
	}
	return nil
}

// PureKv main structure for holding maps and key iterators
type PureKv struct {
	mx        *sync.RWMutex
	Iterators map[string]chan string
	Buckets   map[string]BucketInstance
}

// mapKeysIterator creates a channel which holds keys of the map
func mapKeysIterator(m BucketInstance) chan string {
	c := make(chan string)
	go func() {
		for k := range m {
			c <- k
		}
		close(c)
	}()
	return c
}

// InitPureKv creates PureKv instance with initialized mutex
func InitPureKv() *PureKv {
	return &PureKv{
		mx: new(sync.RWMutex),
	}
}

// Create instantiates a new map
func (kv *PureKv) Create(req Request, res *Response) error {
	kv.mx.Lock()
	defer kv.mx.Unlock()
	if len(req.Bucket) == 0 {
		return errors.New("Map key must be defined")
	}
	kv.Buckets[req.Bucket] = make(BucketInstance)
	return nil
}

// Destroy drops the entire map by key
func (kv *PureKv) Destroy(req Request, res *Response) error {
	kv.mx.Lock()
	if len(req.Bucket) == 0 {
		kv.mx.Unlock()
		return errors.New("Map key must be defined")
	}
	go func() {
		delete(kv.Buckets, req.Bucket)
		delete(kv.Iterators, req.Bucket)
		kv.mx.Unlock()
	}()
	res.Ok = true
	return nil
}

// Del drops any record from map by keys
func (kv *PureKv) Del(req Request, res *Response) error {
	kv.mx.Lock()
	if len(req.Bucket) == 0 {
		kv.mx.Unlock()
		return errors.New("Map key must be defined")
	}
	_, ok := kv.Buckets[req.Bucket]
	if !ok {
		kv.mx.Unlock()
		return nil
	}
	go func() {
		delete(kv.Buckets[req.Bucket], req.Key)
		kv.mx.Unlock()
	}()
	res.Ok = true
	return nil
}

// Set just creates the new key value pair
func (kv *PureKv) Set(req Request, res *Response) error {
	kv.mx.Lock()
	defer kv.mx.Unlock()
	if len(req.Bucket) == 0 {
		return errors.New("Map key must be defined")
	}
	if len(req.Value) > 0 {
		_, ok := kv.Buckets[req.Bucket]
		if !ok {
			return errors.New("Map cannot be found")
		}
		kv.Buckets[req.Bucket][req.Key] = req.Value
		res.Ok = true
	} else {
		return errors.New("Both key and value must be defined")
	}
	return nil
}

// Get returns value by key from one of the maps
func (kv *PureKv) Get(req Request, res *Response) error {
	kv.mx.RLock()
	defer kv.mx.RUnlock()
	if len(req.Bucket) == 0 {
		return errors.New("Map key must be defined")
	}
	_, ok := kv.Buckets[req.Bucket]
	if !ok {
		return nil
	}
	val, ok := kv.Buckets[req.Bucket][req.Key]
	if ok {
		res.Value = val
		res.Ok = true
	}
	return nil
}

// MakeIterator creates the new map iterator based on channel
func (kv *PureKv) MakeIterator(req Request, res *Response) error {
	kv.mx.Lock()
	defer kv.mx.Unlock()
	if len(req.Bucket) == 0 {
		return errors.New("Map key must be defined")
	}
	_, ok := kv.Buckets[req.Bucket]
	if !ok {
		return errors.New("Map cannot be found")
	}
	kv.Iterators[req.Bucket] = mapKeysIterator(kv.Buckets[req.Bucket])
	res.Ok = true
	return nil
}

// Next returns the next key-value pair according to the iterator state
func (kv *PureKv) Next(req Request, res *Response) error {
	kv.mx.Lock()
	if len(req.Bucket) == 0 {
		kv.mx.Unlock()
		return errors.New("Map key must be defined")
	}
	_, ok := kv.Buckets[req.Bucket]
	if !ok {
		kv.mx.Unlock()
		return errors.New("Map cannot be found")
	}
	_, ok = kv.Iterators[req.Bucket]
	if ok {
		key, ok := <-kv.Iterators[req.Bucket]
		if !ok {
			go func() {
				delete(kv.Iterators, req.Bucket)
				kv.mx.Unlock()
			}()
			return nil
		}
		res.Key = key
		res.Value = kv.Buckets[req.Bucket][key]
		res.Ok = true
	}
	kv.mx.Unlock()
	return nil
}

// Dump serilizes buckets and write to disk in parallel
func Dump(kv *PureKv, path string) {
	kv.mx.RLock()
	defer kv.mx.RUnlock()

	for k, v := range kv.Buckets {
		go func(bucketName string, bucket BucketInstance) {
			err := bucket.SaveBucket(filepath.Join(path, bucketName))
			if err != nil {
				log.Panicln(err)
			}
		}(k, v)
	}
}

// Load loads buckets from disk by given dir. path
func Load(kv *PureKv, path string) {
	kv.mx.Lock()
	defer kv.mx.Unlock()

	_ = os.Mkdir(path, 0700)
	files, err := ioutil.ReadDir(path)
	if err != nil {
		log.Panicln(err)
	}
	for _, file := range files {
		if !file.IsDir() {
			go func() {
				fname := file.Name()
				tempBucket := make(BucketInstance)
				err = tempBucket.LoadBucket(filepath.Join(path, fname))
				if err != nil {
					log.Panicln(err)
				}
				kv.Buckets[fname] = tempBucket
			}()
		}
	}
}
