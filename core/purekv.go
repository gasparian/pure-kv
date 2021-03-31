package core

import (
	"errors"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sync"
)

// PureKv main structure for holding maps and key iterators
type PureKv struct {
	mx        *sync.RWMutex
	Iterators map[string]chan string
	Buckets   map[string]BucketInstance
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
	kv.Buckets[req.Bucket] = make(BucketInstance) // TODO: concurrent map
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
// TODO: concurrent map
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
// TODO: concurrent map
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
// TODO: concurrent map
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
// TODO: concurrent map
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
	kv.Iterators[req.Bucket] = kv.Buckets[req.Bucket].MapKeysIterator()
	res.Ok = true
	return nil
}

// Next returns the next key-value pair according to the iterator state
// TODO: concurrent map
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

// DumpDb serilizes buckets and write to disk in parallel
func DumpDb(kv *PureKv, path string) {
	kv.mx.RLock()
	defer kv.mx.RUnlock()

	stored, err := getDirFilesSet(path)
	if err != nil {
		log.Panicln(err)
	}
	for k, v := range kv.Buckets {
		go func(bucketName string, bucket BucketInstance) {
			err := bucket.SaveBucket(filepath.Join(path, bucketName))
			if err != nil {
				log.Panicln(err)
			}
		}(k, v)
	}
	filesToDrop := fnamesSetsDifference(stored, kv.Buckets)
	for _, fname := range filesToDrop {
		fpath := filepath.Join(path, fname)
		os.Remove(fpath)
	}
}

// LoadDb loads buckets from disk by given dir. path
func LoadDb(kv *PureKv, path string) {
	kv.mx.Lock()
	defer kv.mx.Unlock()

	os.MkdirAll(path, FileMode)
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

func getDirFilesSet(path string) (map[string]bool, error) {
	files, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, err
	}
	result := make(map[string]bool)
	for _, file := range files {
		if !file.IsDir() {
			result[file.Name()] = true
		}
	}
	return result, nil
}

func fnamesSetsDifference(filesSet map[string]bool, buckets map[string]BucketInstance) []string {
	var diff []string
	for k := range filesSet {
		if _, ok := buckets[k]; !ok {
			diff = append(diff, k)
		}
	}
	for k := range buckets {
		if _, ok := filesSet[k]; !ok {
			diff = append(diff, k)
		}
	}
	return diff
}
