package core

// influenced by https://github.com/orcaman/concurrent-map
import (
	"bytes"
	"encoding/gob"
	"hash/fnv"
	"io"
	"os"
	"sync"
)

type BucketInstance []*ConcurrentMap

type ConcurrentMap struct {
	items map[string][]byte
	sync.RWMutex
}

func NewBucket(shardCount int) BucketInstance {
	m := make(BucketInstance, shardCount)
	for i := 0; i < shardCount; i++ {
		m[i] = &ConcurrentMap{items: make(map[string][]byte)}
	}
	return m
}

func fnv32(s string) uint32 {
	h := fnv.New32()
	h.Write([]byte(s))
	return h.Sum32()
}

func (b BucketInstance) GetShard(key string) *ConcurrentMap {
	return b[uint(fnv32(key))%uint(len(b))]
}

// Get returns value by string key
func (b BucketInstance) Get(key string) ([]byte, bool) {
	shard := b.GetShard(key)
	shard.RLock()
	val, ok := shard.items[key]
	shard.RUnlock()
	return val, ok
}

// Set places value in the needed shard by string key
func (b BucketInstance) Set(key string, value []byte) {
	shard := b.GetShard(key)
	shard.Lock()
	shard.items[key] = value
	shard.Unlock()
}

// Has checks that key exists in the map
func (b BucketInstance) Has(key string) bool {
	shard := b.GetShard(key)
	shard.RLock()
	_, ok := shard.items[key]
	shard.RUnlock()
	return ok
}

// Del drops value from map by key
func (b BucketInstance) Del(key string) {
	shard := b.GetShard(key)
	shard.Lock()
	delete(shard.items, key)
	shard.Unlock()
}

// MapKeysIterator creates a channel which holds keys of the map
func (b BucketInstance) MapKeysIterator() chan string {
	c := make(chan string)
	go func() {
		for _, shard := range b {
			for k := range shard.items {
				c <- k
			}
			close(c)
		}
	}()
	return c
}

// TODO: replace BucketInstance with concurrent map

// SerializeBucket encodes bucket as the byte array
func (b BucketInstance) SerializeBucket() ([]byte, error) {
	buf := &bytes.Buffer{}
	enc := gob.NewEncoder(buf)
	err := enc.Encode(b)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// SaveBucket dumps bucket to disk
func (b BucketInstance) SaveBucket(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	dump, err := b.SerializeBucket()
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
func (b *BucketInstance) Deserialize(inp []byte) error {
	buf := &bytes.Buffer{}
	buf.Write(inp)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(b)
	if err != nil {
		return err
	}
	return nil
}

// LoadBucket loads byte array from file
func (b *BucketInstance) LoadBucket(path string) error {
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
	err = b.Deserialize(buf.Bytes())
	if err != nil {
		return err
	}
	return nil
}
