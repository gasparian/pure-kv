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

// SHARDS_NUMBER just holds the constant for number of shards in the concurrent map
const SHARDS_NUMBER = 32

// BucketInstance holds a slice of maps' pointers
type BucketInstance []*ConcurrentMap

// ConcurrentMap is just as regular map but with embedded mutex
type ConcurrentMap struct {
	mutex sync.RWMutex
	Items map[string][]byte
}

// NewBucket creates new BucketInstance and fills it with empty ConcurrentMaps
func NewBucket() BucketInstance {
	m := make(BucketInstance, SHARDS_NUMBER)
	for i := 0; i < SHARDS_NUMBER; i++ {
		m[i] = &ConcurrentMap{Items: make(map[string][]byte)}
	}
	return m
}

func fnv32(s string) uint32 {
	h := fnv.New32()
	h.Write([]byte(s))
	return h.Sum32()
}

// GetShard retrieves ConcurrentMap from the BucketInstance slice by string key
func (b BucketInstance) GetShard(key string) *ConcurrentMap {
	return b[uint(fnv32(key))%uint(len(b))]
}

// Get returns value by string key
func (b BucketInstance) Get(key string) ([]byte, bool) {
	shard := b.GetShard(key)
	shard.mutex.RLock()
	val, ok := shard.Items[key]
	shard.mutex.RUnlock()
	return val, ok
}

// Set places value in the needed shard by string key
func (b BucketInstance) Set(key string, value []byte) {
	shard := b.GetShard(key)
	shard.mutex.Lock()
	shard.Items[key] = value
	shard.mutex.Unlock()
}

// Has checks that key exists in the map
func (b BucketInstance) Has(key string) bool {
	shard := b.GetShard(key)
	shard.mutex.RLock()
	_, ok := shard.Items[key]
	shard.mutex.RUnlock()
	return ok
}

// Del drops value from map by key
func (b BucketInstance) Del(key string) {
	shard := b.GetShard(key)
	shard.mutex.Lock()
	delete(shard.Items, key)
	shard.mutex.Unlock()
}

// MapKeysIterator creates a channel which holds keys of the map
func (b BucketInstance) MapKeysIterator() chan string {
	ch := make(chan string)
	go func() {
		wg := sync.WaitGroup{}
		wg.Add(SHARDS_NUMBER)
		for _, shard := range b {
			go func(sh *ConcurrentMap) {
				sh.mutex.RLock()
				for k := range sh.Items {
					ch <- k
				}
				sh.mutex.RUnlock()
				wg.Done()
			}(shard)
		}
		wg.Wait()
		close(ch)
	}()
	var keys []string
	for {
		k, ok := <-ch
		if !ok {
			break
		}
		keys = append(keys, k)
	}
	outCh := make(chan string)
	go func() {
		for _, k := range keys {
			outCh <- k
		}
		close(outCh)
	}()
	return outCh
}

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
