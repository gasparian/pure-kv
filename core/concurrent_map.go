package core

// influenced by https://github.com/orcaman/concurrent-map
import (
	"bytes"
	"encoding/gob"
	"errors"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

var (
	errMapAndFilesLengthNotEqual = errors.New("Bytes slice length defers from the number of shards in concurrent map")
	errWrongFnameFormat          = errors.New("Fname must be started from int idx separated with `_`")
)

// SHARDS_NUMBER just holds the constant for number of shards in the concurrent map
const SHARDS_NUMBER = 32

// ConcurrentMap holds a slice of maps' pointers
type ConcurrentMap []*MapShard

type records map[string][]byte

// MapShard is just as regular map but with embedded mutex
type MapShard struct {
	mutex sync.RWMutex
	Items map[string]records
}

// Serialize encodes shard as a byte array
func (s MapShard) Serialize() ([]byte, error) {
	buf := &bytes.Buffer{}
	enc := gob.NewEncoder(buf)
	err := enc.Encode(s)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Deserialize converts byte array to shard
func (s *MapShard) Deserialize(inp []byte) error {
	buf := &bytes.Buffer{}
	buf.Write(inp)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(s)
	if err != nil {
		return err
	}
	return nil
}

// Save dumps shard to disk
func (s MapShard) Save(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	dump, err := s.Serialize()
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

// Load loads byte array from file
func (s *MapShard) Load(path string) error {
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
	err = s.Deserialize(buf.Bytes())
	if err != nil {
		return err
	}
	return nil
}

// NewMap creates new ConcurrentMap and fills it with empty ConcurrentMaps
func NewMap() ConcurrentMap {
	m := make(ConcurrentMap, SHARDS_NUMBER)
	for i := 0; i < SHARDS_NUMBER; i++ {
		m[i] = &MapShard{Items: make(map[string]records)}
	}
	return m
}

func fnv32(s string) uint32 {
	h := fnv.New32()
	h.Write([]byte(s))
	return h.Sum32()
}

func mergeTwoStrings(s1, s2, delim string) string {
	var sb strings.Builder
	sb.WriteString(s1)
	sb.WriteString(delim)
	sb.WriteString(s2)
	return sb.String()
}

// GetShard gets MapShard by compound key
func (m ConcurrentMap) GetShard(bucketName, key string) *MapShard {
	sb := mergeTwoStrings(bucketName, key, "_")
	hsh := uint(fnv32(sb))
	return m[hsh%uint(SHARDS_NUMBER)]
}

// Get returns value by string key
func (m ConcurrentMap) Get(bucketName, key string) ([]byte, bool) {
	shard := m.GetShard(bucketName, key)
	shard.mutex.RLock()
	bucket, ok := shard.Items[bucketName]
	if !ok {
		return nil, false
	}
	val, ok := bucket[key]
	shard.mutex.RUnlock()
	return val, ok
}

// Set places value in the needed shard by string key
func (m ConcurrentMap) Set(bucketName, key string, value []byte) {
	shard := m.GetShard(bucketName, key)
	shard.mutex.Lock()
	bucket, ok := shard.Items[bucketName]
	if !ok {
		return
	}
	bucket[key] = value
	shard.mutex.Unlock()
}

// Has checks that key exists in the map
func (m ConcurrentMap) Has(bucketName, key string) bool {
	shard := m.GetShard(bucketName, key)
	shard.mutex.RLock()
	bucket, ok := shard.Items[bucketName]
	if !ok {
		return false
	}
	_, ok = bucket[key]
	shard.mutex.RUnlock()
	return ok
}

// Del drops value from map by key
func (m ConcurrentMap) Del(bucketName, key string) {
	shard := m.GetShard(bucketName, key)
	shard.mutex.Lock()
	bucket, ok := shard.Items[bucketName]
	if !ok {
		return
	}
	delete(bucket, key)
	shard.mutex.Unlock()
}

// DelBucket drops bucket from every shard
func (m ConcurrentMap) DelBucket(bucketName string) {
	for _, shard := range m {
		shard.mutex.Lock()
		_, ok := shard.Items[bucketName]
		if ok {
			delete(shard.Items, bucketName)
		}
		shard.mutex.Unlock()
	}
}

// MapKeysIterator creates a channel which holds keys of the map
func (m ConcurrentMap) MapKeysIterator(bucketName string) chan string {
	ch := make(chan string)
	go func() {
		wg := sync.WaitGroup{}
		wg.Add(SHARDS_NUMBER)
		for _, shard := range m {
			go func(sh *MapShard) {
				sh.mutex.RLock()
				bucket, ok := sh.Items[bucketName]
				if ok {
					for k := range bucket {
						ch <- k
					}
				}
				sh.mutex.RUnlock()
				wg.Done()
			}(shard)
		}
		wg.Wait()
		close(ch)
	}()
	return ch
}

type shardProcessor func(int, *MapShard, *sync.WaitGroup, chan error)

func (m ConcurrentMap) iterShard(fn shardProcessor) error {
	errs := make(chan error, SHARDS_NUMBER)
	wg := sync.WaitGroup{}
	wg.Add(SHARDS_NUMBER)
	for i, shard := range m {
		go fn(i, shard, &wg, errs)
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// Dump serilizes buckets and writes them to disk in parallel
func (m ConcurrentMap) Dump(path string) error {
	err := os.RemoveAll(path)
	if err != nil {
		return err
	}
	err = os.MkdirAll(path)
	if err != nil {
		return err
	}
	err = m.iterShard(
		func(idx int, sh *MapShard, wg *sync.WaitGroup, errs chan error) {
			sh.RLock()
			errs <- sh.Save(filepath.Join(path, strconv.Itoa(idx)))
			sh.RUnlock()
			wg.Done()
		},
	)
	if err != nil {
		return err
	}
	return nil
}

// Load loads buckets from disk in parallel
func (m ConcurrentMap) Load(path string) error {
	os.MkdirAll(path, FileMode)
	err := m.iterShard(
		func(idx int, sh *MapShard, wg *sync.WaitGroup, errs chan error) {
			sh.Lock()
			sh.Items = make(map[string]records)
			errs <- sh.Load(filepath.Join(path, strconv.Itoa(idx)))
			sh.Unlock()
			wg.Done()
		},
	)
	if err != nil {
		return err
	}
	return nil
}
