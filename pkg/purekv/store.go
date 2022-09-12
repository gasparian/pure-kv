package purekv

import (
	"hash/fnv"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gasparian/pure-kv/pkg/serializing"
)

const (
	// Default files creation mode
	FileMode = 0700
)

// entry represents value to be stored
type entry struct {
	Expiry time.Time
	Data   []byte
}

type ttlEntry struct {
	Expiry time.Time
	Key    string
}

// shard is just as regular map but with mutex
type shard struct {
	mutex         sync.RWMutex
	heapInputChan chan ttlEntry
	Items         map[string]*entry
}

func newShard(heapInputChan chan ttlEntry) *shard {
	return &shard{
		heapInputChan: heapInputChan,
		Items:         make(map[string]*entry),
	}
}

// getVal checks that value exists and "alive" and returns it
func (sh *shard) getVal(key string) ([]byte, bool) {
	sh.mutex.RLock()
	defer sh.mutex.RUnlock()
	val, ok := sh.Items[key]
	if ok && (val.Expiry.IsZero() || (!val.Expiry.IsZero() && val.Expiry.After(time.Now()))) {
		return val.Data, true
	}
	return nil, false
}

type ttlHeapSynced struct {
	mutex   sync.RWMutex
	TTLHeap *Heap[ttlEntry]
}

func newTTLHeapSynced() *ttlHeapSynced {
	return &ttlHeapSynced{
		TTLHeap: NewHeap(func(a, b ttlEntry) bool { return a.Expiry.Before(b.Expiry) }),
	}
}

func (ths *ttlHeapSynced) len() int {
	ths.mutex.RLock()
	defer ths.mutex.RUnlock()
	return ths.TTLHeap.Len()
}

func (ths *ttlHeapSynced) push(inp ttlEntry) {
	ths.mutex.Lock()
	defer ths.mutex.Unlock()
	ths.TTLHeap.Push(inp)
}

func (ths *ttlHeapSynced) pop() ttlEntry {
	ths.mutex.Lock()
	defer ths.mutex.Unlock()
	return ths.TTLHeap.Pop()
}

type storeConfig struct {
	mutex                         sync.RWMutex
	ShardsNumber                  int
	TTLGarbageCollectionTimeoutMs int
}

// Store is concurrent map
type Store struct {
	spawnedGoroutines int32
	config            *storeConfig
	shards            []*shard
	keysTTLHeap       *ttlHeapSynced
	heapInputChans    []chan ttlEntry
	exitChan          chan struct{}
}

// NewStore creates new Store and fills it with empty ConcurrentMaps
func NewStore(shardsNumber, ttlGarbageCollectionTimeoutMs int) *Store {
	config := &storeConfig{
		ShardsNumber:                  shardsNumber,
		TTLGarbageCollectionTimeoutMs: ttlGarbageCollectionTimeoutMs,
	}
	store := instantiateStore(config)
	if ttlGarbageCollectionTimeoutMs > 0 {
		store.startTTLGarbageCollector()
	}
	return store
}

func instantiateStore(config *storeConfig) *Store {
	store := &Store{
		config:         config,
		shards:         make([]*shard, config.ShardsNumber),
		heapInputChans: make([]chan ttlEntry, config.ShardsNumber),
		exitChan:       make(chan struct{}),
	}
	store.keysTTLHeap = newTTLHeapSynced()
	for i := 0; i < config.ShardsNumber; i++ {
		inputChan := make(chan ttlEntry)
		store.heapInputChans[i] = inputChan
		store.shards[i] = newShard(inputChan)
	}
	return store
}

// Close sends exit signal to goroutines that doing background work for the store
func (s *Store) Close() {
	n := atomic.LoadInt32(&s.spawnedGoroutines)
	var i int32
	for i = 0; i < n; i++ {
		s.exitChan <- struct{}{}
	}
}

func (s *Store) startTTLGarbageCollector() {
	for _, ch := range s.heapInputChans {
		go func(inputChan chan ttlEntry) {
			for {
				select {
				case <-s.exitChan:
					return
				case keyTTLEntry := <-inputChan:
					s.keysTTLHeap.push(keyTTLEntry)
				}
			}
		}(ch)
	}
	atomic.AddInt32(&s.spawnedGoroutines, int32(len(s.heapInputChans)))
	go func() {
		ticker := time.NewTicker(time.Duration(s.config.TTLGarbageCollectionTimeoutMs) * time.Millisecond)
		for {
			select {
			case <-s.exitChan:
				return
			case now := <-ticker.C:
				heapCurrentLen := s.keysTTLHeap.len()
				for i := 0; i < heapCurrentLen; i++ {
					topExpiry := s.keysTTLHeap.TTLHeap.ViewTop().Expiry
					if !topExpiry.IsZero() && now.After(topExpiry) {
						ttlHeapEntry := s.keysTTLHeap.pop()
						s.Del(ttlHeapEntry.Key)
					} else {
						break
					}
				}
			}
		}
	}()
	atomic.AddInt32(&s.spawnedGoroutines, 1)
}

func fnv32(s string) uint32 {
	h := fnv.New32()
	h.Write([]byte(s))
	return h.Sum32()
}

// getShard gets shard by compound key
func (s *Store) getShard(key string) *shard {
	return s.shards[uint(fnv32(key))%uint(len(s.shards))]
}

// Size calculates total size of the map
func (s *Store) Size() uint64 {
	var size uint64 = 0
	s.iterShard(
		func(idx int, sh *shard, wg *sync.WaitGroup, errs chan error) {
			defer wg.Done()
			sh.mutex.RLock()
			defer sh.mutex.RUnlock()
			atomic.AddUint64(&size, uint64(len(sh.Items)))
		},
	)
	return size
}

// Set places the value in the needed shard by string key
func (s *Store) Set(key string, value []byte, ttlMs int) error {
	sh := s.getShard(key)
	sh.mutex.Lock()
	defer sh.mutex.Unlock()
	e := &entry{Data: value}
	if ttlMs > 0 {
		exp := time.Now().Add(time.Millisecond * time.Duration(ttlMs))
		e.Expiry = exp
		sh.heapInputChan <- ttlEntry{
			Key:    key,
			Expiry: exp,
		}
	}
	sh.Items[key] = e
	return nil
}

// Get returns value by string key
func (s *Store) Get(key string) ([]byte, bool) {
	shard := s.getShard(key)
	return shard.getVal(key)
}

// Has checks that key is in store
func (s *Store) Has(key string) bool {
	shard := s.getShard(key)
	_, ok := shard.getVal(key)
	return ok
}

// Del drops value from map by key
func (s *Store) Del(key string) {
	shard := s.getShard(key)
	shard.mutex.Lock()
	defer shard.mutex.Unlock()
	_, ok := shard.Items[key]
	if ok {
		delete(shard.Items, key)
	}
}

// DelAll drops all values from the map
func (s *Store) DelAll() error {
	err := s.iterShard(
		func(idx int, sh *shard, wg *sync.WaitGroup, errs chan error) {
			defer wg.Done()
			sh.mutex.Lock()
			defer sh.mutex.Unlock()
			for key := range sh.Items {
				delete(sh.Items, key)
			}
		},
	)
	return err
}

type shardProcessor func(int, *shard, *sync.WaitGroup, chan error)

func (s *Store) iterShard(fn shardProcessor) error {
	errs := make(chan error, len(s.shards))
	wg := sync.WaitGroup{}
	wg.Add(len(s.shards))
	for i, sh := range s.shards {
		go fn(i, sh, &wg, errs)
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

// Dump serializes buckets and writes them to disk in parallel
func (s *Store) Dump(path string) error {
	os.MkdirAll(path, FileMode)
	s.config.mutex.RLock()
	err := serializing.SerializeAndSave(
		filepath.Join(path, "config"),
		s.config,
	)
	s.config.mutex.RUnlock()
	if err != nil {
		return err
	}
	err = s.iterShard(
		func(idx int, sh *shard, wg *sync.WaitGroup, errs chan error) {
			defer wg.Done()
			fpath := filepath.Join(path, strconv.Itoa(idx))
			sh.mutex.RLock()
			err := serializing.SerializeAndSave(fpath, sh)
			sh.mutex.RUnlock()
			errs <- err
		},
	)
	if err != nil {
		return err
	}
	s.keysTTLHeap.mutex.RLock()
	err = serializing.SerializeAndSave(
		filepath.Join(path, "ttlHeap"),
		s.keysTTLHeap,
	)
	s.keysTTLHeap.mutex.RUnlock()
	return err
}

// Load loads store object from disk (shards being loaded in parallel)
func Load(path string) (*Store, error) {
	config := &storeConfig{}
	err := serializing.LoadAndDeserialize(
		filepath.Join(path, "config"),
		config,
	)
	if err != nil {
		return nil, err
	}
	s := instantiateStore(config)
	err = s.iterShard(
		func(idx int, sh *shard, wg *sync.WaitGroup, errs chan error) {
			defer wg.Done()
			fpath := filepath.Join(path, strconv.Itoa(idx))
			sh.mutex.Lock()
			sh.Items = make(map[string]*entry)
			err := serializing.LoadAndDeserialize(fpath, sh)
			sh.mutex.Unlock()
			errs <- err
		},
	)
	if err != nil {
		return nil, err
	}
	s.keysTTLHeap.mutex.Lock()
	err = serializing.LoadAndDeserialize(
		filepath.Join(path, "ttlHeap"),
		s.keysTTLHeap,
	)
	s.keysTTLHeap.mutex.Unlock()
	s.startTTLGarbageCollector()
	return s, err
}
