package purekv

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"runtime"

	"sync"
	"testing"
)

const (
	mapSize                              = 1000000
	defaultTTLMs                         = 100  // default is 100
	defaultTTLGarbageCollectionTimeoutMs = 2000 // default is 2000
)

var (
	keys, values = generateDataset()
	_            = runtime.GOMAXPROCS(12)
)

func getRandomKey() string {
	return hex.EncodeToString(getRandomArray(20))
}

func getRandInt(topBound int) int {
	buf := make([]byte, 8)
	rand.Read(buf)
	out := binary.BigEndian.Uint64(buf)
	intOut := int(out)
	if intOut < 0 {
		intOut = -intOut
	}
	return intOut % topBound
}

func getRandomArray(size int) []byte {
	b := make([]byte, size)
	rand.Read(b)
	return b
}

func getKeyValuePair(recordSizeBytes int) (string, []byte) {
	return getRandomKey(), getRandomArray(recordSizeBytes)
}

func generateDataset() ([]string, [][]byte) {
	keys := make([]string, mapSize)
	values := make([][]byte, mapSize)
	for i := 0; i < mapSize; i++ {
		k, v := getKeyValuePair(1024)
		keys[i] = k
		values[i] = v
	}
	return keys, values
}

func getPopulatedStore(nShards, ttl int) *Store {
	store := NewStore(nShards, defaultTTLGarbageCollectionTimeoutMs)
	for i := range keys {
		store.Set(keys[i], values[i], ttl)
	}
	return store
}

func getKV(idx int) (string, []byte) {
	return keys[idx], values[idx]
}

func setSync(b *testing.B, nShards, concurrency int) {
	b.StopTimer()
	store := NewStore(nShards, defaultTTLGarbageCollectionTimeoutMs)
	defer store.Close()
	var (
		k string
		v []byte
	)
	b.StartTimer()
	for n := 0; n < b.N; n++ {
		wg := sync.WaitGroup{}
		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func() {
				_ = 2
				wg.Done()
			}()
			k, v = getKV(getRandInt(mapSize))
			err := store.Set(k, v, defaultTTLMs)
			if err != nil {
				b.Logf("ERROR: %v\b", err)
			}
		}
		wg.Wait()
	}
	b.StopTimer()
}

func getSync(b *testing.B, nShards, concurrency int) {
	b.StopTimer()
	store := getPopulatedStore(nShards, defaultTTLMs*1000)
	defer store.Close()
	b.StartTimer()
	for n := 0; n < b.N; n++ {
		wg := sync.WaitGroup{}
		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func() {
				_ = 2
				wg.Done()
			}()
			_, ok := store.Get(keys[getRandInt(mapSize)])
			if !ok {
				b.Log("ERROR: key doesn't exist")
			}
		}
		wg.Wait()
	}
	b.StopTimer()
}

func setConcurrent(b *testing.B, nShards, concurrency int) {
	b.StopTimer()
	store := NewStore(nShards, defaultTTLGarbageCollectionTimeoutMs)
	defer store.Close()
	var (
		k string
		v []byte
	)
	b.StartTimer()
	for n := 0; n < b.N; n++ {
		wg := sync.WaitGroup{}
		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func() {
				k, v = getKV(getRandInt(mapSize))
				err := store.Set(k, v, defaultTTLMs)
				if err != nil {
					b.Logf("ERROR: %v\n", err)
				}
				wg.Done()
			}()
		}
		wg.Wait()
	}
	b.StopTimer()
}

func getConcurrent(b *testing.B, nShards, concurrency int) {
	b.StopTimer()
	store := getPopulatedStore(nShards, defaultTTLMs*1000)
	defer store.Close()
	b.StartTimer()
	for n := 0; n < b.N; n++ {
		wg := sync.WaitGroup{}
		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func() {
				_, ok := store.Get(keys[getRandInt(mapSize)])
				if !ok {
					b.Log("ERROR: key doesn't exist")
				}
				wg.Done()
			}()
		}
		wg.Wait()
	}
	b.StopTimer()
}

func BenchmarkSetSync(b *testing.B) {
	concurrencyLevels := []int{10, 50, 100}
	shardsNumber := []int{1, 2, 4, 8, 16, 32}
	for _, concurrency := range concurrencyLevels {
		for _, nShards := range shardsNumber {
			b.Run(fmt.Sprintf("concurrency_%v_shards_%v", concurrency, nShards), func(b *testing.B) {
				setSync(b, nShards, concurrency)
			})
		}
	}
}

func BenchmarkGetSync(b *testing.B) {
	concurrencyLevels := []int{10, 50, 100}
	shardsNumber := []int{1, 2, 4, 8, 16, 32}
	for _, concurrency := range concurrencyLevels {
		for _, nShards := range shardsNumber {
			b.Run(fmt.Sprintf("concurrency_%v_shards_%v", concurrency, nShards), func(b *testing.B) {
				getSync(b, nShards, concurrency)
			})
		}
	}
}

func BenchmarkSetConcurrent(b *testing.B) {
	concurrencyLevels := []int{10, 50, 100}
	shardsNumber := []int{1, 2, 4, 8, 16, 32}
	for _, concurrency := range concurrencyLevels {
		for _, nShards := range shardsNumber {
			b.Run(fmt.Sprintf("concurrency_%v_shards_%v", concurrency, nShards), func(b *testing.B) {
				setConcurrent(b, nShards, concurrency)
			})
		}
	}
}

func BenchmarkGetConcurrent(b *testing.B) {
	concurrencyLevels := []int{10, 50, 100}
	shardsNumber := []int{1, 2, 4, 8, 16, 32}
	for _, concurrency := range concurrencyLevels {
		for _, nShards := range shardsNumber {
			b.Run(fmt.Sprintf("concurrency_%v_shards_%v", concurrency, nShards), func(b *testing.B) {
				getConcurrent(b, nShards, concurrency)
			})
		}
	}
}
