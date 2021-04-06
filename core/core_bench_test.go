package core

import (
	"crypto/rand"
	"encoding/hex"
	"testing"
)

func getRandomKey() string {
	return hex.EncodeToString(getRandomArray(20))
}

func getRandomArray(size int) []byte {
	b := make([]byte, size)
	rand.Read(b)
	return b
}

func getKeyValuePairs(mapSize, recordSizeBytes int) ([]string, [][]byte) {
	keys := make([]string, mapSize)
	vals := make([][]byte, mapSize)
	for i := 0; i < mapSize; i++ {
		keys[i] = getRandomKey()
		vals[i] = getRandomArray(recordSizeBytes)
	}
	return keys, vals
}

func createMap(shards int) ConcurrentMap {
	m := NewMap(shards)
	m.SetBucket("test")
	return m
}

func populateMap(m ConcurrentMap, keys []string, vals [][]byte) {
	for i, key := range keys {
		m.Set("test", key, vals[i])
	}
}

func BenchmarkSetSync1Shard(b *testing.B) {
	b.StopTimer()
	keys, vals := getKeyValuePairs(b.N, 1024)
	m := createMap(1)
	b.StartTimer()
	for n := 0; n < b.N; n++ {
		m.Set("test", keys[n], vals[n])
	}
}

func BenchmarkGetSync1Shard(b *testing.B) {
	b.StopTimer()
	keys, vals := getKeyValuePairs(b.N, 1024)
	m := createMap(1)
	populateMap(m, keys, vals)
	b.StartTimer()
	for n := 0; n < b.N; n++ {
		m.Get("test", keys[n])
	}
}

func BenchmarkSetParallel1Shard(b *testing.B) {
	b.StopTimer()
	keys, vals := getKeyValuePairs(b.N, 1024)
	m := createMap(1)
	b.StartTimer()
	for n := 0; n < b.N; n++ {
		go func(n int) {
			m.Set("test", keys[n], vals[n])
		}(n)
	}
}

func BenchmarkGetParallel1Shard(b *testing.B) {
	b.StopTimer()
	keys, vals := getKeyValuePairs(b.N, 1024)
	m := createMap(1)
	populateMap(m, keys, vals)
	b.StartTimer()
	for n := 0; n < b.N; n++ {
		go func(n int) {
			m.Get("test", keys[n])
		}(n)
	}
}

func BenchmarkSetSync16Shard(b *testing.B) {
	b.StopTimer()
	keys, vals := getKeyValuePairs(b.N, 1024)
	m := createMap(16)
	b.StartTimer()
	for n := 0; n < b.N; n++ {
		m.Set("test", keys[n], vals[n])
	}
}

func BenchmarkSetParallel16Shard(b *testing.B) {
	b.StopTimer()
	keys, vals := getKeyValuePairs(b.N, 1024)
	m := createMap(16)
	b.StartTimer()
	for n := 0; n < b.N; n++ {
		go func(n int) {
			m.Set("test", keys[n], vals[n])
		}(n)
	}
}

func BenchmarkGetParallel16Shard(b *testing.B) {
	b.StopTimer()
	keys, vals := getKeyValuePairs(b.N, 1024)
	m := createMap(16)
	populateMap(m, keys, vals)
	b.StartTimer()
	for n := 0; n < b.N; n++ {
		go func(n int) {
			m.Get("test", keys[n])
		}(n)
	}
}

func BenchmarkSetParallel128Shard(b *testing.B) {
	b.StopTimer()
	keys, vals := getKeyValuePairs(b.N, 1024)
	m := createMap(128)
	b.StartTimer()
	for n := 0; n < b.N; n++ {
		go func(n int) {
			m.Set("test", keys[n], vals[n])
		}(n)
	}
}

func BenchmarkGetParallel128Shard(b *testing.B) {
	b.StopTimer()
	keys, vals := getKeyValuePairs(b.N, 1024)
	m := createMap(128)
	populateMap(m, keys, vals)
	b.StartTimer()
	for n := 0; n < b.N; n++ {
		go func(n int) {
			m.Get("test", keys[n])
		}(n)
	}
}

func BenchmarkSetParallel512Shard(b *testing.B) {
	b.StopTimer()
	keys, vals := getKeyValuePairs(b.N, 1024)
	m := createMap(512)
	b.StartTimer()
	for n := 0; n < b.N; n++ {
		go func(n int) {
			m.Set("test", keys[n], vals[n])
		}(n)
	}
}

func BenchmarkGetParallel512Shard(b *testing.B) {
	b.StopTimer()
	keys, vals := getKeyValuePairs(b.N, 1024)
	m := createMap(512)
	populateMap(m, keys, vals)
	b.StartTimer()
	for n := 0; n < b.N; n++ {
		go func(n int) {
			m.Get("test", keys[n])
		}(n)
	}
}
