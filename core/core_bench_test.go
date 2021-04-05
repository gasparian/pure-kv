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
	N := b.N
	keys, vals := getKeyValuePairs(N, 1024)
	m := createMap(1)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		m.Set("test", keys[n], vals[n])
	}
}

func BenchmarkGetSync1Shard(b *testing.B) {
	N := b.N
	keys, vals := getKeyValuePairs(N, 1024)
	m := createMap(1)
	populateMap(m, keys, vals)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		m.Get("test", keys[n])
	}
}

func BenchmarkSetParallel1Shard(b *testing.B) {
	N := b.N
	keys, vals := getKeyValuePairs(N, 1024)
	m := createMap(1)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		go func(n int) {
			m.Set("test", keys[n], vals[n])
		}(n)
	}
}

func BenchmarkGetParallel1Shard(b *testing.B) {
	N := b.N
	keys, vals := getKeyValuePairs(N, 1024)
	m := createMap(1)
	populateMap(m, keys, vals)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		go func(n int) {
			m.Get("test", keys[n])
		}(n)
	}
}

func BenchmarkSetParallel16Shard(b *testing.B) {
	N := b.N
	keys, vals := getKeyValuePairs(N, 1024)
	m := createMap(16)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		go func(n int) {
			m.Set("test", keys[n], vals[n])
		}(n)
	}
}

func BenchmarkGetParallel16Shard(b *testing.B) {
	N := b.N
	keys, vals := getKeyValuePairs(N, 1024)
	m := createMap(16)
	populateMap(m, keys, vals)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		go func(n int) {
			m.Get("test", keys[n])
		}(n)
	}
}

func BenchmarkSetParallel64Shard(b *testing.B) {
	N := b.N
	keys, vals := getKeyValuePairs(N, 1024)
	m := createMap(64)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		go func(n int) {
			m.Set("test", keys[n], vals[n])
		}(n)
	}
}

func BenchmarkGetParallel64Shard(b *testing.B) {
	N := b.N
	keys, vals := getKeyValuePairs(N, 1024)
	m := createMap(64)
	populateMap(m, keys, vals)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		go func(n int) {
			m.Get("test", keys[n])
		}(n)
	}
}

func BenchmarkSetParallel256Shard(b *testing.B) {
	N := b.N
	keys, vals := getKeyValuePairs(N, 1024)
	m := createMap(256)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		go func(n int) {
			m.Set("test", keys[n], vals[n])
		}(n)
	}
}

func BenchmarkGetParallel256Shard(b *testing.B) {
	N := b.N
	keys, vals := getKeyValuePairs(N, 1024)
	m := createMap(256)
	populateMap(m, keys, vals)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		go func(n int) {
			m.Get("test", keys[n])
		}(n)
	}
}

func BenchmarkSetParallel1024Shard(b *testing.B) {
	N := b.N
	keys, vals := getKeyValuePairs(N, 1024)
	m := createMap(1024)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		go func(n int) {
			m.Set("test", keys[n], vals[n])
		}(n)
	}
}

func BenchmarkGetParallel1024Shard(b *testing.B) {
	N := b.N
	keys, vals := getKeyValuePairs(N, 1024)
	m := createMap(1024)
	populateMap(m, keys, vals)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		go func(n int) {
			m.Get("test", keys[n])
		}(n)
	}
}
