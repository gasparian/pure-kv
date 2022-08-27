package purekv

import (
	"bytes"
	"errors"
	"os"
	"sync"
	"testing"
	"time"
)

func TestSetGet(t *testing.T) {
	t.Parallel()
	s := NewStore(32, 1000)
	defer s.Close()
	key := "key"
	val := []byte{'a'}

	t.Run("Set", func(t *testing.T) {
		err := s.Set(key, val, 0)
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("Has", func(t *testing.T) {
		ok := s.Has(key)
		if !ok {
			t.Error("Key should exist")
		}
	})

	t.Run("Size", func(t *testing.T) {
		s.Set("key2", val, 0)
		size := s.Size()
		if size < 2 {
			t.Error("Can't get size of the map")
		}
	})

	t.Run("Get", func(t *testing.T) {
		valGet, ok := s.Get(key)
		if !ok {
			t.Error("Key should exist")
		}
		res := bytes.Compare(val, valGet)
		if res != 0 {
			t.Error("Values should be equal")
		}
	})

	t.Run("Del", func(t *testing.T) {
		s.Del(key)
		time.Sleep(250 * time.Millisecond)
		ok := s.Has(key)
		if ok {
			t.Error("Key should not exist")
		}
	})

	t.Run("DelAll", func(t *testing.T) {
		err := s.Set(key, val, 0)
		if err != nil {
			t.Error(err)
		}
		err = s.DelAll()
		if err != nil {
			t.Error(err)
		}
		time.Sleep(250 * time.Millisecond)
		ok := s.Has(key)
		if ok {
			t.Error("Key should not exist")
		}
	})
}

func TestTTL(t *testing.T) {
	ttlGarbageCollectorTimeoutMs := 500
	s := NewStore(2, ttlGarbageCollectorTimeoutMs)
	defer s.Close()
	key := "key"
	val := []byte{'a'}
	ttl := 100
	err := s.Set(key, val, ttl)
	if err != nil {
		t.Error(err)
	}
	valGet, ok := s.Get(key)
	if !ok {
		t.Error("key should be presented yet")
	}
	if !bytes.Equal(val, valGet) {
		t.Errorf("expected value `%v`, but got `%v`", val, valGet)
	}
	time.Sleep(time.Duration(ttl) * time.Millisecond)
	_, ok = s.Get(key)
	if ok {
		t.Error("key should not be valid any more")
	}
	time.Sleep(time.Duration(ttlGarbageCollectorTimeoutMs) * time.Millisecond)
	shard := s.getShard(key)
	shard.mutex.RLock()
	_, ok = shard.Items[key]
	shard.mutex.RUnlock()
	if ok {
		t.Error("key should be deleted at this moment")
	}
}

func TestSetGetConcurrent(t *testing.T) {
	t.Parallel()
	s := NewStore(32, 10000)
	defer s.Close()
	val := []byte{'a'}
	n := 10
	keys := make([]string, n)

	t.Run("Set", func(t *testing.T) {
		wg := sync.WaitGroup{}
		for i := 1; i < n; i += 2 {
			wg.Add(2)
			go func(i int) {
				k1 := getRandomKey()
				keys[i-1] = k1
				k2 := getRandomKey()
				keys[i] = k2
				go func() {
					s.Set(k1, val, 200)
					wg.Done()
				}()
				go func() {
					s.Set(k2, val, 200)
					wg.Done()
				}()
			}(i)
		}
		wg.Wait()
	})

	t.Run("Get", func(t *testing.T) {
		wg := sync.WaitGroup{}
		wg.Add(10)
		errs := make(chan error)
		go func() {
			for i := 0; i < n; i++ {
				go func(i int) {
					defer wg.Done()
					k := keys[i]
					ok := s.Has(k)
					if !ok {
						errs <- errors.New("Key should exist")
					}
				}(i)
			}
			wg.Wait()
			close(errs)
		}()
		for err := range errs {
			if err != nil {
				t.Error(err)
			}
		}
	})
}

func TestDumpLoad(t *testing.T) {
	t.Parallel()
	path := "/tmp/pure-kv-db-core-test"
	defer os.RemoveAll(path)
	s := NewStore(32, 1000)
	defer s.Close()
	s.Set("key1", []byte{'a'}, 0)

	t.Run("Dump", func(t *testing.T) {
		err := s.Dump(path)
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("Load", func(t *testing.T) {
		s2, err := Load(path)
		if err != nil {
			t.Error(err)
		}
		_, ok := s2.Get("key1")
		if !ok {
			t.Error("Key must be in map after load")
		}
	})

}
