package core

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestBucket(t *testing.T) {
	t.Parallel()
	m := NewMap(32)
	bucketName := "test"
	m.SetBucket(bucketName)

	t.Run("HasBucket", func(t *testing.T) {
		ok := m.HasBucket(bucketName)
		if !ok {
			t.Error("Bucket `test` should exist")
		}
	})

	t.Run("DelBucket", func(t *testing.T) {
		m.DelBucket(bucketName)
		time.Sleep(250 * time.Millisecond)
		ok := m.HasBucket(bucketName)
		if ok {
			t.Error("Bucket `test` should not be exist")
		}
	})
}

func TestSetGet(t *testing.T) {
	t.Parallel()
	m := NewMap(32)
	bucketName := "test"
	key := "key"
	val := []byte{'a'}

	t.Run("Set", func(t *testing.T) {
		m.SetBucket(bucketName)
		err := m.Set(bucketName, key, val)
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("HasKey", func(t *testing.T) {
		ok := m.Has(bucketName, key)
		if !ok {
			t.Error("Key should exist")
		}
	})

	t.Run("Size", func(t *testing.T) {
		m.SetBucket("test2")
		m.Set("test2", "key2", val)
		size, err := m.Size("")
		if err != nil || size < 2 {
			t.Error("Can't get size of the map")
		}
		bucketSize, err := m.Size(bucketName)
		if err != nil || bucketSize != 1 {
			t.Error("Bucket size and total size are not equal")
		}
	})

	t.Run("GetKey", func(t *testing.T) {
		valGet, ok := m.Get(bucketName, key)
		if !ok {
			t.Error("Key should exist")
		}
		res := bytes.Compare(val, valGet)
		if res != 0 {
			t.Error("Values should be equal")
		}
	})

	t.Run("DelKey", func(t *testing.T) {
		m.Del(bucketName, key)
		time.Sleep(250 * time.Millisecond)
		ok := m.Has(bucketName, key)
		if ok {
			t.Error("Key should not be exist")
		}
	})
}

func TestSetGetConcurrent(t *testing.T) {
	t.Parallel()
	m := NewMap(32)
	bucketName := "test"
	val := []byte{'a'}
	m.SetBucket(bucketName)

	t.Run("Set", func(t *testing.T) {
		wg := sync.WaitGroup{}
		wg.Add(10)
		for i := 0; i < 10; i++ {
			go func(i int) {
				k := strconv.Itoa(i)
				go m.Set(bucketName, k, val)
				go m.Set(bucketName, k, val)
				wg.Done()
			}(i)
		}
		wg.Wait()
	})

	t.Run("Get", func(t *testing.T) {
		wg := sync.WaitGroup{}
		wg.Add(10)
		errs := make(chan error)
		go func() {
			for i := 0; i < 10; i++ {
				go func(i int) {
					defer wg.Done()
					k := strconv.Itoa(i)
					ok := m.Has(bucketName, k)
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

func TestIterator(t *testing.T) {
	t.Parallel()
	m := NewMap(32)
	bucketName := "test"
	records := Records{
		"key1": []byte{'a'},
		"key2": []byte{'b'},
	}
	m.SetBucket(bucketName)
	for k, v := range records {
		m.Set(bucketName, k, v)
	}
	ch := m.MapKeysIterator(bucketName)
	counter := 0
	for k := range ch {
		if _, ok := records[k]; !ok {
			t.Error("Key is not in map")
		}
		counter++
	}
	if counter != 2 {
		t.Error("Iterator should return two keys")
	}
	m.DelBucket(bucketName)
	time.Sleep(250 * time.Millisecond)
	ch = m.MapKeysIterator(bucketName)
	_, ok := <-ch
	if ok {
		t.Error("Iterator should be empty")
	}
}

func TestDumpLoad(t *testing.T) {
	t.Parallel()
	path := "/tmp/pure-kv-db-core-test"
	defer os.RemoveAll(path)
	m := NewMap(32)
	bucketName := "test"
	m.SetBucket(bucketName)
	m.Set(bucketName, "key1", []byte{'a'})

	t.Run("Dump", func(t *testing.T) {
		err := m.Dump(path)
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("Load", func(t *testing.T) {
		m2 := NewMap(32)
		err := m2.Load(path)
		if err != nil {
			t.Error(err)
		}
		_, ok := m2.Get(bucketName, "key1")
		if !ok {
			t.Error("Key must be in map after load")
		}
	})

}

func TestPureKvBuckets(t *testing.T) {
	t.Parallel()
	pkv := NewPureKv(32)
	req := Request{Bucket: "test"}
	req.Key = "key"

	t.Run("Create", func(t *testing.T) {
		resp := &Response{}
		err := pkv.Create(req, resp)
		if !resp.Ok || err != nil {
			t.Errorf("Bucket has not been created: %v", err)
		}
	})

	t.Run("Destroy", func(t *testing.T) {
		resp := &Response{}
		err := pkv.Destroy(req, resp)
		time.Sleep(250 * time.Millisecond)
		if !resp.Ok || err != nil {
			t.Errorf("Bucket has not been deleted: %v", err)
		}
	})

	t.Run("MakeIterator", func(t *testing.T) {
		resp := &Response{}
		err := pkv.MakeIterator(req, resp)
		if !(!resp.Ok && err != nil) {
			t.Error("Bucket must not exist yet!")
		}
	})

	t.Run("Size", func(t *testing.T) {
		resp := &Response{}
		err := pkv.Size(req, resp)
		size := binary.LittleEndian.Uint64(resp.Value)
		if !resp.Ok || err != nil || size != 0 {
			t.Error("Bucket must be empty")
		}
	})
}

func TestPureKvSetGet(t *testing.T) {
	t.Parallel()
	pkv := NewPureKv(32)
	req := Request{Bucket: "test"}
	req.Key = "key"
	req.Value = []byte{'a'}
	resp := &Response{}
	pkv.Create(req, resp)

	t.Run("Set", func(t *testing.T) {
		err := pkv.Set(req, resp)
		if !resp.Ok || err != nil {
			t.Errorf("Can't set the value: %v", err)
		}
	})

	t.Run("Get", func(t *testing.T) {
		resp := &Response{}
		err := pkv.Get(req, resp)
		if !resp.Ok || err != nil {
			t.Errorf("Can't get the value: %v", err)
		}
		if bytes.Compare(resp.Value, req.Value) != 0 {
			t.Error("Value is not valid")
		}
	})

	t.Run("Size", func(t *testing.T) {
		resp := &Response{}
		err := pkv.Size(req, resp)
		size := binary.LittleEndian.Uint64(resp.Value)
		if !resp.Ok || err != nil || size != 1 {
			t.Error("Map must contain a single object")
		}
	})

	t.Run("Del", func(t *testing.T) {
		resp := &Response{}
		err := pkv.Del(req, resp)
		time.Sleep(250 * time.Millisecond)
		if !resp.Ok || err != nil {
			t.Errorf("Can't delete the value: %v", err)
		}
	})

	t.Run("GetAfterDel", func(t *testing.T) {
		resp := &Response{}
		err := pkv.Get(req, resp)
		if !(!resp.Ok && err != nil) {
			t.Errorf("Error getting deleted value: %v", err)
		}
	})
}

func TestPureKvIter(t *testing.T) {
	t.Parallel()
	pkv := NewPureKv(32)
	reqs := make([]Request, 2)
	reqs[0].Bucket = "test"
	reqs[1].Bucket = "test"
	reqs[0].Key = "key1"
	reqs[1].Key = "key2"
	reqs[0].Value = []byte{'a'}
	reqs[1].Value = []byte{'b'}
	resp := &Response{}
	pkv.Create(reqs[0], resp)
	for _, req := range reqs {
		pkv.Set(req, resp)
	}
	resp = &Response{}
	err := pkv.MakeIterator(reqs[0], resp)
	if !resp.Ok || err != nil {
		t.Errorf("Can't create iterator: %v", err)
	}
	for _, req := range reqs {
		resp = &Response{}
		err = pkv.Next(req, resp)
		if !resp.Ok || err != nil {
			t.Errorf("Can't get next element of iterator: %v", err)
		}
	}
	resp = &Response{}
	err = pkv.Next(reqs[0], resp)
	if !(!resp.Ok || err != nil) {
		t.Errorf("Iterator must be finished: %v", err)
	}
}
