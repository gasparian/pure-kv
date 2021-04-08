package core

import (
	"bytes"
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
	ok := m.HasBucket(bucketName)
	if !ok {
		t.Error("Bucket `test` should exist")
	}
	m.DelBucket(bucketName)
	time.Sleep(250 * time.Millisecond)
	ok = m.HasBucket(bucketName)
	if ok {
		t.Error("Bucket `test` should not be exist")
	}
}

func TestSetGet(t *testing.T) {
	t.Parallel()
	m := NewMap(32)
	bucketName := "test"
	key := "key"
	val := []byte{'a'}
	m.SetBucket(bucketName)
	m.Set(bucketName, key, val)
	ok := m.Has(bucketName, key)
	if !ok {
		t.Error("Key should exist")
	}
	valGet, ok := m.Get(bucketName, key)
	if !ok {
		t.Error("Key should exist")
	}
	res := bytes.Compare(val, valGet)
	if res != 0 {
		t.Error("Values should be equal")
	}
	m.Del(bucketName, key)
	time.Sleep(250 * time.Millisecond)
	ok = m.Has(bucketName, key)
	if ok {
		t.Error("Key should not be exist")
	}
}

func TestSetGetConcurrent(t *testing.T) {
	t.Parallel()
	m := NewMap(32)
	bucketName := "test"
	val := []byte{'a'}
	m.SetBucket(bucketName)
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

	errs := make(chan error)
	go func() {
		wg.Add(10)
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
	path := "/tmp/pure-kv-go"
	defer os.RemoveAll(path)
	m := NewMap(32)
	bucketName := "test"
	m.SetBucket(bucketName)
	m.Set(bucketName, "key1", []byte{'a'})
	err := m.Dump(path)
	if err != nil {
		t.Error(err)
	}
	m2 := NewMap(32)
	err = m2.Load(path)
	if err != nil {
		t.Error(err)
	}
	_, ok := m2.Get(bucketName, "key1")
	if !ok {
		t.Error("Key is not not in map after load")
	}
}

func TestPureKvBuckets(t *testing.T) {
	t.Parallel()
	pkv := NewPureKv(32)
	req := Request{Bucket: "test"}
	req.Key = "key"
	resp := &Response{}
	err := pkv.Create(req, resp)
	if !resp.Ok || err != nil {
		t.Errorf("Bucket has not been created: %v", err)
	}
	resp = &Response{}
	err = pkv.Destroy(req, resp)
	time.Sleep(250 * time.Millisecond)
	if !resp.Ok || err != nil {
		t.Errorf("Bucket has not been deleted: %v", err)
	}
	resp = &Response{}
	err = pkv.MakeIterator(req, resp)
	if !(!resp.Ok && err != nil) {
		t.Error("Bucket is not exist, should be an error here!")
	}
}

func TestPureKvSetGet(t *testing.T) {
	t.Parallel()
	pkv := NewPureKv(32)
	req := Request{Bucket: "test"}
	req.Key = "key"
	req.Value = []byte{'a'}
	resp := &Response{}
	pkv.Create(req, resp)
	err := pkv.Set(req, resp)
	if !resp.Ok || err != nil {
		t.Errorf("Can't set the value: %v", err)
	}
	resp = &Response{}
	err = pkv.Get(req, resp)
	if !resp.Ok || err != nil {
		t.Errorf("Can't get the value: %v", err)
	}
	if bytes.Compare(resp.Value, req.Value) != 0 {
		t.Error("Value is not valid")
	}
	resp = &Response{}
	err = pkv.Del(req, resp)
	time.Sleep(250 * time.Millisecond)
	if !resp.Ok || err != nil {
		t.Errorf("Can't delete the value: %v", err)
	}
	resp = &Response{}
	err = pkv.Get(req, resp)
	if !(!resp.Ok && err != nil) {
		t.Errorf("Error getting deleted value: %v", err)
	}
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
