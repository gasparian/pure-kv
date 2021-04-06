package core_test

import (
	"bytes"
	"os"
	"pure-kv-go/core"
	"testing"
	"time"
)

func TestBucket(t *testing.T) {
	m := core.NewMap(32)
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
	m := core.NewMap(32)
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

func TestIterator(t *testing.T) {
	m := core.NewMap(32)
	bucketName := "test"
	records := core.Records{
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
	path := "/tmp/pure-kv-go"
	defer os.RemoveAll(path)
	m := core.NewMap(32)
	bucketName := "test"
	m.SetBucket(bucketName)
	m.Set(bucketName, "key1", []byte{'a'})
	err := m.Dump(path)
	if err != nil {
		t.Error(err)
	}
	m2 := core.NewMap(32)
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
	pkv := core.NewPureKv(32)
	req := core.Request{Bucket: "test"}
	req.Key = "key"
	resp := &core.Response{}
	err := pkv.Create(req, resp)
	if !resp.Ok || err != nil {
		t.Errorf("Bucket has not been created: %v", err)
	}
	resp = &core.Response{}
	err = pkv.Destroy(req, resp)
	time.Sleep(250 * time.Millisecond)
	if !resp.Ok || err != nil {
		t.Errorf("Bucket has not been deleted: %v", err)
	}
	resp = &core.Response{}
	err = pkv.MakeIterator(req, resp)
	if !(!resp.Ok && err != nil) {
		t.Error("Bucket is not exist, should be an error here!")
	}
}

func TestPureKvSetGet(t *testing.T) {
	pkv := core.NewPureKv(32)
	req := core.Request{Bucket: "test"}
	req.Key = "key"
	req.Value = []byte{'a'}
	resp := &core.Response{}
	pkv.Create(req, resp)
	err := pkv.Set(req, resp)
	if !resp.Ok || err != nil {
		t.Errorf("Can't set the value: %v", err)
	}
	resp = &core.Response{}
	err = pkv.Get(req, resp)
	if !resp.Ok || err != nil {
		t.Errorf("Can't get the value: %v", err)
	}
	if bytes.Compare(resp.Value, req.Value) != 0 {
		t.Error("Value is not valid")
	}
	resp = &core.Response{}
	err = pkv.Del(req, resp)
	time.Sleep(250 * time.Millisecond)
	if !resp.Ok || err != nil {
		t.Errorf("Can't delete the value: %v", err)
	}
	resp = &core.Response{}
	err = pkv.Get(req, resp)
	if !(!resp.Ok && err != nil) {
		t.Errorf("Error getting deleted value: %v", err)
	}
}

func TestPureKvIter(t *testing.T) {
	pkv := core.NewPureKv(32)
	reqs := make([]core.Request, 2)
	reqs[0].Bucket = "test"
	reqs[1].Bucket = "test"
	reqs[0].Key = "key1"
	reqs[1].Key = "key2"
	reqs[0].Value = []byte{'a'}
	reqs[1].Value = []byte{'b'}
	resp := &core.Response{}
	pkv.Create(reqs[0], resp)
	for _, req := range reqs {
		pkv.Set(req, resp)
	}
	resp = &core.Response{}
	err := pkv.MakeIterator(reqs[0], resp)
	if !resp.Ok || err != nil {
		t.Errorf("Can't create iterator: %v", err)
	}
	for _, req := range reqs {
		resp = &core.Response{}
		err = pkv.Next(req, resp)
		if !resp.Ok || err != nil {
			t.Errorf("Can't get next element of iterator: %v", err)
		}
	}
	resp = &core.Response{}
	err = pkv.Next(reqs[0], resp)
	if !(!resp.Ok || err != nil) {
		t.Errorf("Iterator must be finished: %v", err)
	}
}
