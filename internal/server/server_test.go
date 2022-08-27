package server

import (
	"bytes"
	"encoding/binary"
	"net/rpc"
	"os"
	"testing"
	"time"
)

func TestPureKvSetGet(t *testing.T) {
	t.Parallel()
	pkv := NewPureKvRPC(32, 1000)

	t.Run("Set", func(t *testing.T) {
		req := PayLoad{
			Key:   "key",
			Value: []byte{'a'},
			TTL:   100,
		}
		resp := &PayLoad{}
		err := pkv.Set(req, resp)
		if err != nil {
			t.Error(err)
		}
		if !resp.Ok {
			t.Error("Can't set the value")
		}
	})

	t.Run("Has", func(t *testing.T) {
		req := PayLoad{
			Key:   "key2",
			Value: []byte{},
		}
		resp := &PayLoad{}
		err := pkv.Set(req, resp)
		if err != nil {
			t.Error(err)
		}
		resp = &PayLoad{}
		err = pkv.Has(req, resp)
		if err != nil {
			t.Error(err)
		}
		if !resp.Ok {
			t.Error("key should be presented")
		}
	})

	t.Run("Get", func(t *testing.T) {
		val := []byte{'b'}
		req := PayLoad{
			Key:   "key3",
			Value: val,
		}
		resp := &PayLoad{}
		err := pkv.Set(req, resp)
		if err != nil {
			t.Error(err)
		}
		resp = &PayLoad{}
		err = pkv.Get(req, resp)
		if err != nil {
			t.Error(err)
		}
		if !resp.Ok {
			t.Error("Can't get the value")
		}
		if !bytes.Equal(resp.Value, val) {
			t.Error("Value is not valid")
		}
	})

	t.Run("Size", func(t *testing.T) {
		req := PayLoad{
			Key:   "key2",
			Value: []byte{},
		}
		resp := &PayLoad{}
		pkv.Set(req, resp)
		resp = &PayLoad{}
		err := pkv.Size(req, resp)
		if err != nil {
			t.Error(err)
		}
		var size uint64
		buf := bytes.NewReader(resp.Value)
		err = binary.Read(buf, binary.LittleEndian, &size)
		if err != nil {
			t.Error(err)
		}
		if !resp.Ok || err != nil || size == 0 {
			t.Error("Map must contain at least single object")
		}
	})

	t.Run("Del", func(t *testing.T) {
		req := PayLoad{
			Key: "key3",
		}
		resp := &PayLoad{}
		err := pkv.Del(req, resp)
		if err != nil {
			t.Error(err)
		}
		if !resp.Ok {
			t.Errorf("Can't delete the value")
		}
		resp = &PayLoad{}
		err = pkv.Get(req, resp)
		if err != nil {
			t.Error(err)
		}
		if resp.Ok {
			t.Errorf("Value should be deleted")
		}
	})

	t.Run("DelAll", func(t *testing.T) {
		req := PayLoad{
			Key:   "key4",
			Value: []byte{'c'},
		}
		resp := &PayLoad{}
		pkv.Set(req, resp)
		resp = &PayLoad{}
		err := pkv.DelAll(req, resp)
		if err != nil {
			t.Error(err)
		}
		if !resp.Ok {
			t.Errorf("Can't delete the value")
		}
		resp = &PayLoad{}
		err = pkv.Get(req, resp)
		if err != nil {
			t.Error(err)
		}
		if resp.Ok {
			t.Errorf("Value should be deleted")
		}
	})
}

func TestServer(t *testing.T) {
	path := "/tmp/pure-kv-db-server-test"
	srv := InitServer(
		6666,
		32,
		60000,
	)
	defer func() {
		err := srv.Stop()
		if err != nil {
			t.Error(err)
		}
		err = os.RemoveAll(path)
		if err != nil {
			t.Error(err)
		}
		t.Log("Server listener closed, artefacts deleted")
	}()

	req := PayLoad{}
	req.Key = "key"
	req.Value = []byte{'a'}

	t.Run("PopulateDb", func(t *testing.T) {
		resp := &PayLoad{}
		err := srv.db.Set(req, resp)
		if err != nil || !resp.Ok {
			t.Error("Can't set the value")
		}
	})

	t.Run("StartStopRpc", func(t *testing.T) {
		go srv.Start()
		time.Sleep(1 * time.Second)
		c, err := rpc.Dial("tcp", "0.0.0.0:6666")
		if err != nil {
			t.Error(err)
		}
		resp := &PayLoad{}
		err = c.Call("PureKvRPC.Size", req, resp)
		if err != nil {
			t.Error(err)
		}
		var size uint64
		buf := bytes.NewReader(resp.Value)
		err = binary.Read(buf, binary.LittleEndian, &size)
		if err != nil {
			t.Error(err)
		}
		if size != 1 {
			t.Error("Must contain a single element")
		}
	})
}
