package server

import (
	"encoding/binary"
	"github.com/gasparian/pure-kv-go/core"
	"net/rpc"
	"os"
	"testing"
	"time"
)

func TestServer(t *testing.T) {
	path := "/tmp/pure-kv-db-server-test"
	srv := InitServer(
		6666, // port
		1,    // persistence timeout sec.
		32,   // number of shards for concurrent map
		path, // db path
	)
	defer func() {
		err := srv.Close()
		if err != nil {
			t.Error(err)
		}
		err = os.RemoveAll(path)
		if err != nil {
			t.Error(err)
		}
		t.Log("Server listener closed, artefacts deleted")
	}()

	req := core.Request{Bucket: "test"}
	req.Key = "key"
	req.Value = []byte{'a'}

	t.Run("PopulateDb", func(t *testing.T) {
		resp := &core.Response{}
		err := srv.db.Create(req, resp)
		if err != nil || !resp.Ok {
			t.Error("Can't create a bucket")
		}
		resp = &core.Response{}
		err = srv.db.Set(req, resp)
		if err != nil || !resp.Ok {
			t.Error("Can't set the value")
		}
	})

	t.Run("Persist", func(t *testing.T) {
		go srv.persist()
		time.Sleep(2 * time.Second)
		err := core.CheckDirFiles(path)
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("LoadDb", func(t *testing.T) {
		time.Sleep(1 * time.Second)
		resp := &core.Response{}
		err := srv.db.Del(req, resp)
		if err != nil {
			t.Error(err)
		}
		err = srv.loadDb()
		if err != nil {
			t.Error(err)
		}
		resp = &core.Response{}
		err = srv.db.Get(req, resp)
		if err != nil || !resp.Ok {
			t.Error("Can't find a key")
		}
	})

	t.Run("StartStopRpc", func(t *testing.T) {
		go srv.startRPC()
		time.Sleep(1 * time.Second)
		c, err := rpc.Dial("tcp", "0.0.0.0:6666")
		if err != nil {
			t.Error(err)
		}
		var resp = new(core.Response)
		err = c.Call("PureKv.Size", req, resp)
		if err != nil {
			t.Error(err)
		}
		size := binary.LittleEndian.Uint64(resp.Value)
		if size != 1 {
			t.Error("Must contain a single element")
		}
	})
}
