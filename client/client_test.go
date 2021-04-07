package client

import (
	"bytes"
	"io/ioutil"
	"os"
	"pure-kv-go/server"
	"testing"
	"time"
)

const (
	path = "/tmp/pure-kv-db"
)

func prepareServer(t *testing.T) func() error {
	srv := server.InitServer(
		6666, // port
		2,    // persistence timeout sec.
		32,   // number of shards for concurrent map
		path, // db path
	)
	go srv.Run()

	return srv.Close
}

func TestClient(t *testing.T) {
	defer prepareServer(t)()
	defer os.RemoveAll(path)
	time.Sleep(5 * time.Second) // just wait for server to be started

	cli, err := InitPureKvClient("0.0.0.0:6666", uint(5))
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	valSet := []byte{'a'}
	bucketName := "test"
	key := "key"

	t.Run("CreateBucket", func(t *testing.T) {
		err := cli.Create(bucketName)
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("SetVal", func(t *testing.T) {
		err := cli.Set(bucketName, key, valSet)
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("GetVal", func(t *testing.T) {
		val, ok := cli.Get(bucketName, key)
		if !ok || bytes.Compare(val, valSet) != 0 {
			t.Error("Can't get the value from map")
		}
	})

	t.Run("MakeIterator", func(t *testing.T) {
		err := cli.MakeIterator(bucketName)
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("GetIterator", func(t *testing.T) {
		k, val, err := cli.Next(bucketName)
		if err != nil || bytes.Compare(val, valSet) != 0 || k != key {
			t.Error("Can't get the value from map iterator")
		}
	})

	t.Run("CheckPersistance", func(t *testing.T) {
		time.Sleep(2 * time.Second)
		files, err := ioutil.ReadDir(path)
		if err != nil {
			t.Error(err)
		}
		if len(files) == 0 {
			t.Error("Can't find the db dump")
		}
	})

	t.Run("DeleteVal", func(t *testing.T) {
		err = cli.Del(bucketName, key)
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("DeleteBucket", func(t *testing.T) {
		err = cli.Destroy(bucketName)
		if err != nil {
			t.Error(err)
		}
	})
}
