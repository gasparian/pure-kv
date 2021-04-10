package client

import (
	"bytes"
	"errors"
	"github.com/gasparian/pure-kv-go/server"
	"io/ioutil"
	"os"
	"sync"
	"testing"
	"time"
)

const (
	path = "/tmp/pure-kv-db-client-test"
)

func prepareServer(t *testing.T) func() error {
	srv := server.InitServer(
		6668, // port
		2,    // persistence timeout sec.
		32,   // number of shards for concurrent map
		path, // db path
	)
	go srv.Run()

	return srv.Close
}

func TestClient(t *testing.T) {
	defer os.RemoveAll(path)
	defer prepareServer(t)()
	time.Sleep(5 * time.Second) // just wait for server to be started

	cli, err := InitPureKvClient("0.0.0.0:6668", uint(5))
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	bucketName := "test"
	keys := []string{
		"key1", "key2",
	}
	valSet := []byte{'a'}

	t.Run("CreateBucket", func(t *testing.T) {
		err := cli.Create(bucketName)
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("SetVal", func(t *testing.T) {
		err := cli.Set(bucketName, keys[0], valSet)
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("Size", func(t *testing.T) {
		bucketSize, err := cli.Size(bucketName)
		if err != nil || bucketSize != 1 {
			t.Error("Must contain a single record")
		}
		size, err := cli.Size("")
		if err != nil || size != bucketSize {
			t.Error("Total size must be equal to bucket size")
		}
	})

	t.Run("ConcurrentSet", func(t *testing.T) {
		errs := make(chan error)
		go func() {
			wg := sync.WaitGroup{}
			wg.Add(10)
			for i := 0; i < 10; i++ {
				go func() {
					defer wg.Done()
					err := cli.Set(bucketName, keys[0], valSet)
					if err != nil {
						errs <- err
					}
				}()
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

	t.Run("GetVal", func(t *testing.T) {
		val, ok := cli.Get(bucketName, keys[0])
		if !ok || bytes.Compare(val, valSet) != 0 {
			t.Error("Can't get the value from map")
		}
	})

	t.Run("ConcurrentGet", func(t *testing.T) {
		errs := make(chan error)
		go func() {
			wg := sync.WaitGroup{}
			wg.Add(10)
			for i := 0; i < 10; i++ {
				go func() {
					defer wg.Done()
					val, ok := cli.Get(bucketName, keys[0])
					if !ok || bytes.Compare(val, valSet) != 0 {
						errs <- errors.New("Can't get the value from map")
						return
					}
				}()
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

	t.Run("MakeIterator", func(t *testing.T) {
		cli.Set(bucketName, keys[1], valSet)
		err := cli.MakeIterator(bucketName)
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("GetNext", func(t *testing.T) {
		for range keys {
			_, val, err := cli.Next(bucketName)
			if err != nil || bytes.Compare(val, valSet) != 0 {
				t.Error("Can't get the value from map iterator")
			}
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
		err = cli.Del(bucketName, keys[0])
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("GetValAfterDel", func(t *testing.T) {
		time.Sleep(250 * time.Millisecond)
		_, ok := cli.Get(bucketName, keys[0])
		if ok {
			t.Error("Value should be deleted")
		}
	})

	t.Run("DeleteBucket", func(t *testing.T) {
		err = cli.Destroy(bucketName)
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("GetValAfterDestroy", func(t *testing.T) {
		time.Sleep(250 * time.Millisecond)
		_, ok := cli.Get(bucketName, keys[1])
		if ok {
			t.Error("Value should be deleted")
		}
	})
}
