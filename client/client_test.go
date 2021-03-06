package client

import (
	"bytes"
	"errors"
	"github.com/gasparian/pure-kv/server"
	"io/ioutil"
	"os"
	"sync"
	"testing"
	"time"
)

const (
	path = "/tmp/pure-kv-db-client-test"
)

var (
	getValueErr = errors.New("Can't get the value from map")
)

type SomeCustomType struct {
	Key   string
	Value map[string]bool
}

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

func TestSerializers(t *testing.T) {
	t.Parallel()
	emptyObj := &SomeCustomType{}
	origObj := &SomeCustomType{
		Key: "key",
		Value: map[string]bool{
			"a": true,
		},
	}
	duplicateObj := &SomeCustomType{}

	serializedEmpty, err := Serialize(emptyObj)
	serialized, err := Serialize(origObj)
	if err != nil {
		t.Fatal(err)
	}
	if len(serialized) == len(serializedEmpty) {
		t.Fatal("Serialized object can't be equal to serialized empty struct")
	}

	err = Deserialize(serialized, duplicateObj)
	if err != nil {
		t.Fatal(err)
	}
	if duplicateObj.Key != origObj.Key || len(duplicateObj.Value) != len(origObj.Value) {
		t.Fatal("Desirialized object must be equal to the original one")
	}
}

func TestClient(t *testing.T) {
	t.Parallel()
	defer os.RemoveAll(path)
	defer prepareServer(t)()
	time.Sleep(1 * time.Second) // just wait for server to be started

	cli := New("0.0.0.0:6668", 1000)
	err := cli.Open()
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
		N := 10
		errs := make(chan error, N)
		wg := sync.WaitGroup{}
		wg.Add(10)
		for i := 0; i < N; i++ {
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
		for err := range errs {
			if err != nil {
				t.Error(err)
			}
		}
	})

	t.Run("GetVal", func(t *testing.T) {
		tmpVal, ok := cli.Get(bucketName, keys[0])
		if !ok {
			t.Fatal(getValueErr)
		}
		val := tmpVal.([]byte)
		if bytes.Compare(val, valSet) != 0 {
			t.Error(getValueErr)
		}
	})

	t.Run("ConcurrentGet", func(t *testing.T) {
		N := 10
		errs := make(chan error, N)
		wg := sync.WaitGroup{}
		wg.Add(N)
		for i := 0; i < N; i++ {
			go func() {
				defer wg.Done()
				tmpVal, ok := cli.Get(bucketName, keys[0])
				if !ok {
					errs <- getValueErr
					return
				}
				val := tmpVal.([]byte)
				if bytes.Compare(val, valSet) != 0 {
					errs <- getValueErr
					return
				}
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatal(err)
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
			_, tmpVal, err := cli.Next(bucketName)
			val := tmpVal.([]byte)
			if err != nil || bytes.Compare(val, valSet) != 0 {
				t.Error("Can't get the value from map iterator")
			}
		}
		_, tmpVal, _ := cli.Next(bucketName)
		if tmpVal != nil {
			t.Error("Iterator must be closed")
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
		time.Sleep(250 * time.Millisecond)
		_, ok := cli.Get(bucketName, keys[0])
		if ok {
			t.Error("Value should be deleted")
		}
	})

	t.Run("Destroy", func(t *testing.T) {
		err = cli.Destroy(bucketName)
		if err != nil {
			t.Error(err)
		}
		time.Sleep(250 * time.Millisecond)
		_, ok := cli.Get(bucketName, keys[1])
		if ok {
			t.Error("Value should be deleted")
		}
	})

	t.Run("DestroyAll", func(t *testing.T) {
		err = cli.Create(bucketName)
		if err != nil {
			t.Fatal(err)
		}
		cli.Set(bucketName, keys[0], valSet)
		err = cli.DestroyAll()
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(250 * time.Millisecond)
		_, ok := cli.Get(bucketName, keys[0])
		if ok {
			t.Error("Value should be deleted")
		}
	})
}
