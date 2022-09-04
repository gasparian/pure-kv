package client

import (
	"bytes"
	"errors"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/gasparian/pure-kv/internal/server"
)

const (
	path         = "/tmp/pure-kv-db-client-test"
	defaultTTLMs = 100
)

var (
	errGetValue = errors.New("can't get the value from map")
)

type SomeCustomType struct {
	Key   string
	Value map[string]bool
}

func prepareServer(t *testing.T) func() error {
	srv := server.InitServer(
		6668,
		32,
		60000,
	)
	go srv.Start()

	return srv.Stop
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
	if err != nil {
		t.Fatal(err)
	}
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

	t.Run("Set", func(t *testing.T) {
		key := "key1"
		val := []byte{'a'}
		err := cli.Set(key, val, defaultTTLMs)
		if err != nil {
			t.Error(err)
		}
		time.Sleep(time.Duration(defaultTTLMs*2) * time.Millisecond)
		ok := cli.Has(key)
		if ok {
			t.Error("key should not exist")
		}
	})

	t.Run("Size", func(t *testing.T) {
		key := "key2"
		val := []byte{'b'}
		cli.Set(key, val, defaultTTLMs)
		size, err := cli.Size()
		if err != nil {
			t.Error(err)
		}
		if size == 0 {
			t.Error("Store size must be greater than 0")
		}
	})

	t.Run("ConcurrentSet", func(t *testing.T) {
		N := 10
		errs := make(chan error, N)
		wg := sync.WaitGroup{}
		wg.Add(10)
		for i := 0; i < N; i++ {
			go func(k int) {
				defer wg.Done()
				key := strconv.Itoa(k)
				err := cli.Set(key, []byte{'c'}, defaultTTLMs*N)
				if err != nil {
					errs <- err
				}
			}(i)
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Error(err)
			}
		}
		for i := 0; i < N; i++ {
			key := strconv.Itoa(i)
			_, ok := cli.Get(key)
			if !ok {
				t.Errorf("key `%v` should be presented\n", key)
			}
		}
	})

	t.Run("Get", func(t *testing.T) {
		key := "key3"
		val := []byte{'c'}
		err := cli.Set(key, val, defaultTTLMs)
		if err != nil {
			t.Error(err)
		}
		valGet, ok := cli.Get(key)
		if !ok {
			t.Fatal(errGetValue)
		}
		if !bytes.Equal(valGet, val) {
			t.Error(errGetValue)
		}
	})

	t.Run("ConcurrentGet", func(t *testing.T) {
		N := 10
		errs := make(chan error, N)
		val := []byte{'d'}
		for i := 0; i < N; i++ {
			key := strconv.Itoa(i) + "_"
			err := cli.Set(key, val, defaultTTLMs*N)
			if err != nil {
				t.Error(err)
			}
		}
		wg := sync.WaitGroup{}
		wg.Add(N)
		for i := 0; i < N; i++ {
			go func(k int) {
				defer wg.Done()
				val := []byte{'d'}
				key := strconv.Itoa(k) + "_"
				valGet, ok := cli.Get(key)
				if !ok {
					errs <- errGetValue
					return
				}
				if !bytes.Equal(valGet, val) {
					errs <- errGetValue
					return
				}
			}(i)
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatal(err)
			}
		}
	})

	t.Run("Del", func(t *testing.T) {
		key := "key4"
		val := []byte{'e'}
		cli.Set(key, val, defaultTTLMs)
		err = cli.Del(key)
		if err != nil {
			t.Error(err)
		}
		ok := cli.Has(key)
		if ok {
			t.Errorf("key `%v` should be deleted", key)
		}
	})

	t.Run("DelAll", func(t *testing.T) {
		key := "key5"
		val := []byte{'e'}
		cli.Set(key, val, defaultTTLMs)
		err = cli.DelAll()
		if err != nil {
			t.Fatal(err)
		}
		ok := cli.Has(key)
		if ok {
			t.Error("all values should be deleted")
		}
	})
}
