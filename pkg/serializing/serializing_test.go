package serializing

import (
	"os"
	"testing"
)

type testStruct struct {
	A int
	B string
}

type testStructPrivate struct {
	a int
	b string
}

const (
	path = "/tmp/pure-kv-serializing-test"
)

func TestSaveLoad(t *testing.T) {
	defer os.RemoveAll(path)
	data := testStruct{A: 1, B: "bar"}
	err := SerializeAndSave(path, data)
	if err != nil {
		t.Error(err)
	}
	var loaded testStruct
	err = LoadAndDeserialize(path, &loaded)
	if err != nil {
		t.Error(err)
	}
}

func TestSaveLoadErr(t *testing.T) {
	defer os.RemoveAll(path)
	data := testStructPrivate{a: 1, b: "bar"}
	err := SerializeAndSave(path, data)
	if err == nil {
		t.Error("serializing struct with private fields only should fail")
	}
	var deserialized testStruct
	err = Deserialize([]byte{}, &deserialized)
	if err == nil {
		t.Error("deserializing empty byte array should fail")
	}
}
