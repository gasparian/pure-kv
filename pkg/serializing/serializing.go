package serializing

import (
	"bytes"
	"encoding/gob"
	"io"
	"os"
)

// Serialize encodes input object as a byte array
func Serialize[T any](obj T) ([]byte, error) {
	buf := &bytes.Buffer{}
	enc := gob.NewEncoder(buf)
	err := enc.Encode(obj)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Deserialize converts byte array to object
func Deserialize[T any](inp []byte, obj T) error {
	buf := &bytes.Buffer{}
	buf.Write(inp)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(obj)
	if err != nil {
		return err
	}
	return nil
}

// Save dumps data to disk
func Save(path string, data []byte) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Write(data); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	return nil
}

// Load loads byte array from file
func Load(path string) ([]byte, error) {
	buf := &bytes.Buffer{}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	_, err = io.Copy(buf, f)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// SerializeAndSave serializes object, which should be a struct with
// at least one public field, to a byte array and dumps it to disk
func SerializeAndSave[T any](path string, obj T) error {
	serialized, err := Serialize(obj)
	if err != nil {
		return err
	}
	err = Save(path, serialized)
	return err
}

// LoadAndDeserialize loads byte array from disk and deserializes
// it to the provided instantiated object
func LoadAndDeserialize[T any](path string, obj T) error {
	byteArr, err := Load(path)
	if err != nil {
		return err
	}
	err = Deserialize(byteArr, obj)
	return err
}
