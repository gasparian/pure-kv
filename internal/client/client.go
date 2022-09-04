package client

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"net/rpc"
	"time"

	"github.com/gasparian/pure-kv/internal/server"
)

var (
	errCantGetSize         = errors.New("unable to get number of stored elements")
	errCantDeleteKey       = errors.New("unable to delete key")
	errCantDeleteAll       = errors.New("unable to clean the store")
	errCantSetKeyValuePair = errors.New("unable to set the key-value pair")
)

// Client holds client connection
type Client struct {
	timeout time.Duration
	client  *rpc.Client
	address string
}

// Serialize dumps object to byte array via gob encoder
func Serialize(obj interface{}) ([]byte, error) {
	buf := &bytes.Buffer{}
	enc := gob.NewEncoder(buf)
	err := enc.Encode(obj)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Deserialize decodes recieved gob encoded data and writes to input object by reference
func Deserialize(inp []byte, obj interface{}) error {
	buf := &bytes.Buffer{}
	buf.Write(inp)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(obj)
	if err != nil {
		return err
	}
	return nil
}

// New instantiates rpc client
func New(address string, timeout int) *Client {
	return &Client{
		address: address,
		timeout: time.Duration(timeout) * time.Millisecond,
	}
}

// Open creates connection to the rpc server
func (c *Client) Open() error {
	client, err := rpc.Dial("tcp", c.address)
	if err != nil {
		return err
	}
	c.client = client
	return nil
}

// Close terminates the underlying client
func (c *Client) Close() error {
	if c.client != nil {
		err := c.client.Close()
		return err
	}
	return nil
}

// execute runs rpc any given function by it's name
func (c *Client) execute(methodName string, req *server.PayLoad) chan *server.PayLoad {
	respChan := make(chan *server.PayLoad)
	go func() {
		response := new(server.PayLoad)
		err := c.client.Call(methodName, req, response)
		if err != nil {
			response.Ok = false
		}
		respChan <- response
	}()
	return respChan
}

// executeWrapper wraps execute function into context
func (c *Client) executeWrapper(methodName string, req *server.PayLoad) *server.PayLoad {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	respChan := c.execute(methodName, req)
	select {
	case <-ctx.Done():
		return &server.PayLoad{}
	case response := <-respChan:
		return response
	}
}

// Size requests number of elements in the storage
func (c *Client) Size() (uint64, error) {
	request := &server.PayLoad{}
	resp := c.executeWrapper(server.Size, request)
	if !resp.Ok {
		return 0, errCantGetSize
	}
	var size uint64
	buf := bytes.NewReader(resp.Value)
	err := binary.Read(buf, binary.LittleEndian, &size)
	if err != nil {
		return 0, err
	}
	return size, nil
}

// Del makes RPC for deleting any record from the bucket by a given key
func (c *Client) Del(key string) error {
	request := &server.PayLoad{
		Key: key,
	}
	resp := c.executeWrapper(server.Del, request)
	if !resp.Ok {
		return errCantDeleteKey
	}
	return nil
}

// DestroyAll makes RPC for deleting entire db
func (c *Client) DelAll() error {
	resp := c.executeWrapper(server.DelAll, &server.PayLoad{})
	if !resp.Ok {
		return errCantDeleteAll
	}
	return nil
}

// Set makes RPC for creating the new key value pair in specified bucket
func (c *Client) Set(key string, val []byte, ttl int) error {
	request := &server.PayLoad{
		Key:   key,
		Value: val,
		TTL:   ttl,
	}
	resp := c.executeWrapper(server.Set, request)
	if !resp.Ok {
		return errCantSetKeyValuePair
	}
	return nil
}

// Get makes RPC that returns value by key from one of the buckets
func (c *Client) Has(key string) bool {
	request := &server.PayLoad{
		Key: key,
	}
	resp := c.executeWrapper(server.Has, request)
	return resp.Ok
}

// Get makes RPC that returns value by key from one of the buckets
func (c *Client) Get(key string) ([]byte, bool) {
	request := &server.PayLoad{
		Key: key,
	}
	resp := c.executeWrapper(server.Get, request)
	if !resp.Ok {
		return nil, false
	}
	return resp.Value, true
}
