package client

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"github.com/gasparian/pure-kv-go/core"
	"net/rpc"
	"time"
)

var (
	errCantGetSize         = errors.New("unable to get number of stored elements")
	errCantCreateNewBucket = errors.New("unable to create new bucket")
	errCantDeleteBucket    = errors.New("unable to delete the bucket")
	errCantDeleteKey       = errors.New("unable to delete key")
	errCantSetKeyValuePair = errors.New("unable to set the key-value pair")
	errCantGetValue        = errors.New("unable to get the value")
)

// Client holds client connection
type Client struct {
	client  *rpc.Client
	timeout time.Duration
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
func (c *Client) execute(methodName string, req *core.Request) chan *core.Response {
	respChan := make(chan *core.Response)
	go func() {
		var response = new(core.Response)
		err := c.client.Call(methodName, req, response)
		if err == nil {
			response.Ok = true
		}
		respChan <- response
	}()
	return respChan
}

// executeWrapper wraps execute function into context
func (c *Client) executeWrapper(ctx context.Context, methodName string, req *core.Request) *core.Response {
	respChan := c.execute(methodName, req)
	select {
	case <-ctx.Done():
		return nil
	case response := <-respChan:
		if !response.Ok {
			return nil
		}
		return response
	}
}

// Size requests number of elements in the storage
func (c *Client) Size(bucketName string) (uint64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	request := &core.Request{
		Bucket: bucketName,
	}
	resp := c.executeWrapper(ctx, core.Size, request)
	if resp == nil {
		return 0, errCantGetSize
	}
	size := resp.Value.(uint64)
	return size, nil
}

// Create makes RPC for creating the new map on the server
func (c *Client) Create(bucketName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	request := &core.Request{
		Bucket: bucketName,
	}
	resp := c.executeWrapper(ctx, core.Create, request)
	if resp == nil {
		return errCantCreateNewBucket
	}
	return nil
}

// Destroy makes RPC for deleting the map on the server
func (c *Client) Destroy(bucketName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	request := &core.Request{
		Bucket: bucketName,
	}
	resp := c.executeWrapper(ctx, core.Destroy, request)
	if resp == nil {
		return errCantDeleteBucket
	}
	return nil
}

// DestroyAll makes RPC for deleting entire db
func (c *Client) DestroyAll() error {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	resp := c.executeWrapper(ctx, core.DestroyAll, &core.Request{})
	if resp == nil {
		return errCantDeleteBucket
	}
	return nil
}

// Del makes RPC for droping any record from the bucket by a given key
func (c *Client) Del(bucketName, key string) error {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	request := &core.Request{
		Record: core.Record{
			Key: key,
		},
		Bucket: bucketName,
	}
	resp := c.executeWrapper(ctx, core.Del, request)
	if resp == nil {
		return errCantDeleteKey
	}
	return nil
}

// Set makes RPC for creating the new key value pair in specified bucket
func (c *Client) Set(bucketName, key string, val interface{}) error {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	request := &core.Request{
		Record: core.Record{
			Key:   key,
			Value: val,
		},
		Bucket: bucketName,
	}
	resp := c.executeWrapper(ctx, core.Set, request)
	if resp == nil {
		return errCantSetKeyValuePair
	}
	return nil
}

// Get makes RPC that returns value by key from one of the buckets
func (c *Client) Get(bucketName, key string) (interface{}, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	request := &core.Request{
		Record: core.Record{
			Key: key,
		},
		Bucket: bucketName,
	}
	resp := c.executeWrapper(ctx, core.Get, request)
	if resp == nil {
		return nil, false
	}
	return resp.Value, true
}

// MakeIterator makes RPC for creating the new map iterator based on channel
func (c *Client) MakeIterator(bucketName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	request := &core.Request{
		Bucket: bucketName,
	}
	resp := c.executeWrapper(ctx, core.MakeIter, request)
	if resp == nil {
		return errCantSetKeyValuePair
	}
	return nil
}

// Next makes RPC that returns the next key-value pair according to the iterator state
func (c *Client) Next(bucketName string) (string, interface{}, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	request := &core.Request{
		Bucket: bucketName,
	}
	resp := c.executeWrapper(ctx, core.Next, request)
	if resp == nil {
		return "", nil, errCantGetValue
	}
	return resp.Key, resp.Value, nil
}
