package client

import (
	"context"
	"errors"
	"net/rpc"
	"pure-kv-go/core"
	"time"
)

// Client holds client connection
type Client struct {
	client  *rpc.Client
	timeout time.Duration
}

// InitPureKvClient returns initialized rpc client
func InitPureKvClient(address string, timeout uint) (*Client, error) {
	c, err := rpc.Dial("tcp", address)
	if err != nil {
		return nil, err
	}
	pureKvClient := &Client{
		client:  c,
		timeout: time.Duration(timeout) * time.Millisecond,
	}
	return pureKvClient, nil
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

// Create makes RPC for creating the new map on the server
func (c *Client) Create(bucketName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	request := &core.Request{
		Bucket: bucketName,
	}
	resp := c.executeWrapper(ctx, core.Create, request)
	if resp == nil {
		return errors.New("unable to create new bucket")
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
		return errors.New("unable to delete the bucket")
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
		return errors.New("unable to delete key")
	}
	return nil
}

// Set makes RPC for creating the new key value pair in specified bucket
func (c *Client) Set(bucketName, key string, val []byte) error {
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
		return errors.New("unable to set the key-value pair")
	}
	return nil
}

// Get makes RPC that returns value by key from one of the buckets
func (c *Client) Get(bucketName, key string) ([]byte, error) {
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
		return nil, errors.New("unable to set the key-value pair")
	}
	return resp.Value, nil
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
		return errors.New("unable to set the key-value pair")
	}
	return nil
}

// Next makes RPC that returns the next key-value pair according to the iterator state
func (c *Client) Next(bucketName string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	request := &core.Request{
		Bucket: bucketName,
	}
	resp := c.executeWrapper(ctx, core.Next, request)
	if resp == nil {
		return nil, errors.New("unable to set the key-value pair")
	}
	return resp.Value, nil
}
