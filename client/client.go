package client

import (
	"context"
	"errors"
	"net/rpc"
	"pure-kv-go/core"
	"time"
)

// PureKvClient holds client connection
type PureKvClient struct {
	client *rpc.Client
}

// InitPureKvClient returns initialized rpc client
func InitPureKvClient(address string) (*PureKvClient, error) {
	c, err := rpc.Dial("tcp", address)
	if err != nil {
		return nil, err
	}
	pureKvClient := &PureKvClient{
		client: c,
	}
	return pureKvClient, nil
}

// Close gracefully terminates the underlying client
func (c *PureKvClient) Close() error {
	if c.client != nil {
		err := c.client.Close()
		return err
	}
	return nil
}

// execute runs rpc any given function by it's name
func (c *PureKvClient) execute(methodName string, req *core.Request) chan *core.Response {
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
func (c *PureKvClient) executeWrapper(ctx context.Context, methodName string, req *core.Request) *core.Response {
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

// Create
func (c *PureKvClient) Create(bucketName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(10)*time.Millisecond)
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
