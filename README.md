![main build](https://github.com/gasparian/pure-kv-go/actions/workflows/build.yml/badge.svg?branch=main)
![main tests](https://github.com/gasparian/pure-kv-go/actions/workflows/test.yml/badge.svg?branch=main)

# pure-kv-go  
Simple and fast in-memory key-value storage with RPC interface, written in go.  

<p align="center"> <img src="https://github.com/gasparian/pure-kv-go/blob/main/pics/logo.jpg" height=300/> </p>  

Features:  
 * uses RPC interface;  
 * uses concurrency-efficient map;  
 * stores byte arrays only;  
 * supports buckets so you can organize your storage better;  
 * supports iteration over buckets' contence;  
 * persistant by default;  
 * doesn't depend on third-party libraries;  

### Install  
```
go get github.com/gasparian/pure-kv-go
```  

### Run server  
```
make compile
./pure-kv-srv --port 6666 \
              --persist_time 60 \
              --db_path /tmp/pure-kv-db \
              --shards 32
```  

### Usage  

Client:  
```go
package main

import (
    "log"
    pkv "github.com/gasparian/pure-kv-go/client"
)
 
type SomeCustomType struct {
	Key   string
	Value map[string]bool
}

func main() {
    // creates client instance by providing server address and timetout in ms. 
    cli, err := pkv.InitPureKvClient("0.0.0.0:6666", uint(500))
    defer cli.Close() 
    if err != nil {
        log.Fatal(err)
    }
    // creates the new bucket with specified key-value pair type
    err = cli.Create("BucketName") 
    if err != nil {
        log.Fatal(err)
    }
    // creates new key-value pair in the specified bucket
    err = cli.Set("BucketName", "someKey", []byte{'a'}) 
    if err != nil {
        log.Fatal(err)
    }
    // you can use provided function for serializing any struct
	obj := &SomeCustomType{
		Key: "key",
		Value: map[string]bool{
			"a": true,
		},
	}
    serialized, err := Serialize(obj)
    if err != nil {
        log.Fatal(err)
    }
    err = cli.Set("BucketName", "someKey", serialized) 
    if err != nil {
        log.Fatal(err)
    }
    // returns decoded value
    val, ok := cli.Get("BucketName", "someKey") 
    if !ok {
        log.Fatal("Can't get a value")
    }    
    log.Println(val)
    // or you can desirialize byte array to fill the struct
    duplicateObj := &SomeCustomType{}
	err = Deserialize(val, duplicateObj)
    if err != nil {
        log.Fatal(err)
    }
    log.Println(obj, duplicateObj)
    // returns size of specified bucket
    bucketSize, err := cli.Size("BucketName") 
    if err != nil {
        log.Fatal(err)
    }
    log.Println(bucketSize)
    // makes new iterator for specified bucket
    err = cli.MakeIterator("BucketName")
    if err != nil {
        log.Fatal(err)
    }
    // get next element of bucket
    k, val, err := cli.Next("BucketName") 
    if err != nil {
        log.Fatal(err)
    }
    log.Println(k, val)
    // async. delete value from the bucket
    err = cli.Del("BucketName", "someKey") 
    if err != nil {
        log.Fatal(err)
    }
    // async. delete the specified bucket
    err = cli.Destroy("BucketName") 
    if err != nil {
        log.Fatal(err)
    }
    // returns total number of records in storage
    size, err := cli.Size("")
    if err != nil {
        log.Fatal(err)
    }
    log.Println(size)
}
```  

[Server:](https://github.com/gasparian/pure-kv-go/blob/main/main.go)  
```go
package main

import (
    pkv "github.com/gasparian/pure-kv-go/server"
)

func main() {
    flag.Parse()
    srv := pkv.InitServer(
        6666, // port
        60, // persistence timeout sec.
        32, // number of shards for concurrent map
        "/tmp/pure-kv-db", // db path
    )
    srv.Run() // <-- blocks
}
```  

### Tests  

You can run tests by package or left argument empty to run all the tests:  
```
make test path={PACKAGE}
```  

Optionally you can run benchmarks for the concurrent map:  
```
make map-bench
make map-race-test
```  
