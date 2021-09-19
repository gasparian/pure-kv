![main build](https://github.com/gasparian/pure-kv/actions/workflows/build.yml/badge.svg?branch=main)
![main tests](https://github.com/gasparian/pure-kv/actions/workflows/test.yml/badge.svg?branch=main)

# pure-kv  
Simple and fast in-memory key-value storage with RPC interface.  

<p align="center"> <img src="https://github.com/gasparian/pure-kv/blob/main/pics/logo.jpg" height=300/> </p>  

Features:  
 * uses RPC interface;  
 * uses concurrency-efficient map (in the end, my implementation becomes pretty similar to [this one](https://github.com/orcaman/concurrent-map), but a bit simpler, I think);  
 * stores objects as empty interfaces;  
 * supports buckets so you can organize your storage better;  
 * supports iteration over buckets' contence;  
 * persistant by default;  
 * doesn't depend on third-party libraries;  

### Install  
```
go get github.com/gasparian/pure-kv
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
    pkv "github.com/gasparian/pure-kv/client"
)

func main() {
    // creates client instance by providing server address and timetout in ms. 
    cli := pkv.New("0.0.0.0:6668", 500)
    err := cli.Open()
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
    // returns value
    tmpVal, ok := cli.Get("BucketName", "someKey") 
    // you cast the result back to original type
    val := tmpVal.([]byte) 
    if !ok {
        log.Fatal("Can't get a value")
    }    
    log.Println(val)
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
    k, tmpVal, err := cli.Next("BucketName") 
    if err != nil {
        log.Fatal(err)
    }
    log.Println(k, tmpVal)
    // returns total number of records in storage
    size, err := cli.Size("")
    if err != nil {
        log.Fatal(err)
    }
    log.Println(size)
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
    // recreates the entire db, deleting everything
    cli.DestroyAll() 
}
```  

[Server:](https://github.com/gasparian/pure-kv/blob/main/main.go)  
```go
package main

import (
    pkv "github.com/gasparian/pure-kv/server"
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

### TODO  
 - add crdt data structures: https://github.com/alangibson/awesome-crdt  
 - http://archagon.net/blog/2018/03/24/data-laced-with-history/  
 - add pub/sub so when structure changes (propagate back to client change from another client)  
 - try lock-free data structures (--> conflict + lock free as a result)
