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

### Usage  

Client:  
```go
import (
    pkv "github.com/gasparian/pure-kv-go"
)

// creates client instance by providing server address and timetout in sec. 
cli, err := pkv.client.InitPureKvClient("0.0.0.0:6666", uint(30))
defer cli.Close() 
// creates the new bucket with specified key-value pair type
err = cli.Create("BucketName") 
// creates new key-value pair in the specified bucket
err = cli.Set("BucketName", "someKey", []byte{'a'}) 
// returns decoded value
val, ok := cli.Get("BucketName", "someKey") 
// makes new iterator for specified bucket
err = cli.MakeIterator("BucketName")
// get next element of bucket
k, val, err := cli.Next("BucketName") 
// async. delete value from the bucket
err = cli.Del("BucketName", "someKey") 
// async. delete the specified bucket
err = cli.Destroy("BucketName") 
```  

[Server:](https://github.com/gasparian/pure-kv-go/blob/main/main.go)  
```go

import (
    pkv "github.com/gasparian/pure-kv-go"
)

func main() {
    flag.Parse()
    srv := server.InitServer(
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
./test.sh {PACKAGE}
```  

Optionally you can run benchmarks for the concurrent map:  
```
./bench.sh
./test_race.sh
./bench_trace.sh
```  
