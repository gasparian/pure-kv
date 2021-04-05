# pure-kv-go  
Simple and fast in-memory key-value storage with RPC interface, written in go.  

<p align="center"> <img src="https://github.com/gasparian/pure-kv-go/blob/main/pics/logo.jpg" height=300/> </p>  

Has the following properties:  
 * uses RPC interface;  
 * stores byte arrays only;  
 * persistant;  
 * uses concurrency-effective maps;  
 * supports iteration over maps;  
 * no third-party libraries has been used;  

### Reference  

Client:  
```go
import (
    pkv "pure-kv-go"
)

cli, _ := pkv.client.InitPureKvClient("0.0.0.0:8001", uint(30))
// creates the new bucket with specified key-value pair type
cli.Create("BucketName") 
// creates new key-value pair in the specified bucket
cli.Set("BucketName", "someKey", []byte{"a"}) 
// returns decoded value
val, _ := cli.Get("BucketName", "someKey") 
// get next element of bucket
k, val, _ := cli.Next("BucketName") 
// async. delete value from the bucket
cli.Del("BucketName", "someKey") 
// async. delete the specified bucket
cli.Destroy("BucketName") 

cli.Close() 
```  

[Server](https://github.com/gasparian/pure-kv-go/blob/main/main.go):  
```go

import (
	pkv "pure-kv-go"
)

func main() {
	srv := pkv.server.InitServer(
		6666, // port
		60, // persistence timeout sec.
		"/tmp/pure-kv-db", // db path
	)
	srv.Run()
}
```

### TODO  
 - unit tests;  
 - benchmarks;  
 - tests for race detection;  
 - github actions and badges;  
