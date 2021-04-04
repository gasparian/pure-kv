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

### API  

```go
import (
    "pure-kv-go/client"
)

cli, _ := client.InitPureKvClient("0.0.0.0:8001", uint(30))
cli.Create("BucketName") // creates the new bucket with specified key-value pair type
cli.Set("BucketName", "someKey", []byte{"a"}) // creates new key-value pair in the specified bucket
val, _ := cli.Get("BucketName", "someKey") // returns decoded value
k, val, _ := cli.Next("BucketName") // get next element of bucket
cli.Del("BucketName", "someKey") // async. delete value from the bucket
cli.Destroy("BucketName") // async. delete the specified bucket
cli.Close() 
```  

### TODO  
 - unit tests;  
 - benchmarks;  
 - tests for race detection;  
 - github actions and badges;  
