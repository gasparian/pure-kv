# pure-kv-go  
Simple and fast in-memory key-value storage with RPC interface, written in go.  

<p align="center"> <img src="https://github.com/gasparian/pure-kv-go/blob/main/pics/logo.jpg" height=300/> </p>  

## Reference  
Has the following properties:  
 * uses RPC interface;  
 * stores byte arrays only;  
 * persistant;  
 * uses concurrency-effective maps;  
 * supports iteration over maps;  
 * no third-party libraries has been used;  

### API  
 - `CREATE`: creates the new map with specified key-value pair type;  
 - `SET`: creates new key-value pair in the specified map;  
 - `GET`: returns decoded value;  
 - `NEXT`: get next element of a slice or map;  
 - `DEL`: async. delete value from the map;  
 - `DESTROY`: async. delete the specified map;  

### TODO  
 - implement consistent hashing sharding for the struct that stores buckets;  
 - unit tests;  
 - benchmarks;  
 - tests for race detection;  
 - github actions and badges;  
