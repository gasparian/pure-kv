# pure-kv-go  
Simple in-memory key-value storage with RPC interface, written in go.  

<p align="center"> <img src="https://github.com/gasparian/pure-kv-go/blob/main/pics/logo.jpg" height=300/> </p>  

## Reference  
Has the following properties:  
 * uses RPC interface.  
 * Stores byte arrays only.  
 * Persistant.  
 * Uses concurrency-effective maps.  
 * Supports iteration over maps ("buckets").  
 * No third-party libraries has been used.  

### API  
 - `CREATE`: creates the new map with specified key-value pair type;  
 - `SET`: creates new key-value pair in the specified map;  
 - `GET`: returns decoded value;  
 - `NEXT`: get next element of a slice or map;  
 - `DEL`: async. delete value from the map;  
 - `DESTROY`: async. delete the specified map;  

### TODO  
 - implement more concurrent-effective map, like adding the sharding as it has been done [here](https://github.com/orcaman/concurrent-map);  
 - unit tests;  
 - benchmarks, including concurrency;  
 - github actions and badges;  
