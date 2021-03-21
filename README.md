# pure-kv-go  
Simple in-memory key-value storage with RPC interface, written in go.  

<p align="center"> <img src="https://github.com/gasparian/pure-kv-go/blob/main/pics/logo.jpg" height=300/> </p>  

## Reference  
Service that can create and store go maps with byte arrays as values.  
Has the following properties:  
 * RPC interface.  
 * Persistant.  
 * Supports iteration over maps.  
 * No third-party libraries has been used.  

### API  
 - `CREATE`: creates the new map with specified key-value pair type;  
 - `SET`: creates new key-value pair in the specified map;  
 - `GET`: returns decoded value;  
 - `NEXT`: get next element of a slice or map;  
 - `DEL`: async. delete value from the map;  
 - `DESTROY`: async. delete the specified map;  

### TODO  
 - tests;  
 - github actions and badges;  
