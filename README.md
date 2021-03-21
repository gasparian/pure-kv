# pure-kv-go  
Simple in-memory key-value storage with RPC interface, written in pure go  

<p align="center"> <img src="https://github.com/gasparian/pure-kv-go/blob/main/pics/logo.jpg" height=320/> </p>  

## Proposal  
This is must be a RPC service, that can create and store go maps on it's side.  
The data must be exchanged in binary format, over tcp.  
No third-party libraries allowed.  

Should support the following operations:  
 - `CREATE`: creates the new map with specified key-value pair type;  
 - `SET`: creates new key-value pair in the specified map;  
 - `GET`: returns decoded value;  
 - `NEXT`: get next element of a slice or map;  
 - `DEL`: async. delete value from the map;  
 - `DESTROY`: async. delete the specified map;  

### Links  
 - https://github.com/tensor-programming/go-basic-rpc  

### TODO  
 - main runner for server;  
 - tests;  
 - github actions and badges;  
 - add persistance (configurable) - fo example dump all maps to disk every 600 sec.;  
