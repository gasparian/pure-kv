# pure-kv-go  
Simple in-memory key-value storage with RPC interface, written in pure go  

## Proposal  

This storage must be a web service, that can create and store go maps on it's side.  
The data exchange format must be in binary format, gob encoded.  
No third-party libraries must be used.  
Communication between client and server must be implemented via RPC.  

Maps must support the folowing data-types:  
 - go primitive types;  
 - slices for all go primitives;  
 - binary data;  

Should support the following operations:  
 - `CREATE`: creates the new map with specified type on server;  
 - `ADD` - behavior depends on the data type:  
     - `string` - concatenates strings;  
     - `int/float` - makes addition operation;  
     - `[]` - pushes new value to the slice if the data type is the same;  
 - `SET`: creates new key-value pair in the specified map;  
 - `DEL`: async. delete value from the map;  
 - `DESTROY`: async. delete the specified map;  
