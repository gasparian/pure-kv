package core

// Shortcuts for RPC methods
const (
	Create  = "PureKv.Create"
	Destroy = "PureKv.Destroy"
	Set     = "PureKv.Set"
	Get     = "PureKv.Get"
	Next    = "PureKv.Next"
	Del     = "PureKv.Del"
)

// Record holds all needed data for each map entry
type Record struct {
	Key   string
	Value []byte
}

// Response holds binary value from server and status
type Response struct {
	Record
	Ok bool
}

// Request holds keys and values, all optional
type Request struct {
	Record
	MapKey string
}
