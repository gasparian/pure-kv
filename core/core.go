package core

// Shortcuts for RPC methods
const (
	Destroy = "PureKv.Destroy"
	Set     = "PureKv.Set"
	Get     = "PureKv.Get"
	Next    = "PureKv.Next"
	Del     = "PureKv.Del"
)

// Response holds binary value from server and status
type Response struct {
	Body []byte
	Ok   bool
}

// Request holds keys and values, all optional
type Request struct {
	MapKey    string
	StringKey string
	IntKey    uint64
	Value     []byte
}
