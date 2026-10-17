package rpc

// ObjectStream is a bidirectional stream of JSON-RPC objects. Implementations
// are responsible for framing (for example one JSON object per WebSocket text
// message). ReadObject and WriteObject may be called from different
// goroutines; the connection serializes writes.
type ObjectStream interface {
	ReadObject(v any) error
	WriteObject(v any) error
	Close() error
}
