// Package rpc implements the subset of JSON-RPC 2.0 needed by ACP:
// requests, responses, notifications, and request cancellation.
package rpc

import (
	"encoding/json"
	"fmt"
)

// Version is the only JSON-RPC version this package speaks.
const Version = "2.0"

// Request is an inbound or outbound JSON-RPC request. A nil ID means the
// message is a notification.
type Request struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

// IsNotification reports whether the request carries no ID.
func (r *Request) IsNotification() bool { return len(r.ID) == 0 }

// DecodeParams unmarshals the request parameters into v. An absent params
// member is treated as null.
func (r *Request) DecodeParams(v any) error {
	if len(r.Params) == 0 {
		return nil
	}
	return json.Unmarshal(r.Params, v)
}

// Response is a JSON-RPC response. Exactly one of Result or Error is set.
type Response struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *Error          `json:"error,omitempty"`
}

// Error is a JSON-RPC error object.
type Error struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	if e.Data != nil {
		return fmt.Sprintf("%s (%d): %v", e.Message, e.Code, e.Data)
	}
	return fmt.Sprintf("%s (%d)", e.Message, e.Code)
}

// Standard JSON-RPC error constructors. ACP uses the standard set plus
// -32800 for cancelled requests.
var (
	ErrParse          = &Error{Code: -32700, Message: "Parse error"}
	ErrInvalidRequest = &Error{Code: -32600, Message: "Invalid Request"}
	ErrInternal       = &Error{Code: -32603, Message: "Internal error"}
	ErrCancelled      = &Error{Code: -32800, Message: "Request cancelled"}
	ErrClosed         = &Error{Code: -32000, Message: "Connection closed"}
)

// ErrMethodNotFound returns the standard "method not found" error.
func ErrMethodNotFound(method string) *Error {
	return &Error{Code: -32601, Message: "Method not found: " + method}
}

// ErrInvalidParams returns the standard "invalid params" error.
func ErrInvalidParams(reason string) *Error {
	return &Error{Code: -32602, Message: "Invalid params: " + reason}
}

// ErrInternalMsg returns an internal error carrying a human-readable reason.
func ErrInternalMsg(reason string) *Error {
	return &Error{Code: -32603, Message: "Internal error: " + reason}
}

func marshalResult(v any) (json.RawMessage, error) {
	if v == nil {
		v = struct{}{}
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(b), nil
}

func marshalParams(v any) (json.RawMessage, error) {
	if v == nil {
		return nil, nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(b), nil
}
