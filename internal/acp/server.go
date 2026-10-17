package acp

import (
	"context"
	"io"

	"github.com/coxlong/claude-acp/internal/rpc"
)

// Handler implements the ACP agent methods this server supports. Methods not
// part of the P1 surface are answered with -32601 by Server.Handle.
type Handler interface {
	Initialize(ctx context.Context, req InitializeRequest) (InitializeResponse, *rpc.Error)
	Authenticate(ctx context.Context, req AuthenticateRequest) (AuthenticateResponse, *rpc.Error)

	NewSession(ctx context.Context, req NewSessionRequest) (NewSessionResponse, *rpc.Error)
	LoadSession(ctx context.Context, req LoadSessionRequest) (LoadSessionResponse, *rpc.Error)
	ListSessions(ctx context.Context, req ListSessionsRequest) (ListSessionsResponse, *rpc.Error)
	CloseSession(ctx context.Context, req CloseSessionRequest) (CloseSessionResponse, *rpc.Error)

	Prompt(ctx context.Context, req PromptRequest) (PromptResponse, *rpc.Error)
	Cancel(ctx context.Context, req CancelNotification) *rpc.Error

	SetSessionMode(ctx context.Context, req SetSessionModeRequest) (SetSessionModeResponse, *rpc.Error)
	SetSessionConfigOption(ctx context.Context, req SetSessionConfigOptionRequest) (SetSessionConfigOptionResponse, *rpc.Error)
}

// Server dispatches JSON-RPC requests to an ACP Handler.
type Server struct {
	handler Handler
}

// NewServer creates an ACP method dispatcher.
func NewServer(handler Handler) *Server {
	return &Server{handler: handler}
}

// Close releases the underlying handler if it owns resources.
func (s *Server) Close() error {
	if closer, ok := s.handler.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

// Handle implements rpc.Handler.
func (s *Server) Handle(ctx context.Context, req *rpc.Request) (any, *rpc.Error) {
	switch req.Method {
	case MethodInitialize:
		var p InitializeRequest
		if err := decode(req, &p); err != nil {
			return nil, err
		}
		return s.handler.Initialize(ctx, p)

	case MethodAuthenticate:
		var p AuthenticateRequest
		if err := decode(req, &p); err != nil {
			return nil, err
		}
		return s.handler.Authenticate(ctx, p)

	case MethodSessionNew:
		var p NewSessionRequest
		if err := decode(req, &p); err != nil {
			return nil, err
		}
		return s.handler.NewSession(ctx, p)

	case MethodSessionLoad:
		var p LoadSessionRequest
		if err := decode(req, &p); err != nil {
			return nil, err
		}
		return s.handler.LoadSession(ctx, p)

	case MethodSessionList:
		var p ListSessionsRequest
		if err := decode(req, &p); err != nil {
			return nil, err
		}
		return s.handler.ListSessions(ctx, p)

	case MethodSessionClose:
		var p CloseSessionRequest
		if err := decode(req, &p); err != nil {
			return nil, err
		}
		return s.handler.CloseSession(ctx, p)

	case MethodSessionPrompt:
		var p PromptRequest
		if err := decode(req, &p); err != nil {
			return nil, err
		}
		return s.handler.Prompt(ctx, p)

	case MethodSessionCancel:
		var p CancelNotification
		if err := decode(req, &p); err != nil {
			return nil, err
		}
		return nil, s.handler.Cancel(ctx, p)

	case MethodSessionSetMode:
		var p SetSessionModeRequest
		if err := decode(req, &p); err != nil {
			return nil, err
		}
		return s.handler.SetSessionMode(ctx, p)

	case MethodSessionSetConfig:
		var p SetSessionConfigOptionRequest
		if err := decode(req, &p); err != nil {
			return nil, err
		}
		return s.handler.SetSessionConfigOption(ctx, p)

	case MethodLogout:
		return struct{}{}, nil

	default:
		return nil, rpc.ErrMethodNotFound(req.Method)
	}
}

func decode(req *rpc.Request, v any) *rpc.Error {
	if err := req.DecodeParams(v); err != nil {
		return rpc.ErrInvalidParams(err.Error())
	}
	return nil
}
