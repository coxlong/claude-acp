package agent

import (
	"context"
	"sync"
	"time"

	"github.com/coxlong/claude-acp/internal/acp"
	"github.com/coxlong/claude-acp/internal/claude"
)

// Session is one ACP session backed by one Claude CLI process.
type Session struct {
	id     string
	cwd    string
	agent  *Agent
	client *claude.Client

	// promptMu serializes prompts within a session.
	promptMu sync.Mutex

	mu              sync.Mutex
	mode            string
	model           string
	effort          string
	models          []claude.ModelInfo
	configOptions   []acp.SessionConfigOption
	cancelled       bool
	running         bool
	turnCtx         context.Context
	turnCancel      context.CancelFunc
	cancelPending   bool
	cancelPendingAt time.Time
	sawText         bool
	sawThinking     bool
	toolCalls       map[string]bool
}

func (s *Session) modes() *acp.SessionModeState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return &acp.SessionModeState{
		CurrentModeID:  s.mode,
		AvailableModes: availableModes(),
	}
}

func availableModes() []acp.SessionMode {
	return []acp.SessionMode{
		{ID: "default", Name: "Default", Description: "Ask before acting"},
		{ID: "acceptEdits", Name: "Accept Edits", Description: "Auto-approve file edits"},
		{ID: "plan", Name: "Plan", Description: "Plan only, do not execute"},
	}
}

func (s *Session) snapshotConfigOptions() []acp.SessionConfigOption {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]acp.SessionConfigOption, len(s.configOptions))
	copy(out, s.configOptions)
	return out
}

func (s *Session) buildConfigOptions() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.buildConfigOptionsLocked()
}

func (s *Session) buildConfigOptionsLocked() {
	options := make([]acp.SessionConfigOption, 0, 3)

	modes := availableModes()
	modeOptions := make([]acp.SessionConfigSelectOption, 0, len(modes))
	for _, m := range modes {
		modeOptions = append(modeOptions, acp.SessionConfigSelectOption{Value: m.ID, Name: m.Name, Description: m.Description})
	}
	currentMode := s.mode
	if currentMode == "" {
		currentMode = "default"
	}
	s.mode = currentMode
	options = append(options, acp.SessionConfigOption{
		ID:           "mode",
		Name:         "Mode",
		Description:  "Session permission mode",
		Category:     "mode",
		Type:         "select",
		CurrentValue: currentMode,
		Options:      modeOptions,
	})

	modelOptions := make([]acp.SessionConfigSelectOption, 0, len(s.models))
	for _, m := range s.models {
		value := m.Name()
		if value == "" {
			continue
		}
		name := m.DisplayName
		if name == "" {
			name = value
		}
		modelOptions = append(modelOptions, acp.SessionConfigSelectOption{
			Value:       value,
			Name:        name,
			Description: m.Description,
		})
	}
	if len(modelOptions) == 0 {
		modelOptions = append(modelOptions, acp.SessionConfigSelectOption{Value: "default", Name: "Default"})
	}
	currentModel := s.model
	if currentModel == "" || !hasOption(modelOptions, currentModel) {
		currentModel = modelOptions[0].Value
		for _, o := range modelOptions {
			if o.Value == "default" {
				currentModel = "default"
				break
			}
		}
	}
	s.model = currentModel
	options = append(options, acp.SessionConfigOption{
		ID:           "model",
		Name:         "Model",
		Description:  "AI model to use",
		Category:     "model",
		Type:         "select",
		CurrentValue: currentModel,
		Options:      modelOptions,
	})

	levels := s.effortLevelsLocked()
	if len(levels) > 0 {
		effortOptions := []acp.SessionConfigSelectOption{{Value: "default", Name: "Default"}}
		for _, level := range levels {
			effortOptions = append(effortOptions, acp.SessionConfigSelectOption{Value: level, Name: level})
		}
		currentEffort := s.effort
		if currentEffort == "" || !hasOption(effortOptions, currentEffort) {
			currentEffort = "default"
		}
		s.effort = currentEffort
		options = append(options, acp.SessionConfigOption{
			ID:           "effort",
			Name:         "Thinking Effort",
			Description:  "Reasoning effort for the current model",
			Category:     "thought_level",
			Type:         "select",
			CurrentValue: currentEffort,
			Options:      effortOptions,
		})
	}

	s.configOptions = options
}

func (s *Session) effortLevelsLocked() []string {
	if s.client == nil {
		return nil
	}
	if levels := s.client.SupportedEffortLevels(); len(levels) > 0 {
		return levels
	}
	for _, m := range s.models {
		if m.Name() == s.model {
			return m.SupportedEffortLevels
		}
	}
	return nil
}

func (s *Session) setMode(mode string) {
	s.mu.Lock()
	s.mode = mode
	s.buildConfigOptionsLocked()
	s.mu.Unlock()
}

func (s *Session) setModel(model string) {
	s.mu.Lock()
	s.model = model
	s.buildConfigOptionsLocked()
	s.mu.Unlock()
}

func (s *Session) setEffort(effort string) {
	s.mu.Lock()
	s.effort = effort
	s.buildConfigOptionsLocked()
	s.mu.Unlock()
}

func (s *Session) markCancelled() {
	s.mu.Lock()
	s.cancelled = true
	s.mu.Unlock()
}

func (s *Session) wasCancelled() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cancelled
}

// beginTurn marks a turn as running and creates the cancellable context shared
// by the prompt loop and permission requests. It reports whether a cancel
// arrived between the prompt's dispatch and this call.
func (s *Session) beginTurn(ctx context.Context) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.sawText = false
	s.sawThinking = false
	s.running = true
	s.turnCtx, s.turnCancel = context.WithCancel(ctx)
	s.toolCalls = make(map[string]bool)
	s.cancelled = false

	if s.cancelPending && time.Since(s.cancelPendingAt) <= 5*time.Second {
		s.cancelled = true
	}
	s.cancelPending = false
	return s.cancelled
}

// endTurn clears per-turn state and releases any pending permission wait.
func (s *Session) endTurn() {
	s.mu.Lock()
	if s.turnCancel != nil {
		s.turnCancel()
	}
	s.turnCancel = nil
	s.turnCtx = nil
	s.running = false
	s.cancelled = false
	s.mu.Unlock()
}

func (s *Session) isRunning() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.running
}

// turnContext is the context for the active turn. When no turn is active it
// returns an already-cancelled context so a late caller cannot block.
func (s *Session) turnContext() context.Context {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.turnCtx != nil {
		return s.turnCtx
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

// cancelTurn cancels the active turn's context (prompt loop and permission
// calls) without marking the session cancelled.
func (s *Session) cancelTurn() {
	s.mu.Lock()
	cancel := s.turnCancel
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// markCancelPending records a cancel that raced ahead of the prompt dispatch.
func (s *Session) markCancelPending() {
	s.mu.Lock()
	if !s.running {
		s.cancelPending = true
		s.cancelPendingAt = time.Now()
	}
	s.mu.Unlock()
}

func (s *Session) isAlive() bool {
	if s.client == nil {
		return false
	}
	select {
	case <-s.client.Done():
		return false
	default:
		return true
	}
}

func (s *Session) markTextDelta() {
	s.mu.Lock()
	s.sawText = true
	s.mu.Unlock()
}

func (s *Session) markThinkingDelta() {
	s.mu.Lock()
	s.sawThinking = true
	s.mu.Unlock()
}

func (s *Session) sawTextDelta() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sawText
}

func (s *Session) sawThinkingDelta() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sawThinking
}

// noteToolCall records a tool call and reports whether it is new.
func (s *Session) noteToolCall(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if id == "" {
		return false
	}
	if s.toolCalls[id] {
		return false
	}
	s.toolCalls[id] = true
	return true
}

func hasOption(options []acp.SessionConfigSelectOption, value string) bool {
	for _, o := range options {
		if o.Value == value {
			return true
		}
	}
	return false
}
