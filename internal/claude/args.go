package claude

import (
	"fmt"
	"log/slog"
	"strings"
)

// Options controls how the CLI process is started.
type Options struct {
	SessionID      string
	Resume         string
	Cwd            string
	Model          string
	PermissionMode string
	// AllowBypass makes bypassPermissions switchable at runtime without
	// starting the session in that mode.
	AllowBypass    bool
	Effort         string
	McpConfig      string
	AdditionalDirs []string
	SettingSources []string
	CliPath        string
	Env            map[string]string
	CanUseTool     func(req *PermissionRequest) (PermissionResult, error)
	OnStderr       func(string)
	Log            *slog.Logger
}

// BuildArgs returns the CLI arguments for the given options.
func BuildArgs(o Options) []string {
	args := []string{
		"--output-format", "stream-json",
		"--verbose",
		"--input-format", "stream-json",
		"--include-partial-messages",
		"--permission-prompt-tool", "stdio",
		"--replay-user-messages",
	}

	if o.Resume != "" {
		args = append(args, "--resume="+o.Resume)
	} else if o.SessionID != "" {
		args = append(args, "--session-id="+o.SessionID)
	}
	if o.Model != "" {
		args = append(args, "--model", o.Model)
	}
	if o.PermissionMode != "" {
		args = append(args, "--permission-mode", o.PermissionMode)
	}
	if o.AllowBypass {
		args = append(args, "--allow-dangerously-skip-permissions")
	}
	if o.Effort != "" {
		args = append(args, "--effort", o.Effort)
	}
	if o.McpConfig != "" {
		args = append(args, "--mcp-config", o.McpConfig)
	}
	sources := o.SettingSources
	if sources == nil {
		sources = []string{"user", "project", "local"}
	}
	if len(sources) > 0 {
		args = append(args, "--setting-sources="+strings.Join(sources, ","))
	}
	for _, dir := range o.AdditionalDirs {
		args = append(args, "--add-dir", dir)
	}
	return args
}

// Validate checks option combinations that the CLI rejects.
func (o Options) Validate() error {
	if o.SessionID != "" && o.Resume != "" {
		return fmt.Errorf("session id and resume are mutually exclusive")
	}
	if o.PermissionMode != "" && !validPermissionMode(o.PermissionMode) {
		return fmt.Errorf("unknown permission mode %q", o.PermissionMode)
	}
	return nil
}

func validPermissionMode(mode string) bool {
	switch mode {
	case "manual", "acceptEdits", "plan", "auto", "dontAsk", "bypassPermissions":
		return true
	}
	return false
}
