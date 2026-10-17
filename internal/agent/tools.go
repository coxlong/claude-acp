package agent

import (
	"fmt"
	"strings"

	"github.com/coxlong/claude-acp/internal/acp"
)

func toolKind(name string) string {
	switch name {
	case "Read":
		return acp.ToolKindRead
	case "Edit", "Write", "MultiEdit", "NotebookEdit":
		return acp.ToolKindEdit
	case "Bash", "BashOutput", "KillShell":
		return acp.ToolKindExecute
	case "Glob", "Grep":
		return acp.ToolKindSearch
	case "WebFetch", "WebSearch":
		return acp.ToolKindFetch
	case "Task", "Agent":
		return acp.ToolKindOther
	default:
		return acp.ToolKindOther
	}
}

func toolTitle(name string, input map[string]any) string {
	switch name {
	case "Read":
		if v := firstString(input, "file_path", "path"); v != "" {
			return "Read " + v
		}
	case "Edit", "Write", "MultiEdit", "NotebookEdit":
		if v := firstString(input, "file_path", "path"); v != "" {
			return name + " " + v
		}
	case "Bash":
		if v := firstString(input, "command"); v != "" {
			return "Run: " + truncate(v, 80)
		}
	case "Grep":
		if v := firstString(input, "pattern"); v != "" {
			return "Search: " + truncate(v, 60)
		}
	case "Glob":
		if v := firstString(input, "pattern"); v != "" {
			return "Find: " + truncate(v, 60)
		}
	case "WebFetch":
		if v := firstString(input, "url"); v != "" {
			return "Fetch: " + truncate(v, 80)
		}
	case "WebSearch":
		if v := firstString(input, "query"); v != "" {
			return "Search web: " + truncate(v, 60)
		}
	case "Task", "Agent":
		if v := firstString(input, "description", "prompt"); v != "" {
			return "Task: " + truncate(v, 60)
		}
	}
	if name == "" {
		return "Tool call"
	}
	return name
}

func firstString(input map[string]any, keys ...string) string {
	for _, key := range keys {
		if v, ok := input[key]; ok {
			if s, ok := v.(string); ok && s != "" {
				return s
			}
		}
	}
	return ""
}

func truncate(s string, limit int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) <= limit {
		return s
	}
	return fmt.Sprintf("%s…", s[:limit])
}
