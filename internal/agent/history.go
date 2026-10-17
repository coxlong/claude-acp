package agent

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/coxlong/claude-acp/internal/acp"
)

const (
	historyScanChunk = 64 << 10
	historyMaxItems  = 200
	historyMaxText   = 8000
)

type transcriptEntry struct {
	Type         string `json:"type"`
	Subtype      string `json:"subtype"`
	Summary      string `json:"summary"`
	Cwd          string `json:"cwd"`
	RelocatedCwd string `json:"relocatedCwd"`
	IsMeta       bool   `json:"isMeta"`
	Message      struct {
		Role    string          `json:"role"`
		Content json.RawMessage `json:"content"`
	} `json:"message"`
}

func (a *Agent) configDir() string {
	if a.cfg.ConfigDir != "" {
		return a.cfg.ConfigDir
	}
	if v := os.Getenv("CLAUDE_CONFIG_DIR"); v != "" {
		return v
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ".claude"
	}
	return filepath.Join(home, ".claude")
}

func (a *Agent) listSessions(cwd string) ([]acp.SessionInfo, error) {
	root := filepath.Join(a.configDir(), "projects")
	dirs, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	out := make([]acp.SessionInfo, 0)
	for _, dir := range dirs {
		if !dir.IsDir() {
			continue
		}
		files, err := os.ReadDir(filepath.Join(root, dir.Name()))
		if err != nil {
			continue
		}
		for _, file := range files {
			if file.IsDir() || !strings.HasSuffix(file.Name(), ".jsonl") {
				continue
			}
			path := filepath.Join(root, dir.Name(), file.Name())
			info, ok := scanSessionFile(path, cwd)
			if ok {
				out = append(out, info)
			}
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].UpdatedAt > out[j].UpdatedAt })
	if len(out) > historyMaxItems {
		out = out[:historyMaxItems]
	}
	return out, nil
}

func scanSessionFile(path, wantCwd string) (acp.SessionInfo, bool) {
	stat, err := os.Stat(path)
	if err != nil {
		return acp.SessionInfo{}, false
	}
	id := strings.TrimSuffix(filepath.Base(path), ".jsonl")

	var (
		foundCwd  string
		title     string
		firstUser string
	)
	scan := func(data []byte, skipFirst bool) {
		lines := strings.Split(string(data), "\n")
		for i, line := range lines {
			if i == 0 && skipFirst {
				continue
			}
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			var entry transcriptEntry
			if json.Unmarshal([]byte(line), &entry) != nil {
				continue
			}
			if foundCwd == "" {
				if entry.Cwd != "" {
					foundCwd = entry.Cwd
				} else if entry.RelocatedCwd != "" {
					foundCwd = entry.RelocatedCwd
				}
			}
			if title == "" && entry.Type == "summary" && entry.Summary != "" {
				title = entry.Summary
			}
			if firstUser == "" && entry.Type == "user" && !entry.IsMeta {
				if text, hasTool := contentText(entry.Message.Content); !hasTool && text != "" {
					firstUser = text
				}
			}
		}
	}

	f, err := os.Open(path)
	if err != nil {
		return acp.SessionInfo{}, false
	}
	defer f.Close()

	head := make([]byte, historyScanChunk)
	n, _ := f.Read(head)
	head = head[:n]
	scan(head, false)

	if stat.Size() > int64(len(head)) {
		offset := stat.Size() - historyScanChunk
		if offset < 0 {
			offset = 0
		}
		if _, err := f.Seek(offset, io.SeekStart); err == nil {
			tail := make([]byte, historyScanChunk)
			m, _ := f.Read(tail)
			tail = tail[:m]
			scan(tail, offset > 0)
		}
	}

	if wantCwd != "" {
		if foundCwd == "" || !samePath(foundCwd, wantCwd) {
			return acp.SessionInfo{}, false
		}
	}
	if title == "" {
		title = truncateRunes(firstUser, 60)
	}
	if title == "" {
		title = id
	}

	return acp.SessionInfo{
		SessionID: id,
		Cwd:       foundCwd,
		Title:     title,
		UpdatedAt: stat.ModTime().UTC().Format(time.RFC3339),
	}, true
}

func (a *Agent) findSessionFile(id, cwd string) string {
	root := filepath.Join(a.configDir(), "projects")
	dirs, err := os.ReadDir(root)
	if err != nil {
		return ""
	}
	var fallback string
	for _, dir := range dirs {
		if !dir.IsDir() {
			continue
		}
		path := filepath.Join(root, dir.Name(), id+".jsonl")
		if _, err := os.Stat(path); err != nil {
			continue
		}
		if cwd == "" {
			return path
		}
		if _, ok := scanSessionFile(path, cwd); ok {
			return path
		}
		if fallback == "" {
			fallback = path
		}
	}
	return fallback
}

func (a *Agent) replayHistory(sess *Session) {
	path := a.findSessionFile(sess.id, sess.cwd)
	if path == "" {
		return
	}
	entries := readTranscript(path)
	for _, entry := range entries {
		switch entry.Type {
		case "user":
			text, hasTool := contentText(entry.Message.Content)
			if hasTool || entry.IsMeta || text == "" {
				continue
			}
			a.notify(sess, acp.SessionUpdate{
				SessionUpdate: acp.UpdateUserMessageChunk,
				Content:       acp.TextContent(truncateRunes(text, historyMaxText)),
			})
		case "assistant":
			text, _ := contentText(entry.Message.Content)
			if text == "" {
				continue
			}
			a.notify(sess, acp.SessionUpdate{
				SessionUpdate: acp.UpdateAgentMessageChunk,
				Content:       acp.TextContent(truncateRunes(text, historyMaxText)),
			})
		}
	}
}

func readTranscript(path string) []transcriptEntry {
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()

	var entries []transcriptEntry
	r := newLineReader(f, 16<<20)
	for {
		line, err := r.next()
		if len(line) > 0 {
			var entry transcriptEntry
			if json.Unmarshal(line, &entry) == nil {
				if entry.Type == "user" || entry.Type == "assistant" {
					entries = append(entries, entry)
					if len(entries) > historyMaxItems {
						entries = entries[len(entries)-historyMaxItems:]
					}
				}
			}
		}
		if err != nil {
			break
		}
	}
	return entries
}

// contentText extracts text from a message content value (string or content
// blocks) and reports whether it contained a tool_result.
func contentText(raw json.RawMessage) (string, bool) {
	if len(raw) == 0 {
		return "", false
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s, false
	}
	var blocks []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if json.Unmarshal(raw, &blocks) != nil {
		return "", false
	}
	var b strings.Builder
	hasTool := false
	for _, block := range blocks {
		switch block.Type {
		case "text":
			if block.Text != "" {
				if b.Len() > 0 {
					b.WriteByte('\n')
				}
				b.WriteString(block.Text)
			}
		case "tool_result":
			hasTool = true
		}
	}
	return b.String(), hasTool
}

type lineReader struct {
	r   *bufio.Reader
	max int
}

func newLineReader(r io.Reader, max int) *lineReader {
	return &lineReader{r: bufio.NewReaderSize(r, 64<<10), max: max}
}

func (l *lineReader) next() ([]byte, error) {
	var buf []byte
	for {
		chunk, err := l.r.ReadSlice('\n')
		if len(buf)+len(chunk) > l.max {
			return nil, errors.New("line exceeds limit")
		}
		buf = append(buf, chunk...)
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		return bytes.TrimRight(buf, "\r\n"), err
	}
}

// samePath compares paths after cleaning and resolving symlinks. macOS
// reports /private/tmp for /tmp, and Claude Code stores the canonical form.
func samePath(a, b string) bool {
	if filepath.Clean(a) == filepath.Clean(b) {
		return true
	}
	ra, errA := filepath.EvalSymlinks(a)
	rb, errB := filepath.EvalSymlinks(b)
	return errA == nil && errB == nil && ra == rb
}

func truncateRunes(s string, limit int) string {
	runes := []rune(s)
	if len(runes) <= limit {
		return s
	}
	return string(runes[:limit]) + "…"
}
