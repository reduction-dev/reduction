package logging

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"
)

// TextHandler writes log lines optimized for local debugging where each log line
// has an instance ID to allow filtering by a specific logging concern.
type TextHandler struct {
	instanceID string
	mu         *sync.Mutex // Serialize writes to attrs
	attrs      []slog.Attr
}

func NewTextHandler() *TextHandler {
	return &TextHandler{
		mu:         &sync.Mutex{},
		instanceID: "root",
	}
}

func (h *TextHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return level >= globalLevel.Level()
}

func (h *TextHandler) Handle(ctx context.Context, r slog.Record) error {
	buf := make([]byte, 0, 1024)
	buf = fmt.Appendf(buf, "%s ", time.Now().Format("2006/01/02 15:04:05"))
	buf = fmt.Appendf(buf, "%s ", r.Level.String())
	buf = fmt.Appendf(buf, "[%s] ", h.instanceID)
	buf = fmt.Appendf(buf, "%s", r.Message)

	r.Attrs(func(a slog.Attr) bool {
		buf = fmt.Appendf(buf, " %s=", a.Key)
		buf = appendValue(buf, a.Value)
		return true
	})

	fmt.Fprintln(os.Stderr, string(buf))
	return nil
}

func (h *TextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Set the instance ID to a special logger member and remove it from the
	// attrs.  It should be the first item in attrs.
	nextHandler := h.clone()
	for i, a := range attrs {
		if a.Key == "instanceID" {
			nextHandler.instanceID = a.Value.String()
			attrs = slices.Delete(attrs, i, i+1)
			break
		}
	}

	nextHandler.attrs = append(nextHandler.attrs, attrs...)
	return nextHandler
}

func (h *TextHandler) WithGroup(name string) slog.Handler {
	panic("groups not supported")
}

func (h *TextHandler) clone() *TextHandler {
	return &TextHandler{
		mu:         h.mu,
		instanceID: h.instanceID,
	}
}

// Append a value to the buffer wrapping in quotes if needed.
func appendValue(buf []byte, value slog.Value) []byte {
	s := value.String()
	if needsQuoting(s) {
		buf = fmt.Appendf(buf, "%q", s)
	} else {
		buf = fmt.Appendf(buf, "%s", s)
	}
	return buf
}

// Copied from the std library with safeSet check removed since really only
// spaces and `=` should be a problem with the text logger.
func needsQuoting(s string) bool {
	if len(s) == 0 {
		return true
	}
	for i := 0; i < len(s); {
		b := s[i]
		if b < utf8.RuneSelf {
			// Quote anything except a backslash that would need quoting in a
			// JSON string, as well as space and '='
			if b != '\\' && (b == ' ' || b == '=') {
				return true
			}
			i++
			continue
		}
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError || unicode.IsSpace(r) || !unicode.IsPrint(r) {
			return true
		}
		i += size
	}
	return false
}
