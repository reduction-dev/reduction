package logging

import "log/slog"

var globalLevel = &slog.LevelVar{}

func SetLevel(level slog.Level) {
	globalLevel.Set(level)
}
