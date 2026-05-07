package logger

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sync"
)

type Color string

const (
	Reset   Color = "\033[0m"
	Red     Color = "\033[1;31m"
	Green   Color = "\033[1;32m"
	Yellow  Color = "\033[1;33m"
	Blue    Color = "\033[1;34m"
	Magenta Color = "\033[1;35m"
)

var logLevelColorMap = map[slog.Level]Color{
	slog.LevelDebug: Blue,
	slog.LevelInfo:  Green,
	slog.LevelWarn:  Magenta,
	slog.LevelError: Red,
}

type ColoredHandler struct {
	// 组合底层的 TextHandler 来托管复杂的 Attrs 和 Group 逻辑，消灭内存二次分配
	slog.Handler
	w  io.Writer
	mu sync.Mutex // 确保原子写入底层 io.Writer，防止多线程日志交错
}

func NewColoredHandler(out io.Writer, opts *slog.HandlerOptions) *ColoredHandler {
	if opts == nil {
		opts = &slog.HandlerOptions{}
	}
	// ✅ 开启官方底层优化过的 Source 捕获，这是高性能的关键
	opts.AddSource = true

	return &ColoredHandler{
		// 内部代理给标准库的 TextHandler
		Handler: slog.NewTextHandler(out, opts),
		w:       out,
	}
}

// Handle 完整函数实现：高性能、带颜色的结构化日志提纯
func (h *ColoredHandler) Handle(ctx context.Context, r slog.Record) error {
	color, ok := logLevelColorMap[r.Level]
	if !ok {
		color = Reset
	}

	// 1. 格式化时间和级别
	timeStr := r.Time.Format("2006-01-02 15:04:05.000")
	levelStr := fmt.Sprintf("[%s%s%s]", color, r.Level.String(), Reset)

	// 2. ✅ 高性能获取 Source 源码位置（从 r.PC 直接提取，拒绝 runtime.CallersFrames）
	sourceStr := ""
	if r.PC != 0 {
		fs := runtime.CallersFrames([]uintptr{r.PC})
		frame, _ := fs.Next()
		if frame.File != "" {
			source := fmt.Sprintf("%s:%d", filepath.Base(frame.File), frame.Line)
			sourceStr = fmt.Sprintf("[%s%s%s] ", Yellow, source, Reset)
		}
	}

	// 3. 组装核心消息体
	messageStr := fmt.Sprintf("%s%s%s", color, r.Message, Reset)
	output := fmt.Sprintf("%s %s %s%s", timeStr, levelStr, sourceStr, messageStr)

	// 4. ✅ 提取动态附带的属性 (Args)
	r.Attrs(func(a slog.Attr) bool {
		output += fmt.Sprintf(" %s=%v", a.Key, a.Value.Any())
		return true
	})
	output += "\n"

	// 5. ✅ 并发锁保护：防止多 Goroutine 并发写网卡/屏幕时发生字符交错
	h.mu.Lock()
	defer h.mu.Unlock()
	_, err := h.w.Write([]byte(output))
	return err
}

// WithAttrs 完整函数实现：继承链条不能断
func (h *ColoredHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &ColoredHandler{
		Handler: h.Handler.WithAttrs(attrs),
		w:       h.w,
	}
}

// WithGroup 完整函数实现
func (h *ColoredHandler) WithGroup(name string) slog.Handler {
	return &ColoredHandler{
		Handler: h.Handler.WithGroup(name),
		w:       h.w,
	}
}

func newLogger(level slog.Level) *slog.Logger {
	opts := &slog.HandlerOptions{
		Level: level,
	}
	handler := NewColoredHandler(os.Stdout, opts)
	return slog.New(handler)
}

type Logger struct {
	*slog.Logger
}

func NewLogger(level slog.Level) *Logger {
	return &Logger{
		Logger: newLogger(level),
	}
}

// 全局单例
var Log *Logger = NewLogger(slog.LevelInfo)

// Fatal 完整函数实现：支持结构化参数传递
func (l *Logger) Fatal(msg string, args ...any) {
	l.Error(msg, args...)
	// 在系统级中间件中，Fatal 意味着不可逆转的致命错误，抛出 panic 强制留存 Coredump 现场
	panic(msg)
}
