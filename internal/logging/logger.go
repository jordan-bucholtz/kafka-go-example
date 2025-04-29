package logging

import (
	"fmt"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Logger is a structured logger used throughout the application
type Logger struct {
	*zap.SugaredLogger
}

// Config holds configuration for the logger
type Config struct {
	Level      string `json:"level"`
	OutputPath string `json:"outputPath"`
	Encoding   string `json:"encoding"`
	DevMode    bool   `json:"devMode"`
}

// DefaultConfig returns a default logging configuration
func DefaultConfig() Config {
	return Config{
		Level:      "info",
		OutputPath: "stdout",
		Encoding:   "json",
		DevMode:    false,
	}
}

// New creates a new logger with the given configuration
func New(cfg Config) (*Logger, error) {
	// Parse log level
	level, err := zapcore.ParseLevel(cfg.Level)
	if err != nil {
		return nil, fmt.Errorf("invalid log level %q: %w", cfg.Level, err)
	}

	// Create zap logger config
	var zapConfig zap.Config
	if cfg.DevMode {
		// Development mode: pretty console logging
		zapConfig = zap.NewDevelopmentConfig()
		zapConfig.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	} else {
		// Production mode: structured JSON logging
		zapConfig = zap.NewProductionConfig()
		zapConfig.EncoderConfig.TimeKey = "timestamp"
		zapConfig.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	}

	// Apply custom config
	zapConfig.Level = zap.NewAtomicLevelAt(level)
	zapConfig.OutputPaths = []string{cfg.OutputPath}
	if cfg.Encoding != "" {
		zapConfig.Encoding = cfg.Encoding
	}

	// Create logger
	zapLogger, err := zapConfig.Build(
		zap.AddCaller(),
		zap.AddCallerSkip(1),
		zap.AddStacktrace(zapcore.ErrorLevel),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to build zap logger: %w", err)
	}

	// Return sugared logger wrapped in our Logger type
	sugar := zapLogger.Sugar()
	return &Logger{sugar}, nil
}

// NewDevelopmentLogger creates a logger optimized for development
func NewDevelopmentLogger() (*Logger, error) {
	cfg := DefaultConfig()
	cfg.Level = "debug"
	cfg.Encoding = "console"
	cfg.DevMode = true
	return New(cfg)
}

// NewProductionLogger creates a logger optimized for production
func NewProductionLogger() (*Logger, error) {
	cfg := DefaultConfig()
	return New(cfg)
}

// WithField adds a field to the logger context
func (l *Logger) WithField(key string, value interface{}) *Logger {
	return &Logger{l.With(key, value)}
}

// WithFields adds multiple fields to the logger context
func (l *Logger) WithFields(fields map[string]interface{}) *Logger {
	args := make([]interface{}, 0, len(fields)*2)
	for k, v := range fields {
		args = append(args, k, v)
	}
	return &Logger{l.With(args...)}
}

// WithError adds an error field to the logger context
func (l *Logger) WithError(err error) *Logger {
	return &Logger{l.With("error", err)}
}
