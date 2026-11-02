package config

import (
	"log/slog"
	"os"

	"go.temporal.io/sdk/log"
)

// LoggingConfig contains log-related configuration for the controller.
type LoggingConfig struct {
	// Logger allows overriding the root logger used by the controller.
	Logger log.Logger
	// TODO(jaypipes): Add declarative logging configuration options instead of
	// relying on zap's options loaded from command-line flags.
}

// SetDefaults sets the LoggingConfig's default values if empty.
func (c *LoggingConfig) SetDefaults() {
	if c.Logger == nil {
		c.Logger = defaultLogger()
	}
}

// defaultLogger returns the default Temporal SDK logger used by the
// controller.
func defaultLogger() log.Logger {
	return log.NewStructuredLogger(
		slog.New(slog.NewJSONHandler(os.Stdout, nil)),
	)
}

// WithLogger sets the root logger used by the controller.
func WithLogger(l log.Logger) Option {
	return func(c *Config) {
		c.Logging.Logger = l
	}
}
