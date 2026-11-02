package config

import (
	"fmt"

	"github.com/temporalio/temporal-worker-controller/internal/controller/clientpool"
)

// ConnectionConfig holds instructions for how the controller should handle
// Connections to Temporal API endpoints.
type ConnectionConfig struct {
	// CreateClientFunc allows deployers to completely override how the
	// controller instantiates client connections to Temporal API endpoints.
	//
	// Note that using this requires wrapping the temporal-worker-controller
	// binary and building a new container image.
	//
	// Note this option is mutually exclusive with ClientOptionsFunc.
	CreateClientFunc clientpool.CreateClientFunc
	// ClientOptionsFunc overrides the Temporal sdkclient.Options used when
	// creating client connections to Temporal API endpoints.
	//
	// Note that using this requires wrapping the temporal-worker-controller
	// binary and building a new container image.
	//
	// Note this option is mutually exclusive with CreateClientFunc.
	ClientOptionsFunc clientpool.ClientOptionsFunc
}

// Validate returns an error if the ConnectionConfig isn't valid.
func (c ConnectionConfig) Validate() error {
	if c.CreateClientFunc != nil && c.ClientOptionsFunc != nil {
		return fmt.Errorf(
			"CreateClientFunc and ClientOptionsFunc are mutually exclusive",
		)
	}
	return nil
}

// WithConnectionCreateClientFunc overrides the default client-creation path.
// The function owns the full construction: secret parsing, options, dialing,
// and health check.
//
// Note that using this requires wrapping the temporal-worker-controller binary
// and building a new container image.
//
// Note this option is mutually exclusive with the
// WithConnectionClientOptionsFunc option.
func WithConnectionCreateClientFunc(fn clientpool.CreateClientFunc) Option {
	return func(c *Config) {
		c.Connection.CreateClientFunc = fn
	}
}

// WithConnectionClientOptionsFunc overrides the Temporal sdkclient.Options
// used when creating client connections to Temporal API endpoints.
//
// Note that using this requires wrapping the temporal-worker-controller binary
// and building a new container image.
//
// Note this option is mutually exclusive with the
// WithConnectionCreateClientFunc option.
func WithConnectionClientOptionsFunc(fn clientpool.ClientOptionsFunc) Option {
	return func(c *Config) {
		c.Connection.ClientOptionsFunc = fn
	}
}
