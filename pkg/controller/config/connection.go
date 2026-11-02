package config

import (
	"context"
	"fmt"

	sdkclient "go.temporal.io/sdk/client"

	"github.com/temporalio/temporal-worker-controller/api/v1alpha1"
)

// ClientWithIsValid pairs a Temporal SDK client with an IsValid method.
type ClientWithIsValid interface {
	// Client returns the Temporal SDK client.
	Client() sdkclient.Client
	// IsValid returns true if the Temporal SDK client connection is valid.
	IsValid() bool
}

// CreateClientFunc creates a Temporal SDK client for a connection spec: secret
// parsing, options, dialing, and health check.
type CreateClientFunc func(
	ctx context.Context,
	spec v1alpha1.ConnectionSpec,
	temporalNamespace string,
	k8sNamespace string,
	identity string,
) (ClientWithIsValid, error)

// ClientOptionsFunc overrides the Temporal SDK client options the controller
// uses when creating client connections to a Temporal API endpoint.
type ClientOptionsFunc func(sdkclient.Options) sdkclient.Options

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
	CreateClientFunc CreateClientFunc
	// ClientOptionsFunc overrides the Temporal sdkclient.Options used when
	// creating client connections to Temporal API endpoints.
	//
	// Note that using this requires wrapping the temporal-worker-controller
	// binary and building a new container image.
	//
	// Note this option is mutually exclusive with CreateClientFunc.
	ClientOptionsFunc ClientOptionsFunc
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
func WithConnectionCreateClientFunc(fn CreateClientFunc) Option {
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
func WithConnectionClientOptionsFunc(fn ClientOptionsFunc) Option {
	return func(c *Config) {
		c.Connection.ClientOptionsFunc = fn
	}
}
