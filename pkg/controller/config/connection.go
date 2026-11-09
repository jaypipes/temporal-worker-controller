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
//
// Connection and ClusterConnection are used to describe configuration options
// such as the HostPort and authentication credentials for a client connection
// to a Temporal API server.
//
// Both the temporal-worker-controller Pod (aka "the controller Pod") and the
// worker Pods that are owned by the Kubernetes Deployment associated with a
// WorkerDeploymentVersion make client connections to a Temporal API server.
//
// In some environments the controller Pod needs to use a separate identity
// when connecting or needs to configure its authentication credentials
// differently from the client connection used by worker Pods. Operators have
// two options when deploying TWC into an environment with these requirements:
//
// 1. Programmatic control of the way clients are created
// 2. Declarative client configuration options
//
// Using programmatic control of the way clients are created gives the operator
// much more flexibility to integrate with custom authentication or credential
// systems. However, the operator will need to wrap the
// temporal-worker-controller binary with custom code and compile that wrapped
// code into a separate container image.
//
// Using declarative client configuration options gives the operator less
// flexibility than programmatic control however the operator does not need to
// compile a wrapped binary.
//
// # Programmatic control
//
// There are two mutually-exclusive ways to programmatically control how the
// controller manages Temporal client connections.
//
// a. Use controller.WithConnectionCreateClientFunc()
//
// Using controller.WithConnectionCreateClientFunc() with controller.New()
// gives the operator full control over how the controller handles the creation
// of all Temporal sdkclient.Client objects.
//
// b. Use controller.WithConnectionClientOptionsFunc()
//
// Using controller.WithConnectionClientOptionsFunc() with controller.New()
// gives the operator the ability to inject custom Interceptors or
// HeaderProviders without needing to supply a full client creation func.
//
// # Declarative connection configuration
//
// Use the controller.WithConnectionOptions() to declaratively configure the
// controller's handling of Temporal client connections.
//
// The config.Connection.Options struct allows operators to declaratively
// configure the behaviour of the controller in creating a Temporal
// sdkclient.Client object.
//
// Options.HostPort contains configuration options that instruct the
// controller Pod to load Connection credentials from a ClusterConnection or
// Connection mapped to the HostPort of the Connection referenced by
// `WorkerDeployment.spec.workerOptions.connectionRef`.
//
// Options.HostPort.From instructs the controller where to load the map of
// HostPort to Connection name from.
//
// Options.HostPort.From.ConfigMap is an optional field containing the name of
// a ConfigMap where the controller can find the HostPort to Connection name
// map. Keys for the ConfigMap should be the HostPort string -- e.g.
// "localhost:7233". Values for the ConfigMap should be the ClusterConnection
// name -- e.g. "my-cluster-conn" -- or the Connection's Kubernetes Namespace
// and Name in the format "<namespace>/<name>" -- e.g. "my-namespace/my-conn".
//
// Options.HostPort.Required indicates whether the controller must only use the
// Connection found in the HostPort map. When Options.HostPort.Required is
// `true` (not the default), and there was no entry for the HostPort specified
// in the Connection referenced from
// `WorkerDeployment.spec.workerOptions.connectionRef`, the WorkerDeployment
// will be have a Stalled condition applied to its Status with a reason
// indicating the controller was unable to load the Connection information.
// When Options.HostPort.Required is `false` (the default), a missing entry in
// the HostPort map causes the controller to fall back to using the exact
// Connection referenced in `WorkerDeployment.spec.workerOptions.connectionRef`
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
	// Options contains declarative configuration options instructing the
	// controller how to handle Temporal SDK client connections.
	//
	// Note this option is mutually exclusive with both CreateClientFunc and
	// ClientOptionsFunc.
	Options *ConnectionOptions
}

// Validate returns an error if the ConnectionConfig isn't valid.
func (c ConnectionConfig) Validate() error {
	if c.CreateClientFunc != nil && c.ClientOptionsFunc != nil {
		return fmt.Errorf(
			"CreateClientFunc and ClientOptionsFunc are mutually exclusive",
		)
	}
	if c.Options != nil {
		if c.CreateClientFunc != nil {
			return fmt.Errorf(
				"Options and CreateClientFunc are mutually exclusive",
			)
		}
		if c.ClientOptionsFunc != nil {
			return fmt.Errorf(
				"Options and ClientOptionsFunc are mutually exclusive",
			)
		}
	}
	return nil
}

// ConnectionOptions provides a declarative way for operators to instruct the
// controller how it should handle Temporal SDK client connections.
type ConnectionOptions struct {
	// HostPort contains instructions for the controller to look up Connection
	// credentials via a HostPort mapping instead of using the Connection
	// referenced by `WorkerDeployment.spec.workerOptions.connectionRef`
	HostPort ConnectionHostPortConfig
}

// ConnectionHostPortConfig contains instructions for the controller to look up
// Connection credentials via a HostPort mapping instead of using the
// Connection referenced by `WorkerDeployment.spec.workerOptions.connectionRef`
type ConnectionHostPortConfig struct {
	// From instructs the controller where to load the map of HostPort to
	// Connection name from.
	From ConnectionHostPortFrom
	// Options.HostPort.Required indicates whether the controller must only use the
	// Connection found in the HostPort map. When Options.HostPort.Required is
	// `true` (not the default), and there was no entry for the HostPort specified
	// in the Connection referenced from
	// `WorkerDeployment.spec.workerOptions.connectionRef`, the WorkerDeployment
	// will be have a Stalled condition applied to its Status with a reason
	// indicating the controller was unable to load the Connection information.
	// When Options.HostPort.Required is `false` (the default), a missing entry in
	// the HostPort map causes the controller to fall back to using the exact
	// Connection referenced in `WorkerDeployment.spec.workerOptions.connectionRef`
	Required bool
}

type ConnectionHostPortFrom struct {
	// ConfigMap is an optional field containing the name of a ConfigMap where
	// the controller can find the HostPort to Connection name map. The
	// ConfigMap must be in the same Kubernetes Namespace as the controller.
	//
	// Keys for
	// the ConfigMap should be the HostPort string -- e.g.  "localhost:7233".
	// Values for the ConfigMap should be the ClusterConnection name -- e.g.
	// "my-cluster-conn" -- or the Connection's Kubernetes Namespace and Name
	// in the format "<namespace>/<name>" -- e.g. "my-namespace/my-conn".
	ConfigMap string
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

// WithConnectionOptions allows declarative configuration of how the controller
// handles Temporal SDK client connections.
func WithConnectionOptions(copts ConnectionOptions) Option {
	return func(c *Config) {
		c.Connection.Options = &copts
	}
}
