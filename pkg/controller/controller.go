// Unless explicitly stated otherwise all files in this repository are licensed under the MIT License.
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2024 Datadog, Inc.

// Package controller lets a wrapper binary embed the worker-deployment controller and run it with
// custom Temporal client configuration.
package controller

import (
	ctrl "sigs.k8s.io/controller-runtime"

	internalcontroller "github.com/temporalio/temporal-worker-controller/internal/controller"
	"github.com/temporalio/temporal-worker-controller/internal/controller/clientpool"
	"github.com/temporalio/temporal-worker-controller/pkg/controller/config"
)

// Controller reconciles WorkerDeployment resources.
type Controller = internalcontroller.WorkerDeploymentReconciler

// CreateClientFunc creates a Temporal SDK client for a connection spec: secret parsing, options,
// dialing, and health check. The pool caches the returned CachedClient.
type CreateClientFunc = clientpool.CreateClientFunc

// CustomizeClientOptionsFunc mutates SDK client options before dialing. It only applies to
// the default client-creation path (WithDefaultClient).
type ClientOptionsFunc clientpool.ClientOptionsFunc

type (
	Option = config.Option
)

var (
	WithLogger                                     = config.WithLogger
	WithConnectionCreateClientFunc                 = config.WithConnectionCreateClientFunc
	WithConnectionClientOptionsFunc                = config.WithConnectionClientOptionsFunc
	WithMaxDeploymentVersionsIneligibleForDeletion = config.WithMaxDeploymentVersionsIneligibleForDeletion
	WithDisableDeprecatedTWD                       = config.WithDisableDeprecatedTWD
	WithDisableRecoverPanic                        = config.WithDisableRecoverPanic
	WithDisableClusterConnections                  = config.WithDisableClusterConnections
	WithWRTHPAMatchLabelsStripTemporalPrefix       = config.WithWRTHPAMatchLabelsStripTemporalPrefix
)

// New returns a Controller wired with the given manager, or an
// error if the options are misconfigured (e.g. both WithCreateClient and WithDefaultClient).
// The manager supplies the Kubernetes client, scheme, and event recorder; the Temporal
// client pool is built internally from the manager's client.
func New(mgr ctrl.Manager, opts ...Option) (*Controller, error) {
	cfg := config.ConfigFromEnv()
	for _, opt := range opts {
		opt(&cfg)
	}
	cfg.SetDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	pool := clientpool.New(
		cfg.Logger(),
		mgr.GetClient(),
		cfg.Connection.CreateClientFunc,
		cfg.Connection.ClientOptionsFunc,
	)
	return &Controller{
		Client:             mgr.GetClient(),
		Scheme:             mgr.GetScheme(),
		TemporalClientPool: pool,
		Recorder:           mgr.GetEventRecorderFor("temporal-worker-controller"), //nolint:staticcheck // deprecated; migration to GetEventRecorder requires changing the reconciler field type and all Eventf call sites
		Config:             cfg,
	}, nil
}
