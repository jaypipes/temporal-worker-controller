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

type (
	Option            = config.Option
	ClientWithIsValid = config.ClientWithIsValid
	CreateClientFunc  = config.CreateClientFunc
	ClientOptionsFunc = config.ClientOptionsFunc
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
	pool := clientpool.New(cfg, mgr.GetClient())
	return &Controller{
		Client:             mgr.GetClient(),
		Scheme:             mgr.GetScheme(),
		TemporalClientPool: pool,
		Recorder:           mgr.GetEventRecorderFor("temporal-worker-controller"), //nolint:staticcheck // deprecated; migration to GetEventRecorder requires changing the reconciler field type and all Eventf call sites
		Config:             cfg,
	}, nil
}
