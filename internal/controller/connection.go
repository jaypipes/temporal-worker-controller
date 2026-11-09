package controller

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/go-logr/logr"
	temporaliov1alpha1 "github.com/temporalio/temporal-worker-controller/api/v1alpha1"
	"github.com/temporalio/temporal-worker-controller/pkg/controller/config"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// getConnectionByRef fetches the connection resource identified by ref and
// returns its spec and the underlying object. namespace is used only for a
// namespaced Connection; it is ignored for a cluster-scoped ClusterConnection.
// The object is returned as a client.Object so callers can manage the finalizer
// on it regardless of kind.
func (r *WorkerDeploymentReconciler) getConnectionByRef(
	ctx context.Context,
	ref temporaliov1alpha1.ConnectionReference,
	namespace string,
) (temporaliov1alpha1.ConnectionSpec, client.Object, error) {
	name := ref.ResolvedName()
	if ref.IsCluster() {
		var cc temporaliov1alpha1.ClusterConnection
		if err := r.Get(ctx, types.NamespacedName{Name: name}, &cc); err != nil {
			return temporaliov1alpha1.ConnectionSpec{}, nil, err
		}
		return cc.Spec, &cc, nil
	}
	var conn temporaliov1alpha1.Connection
	if err := r.Get(ctx, types.NamespacedName{
		Name:      name,
		Namespace: namespace,
	}, &conn); err != nil {
		return temporaliov1alpha1.ConnectionSpec{}, nil, err
	}
	if r.Config.Connection.Options != nil {
		return r.getControllerConnection(
			ctx, *r.Config.Connection.Options, conn,
		)
	}
	return conn.Spec, &conn, nil
}

// getControllerNamespace returns the Kubernetes Namespace in which the
// controller is running.
func (r *WorkerDeploymentReconciler) getControllerNamespace() (string, error) {
	if r.controllerNamespace != "" {
		return r.controllerNamespace, nil
	}
	getSANamespace := sync.OnceValues(func() (string, error) {
		path := "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
		namespace, err := os.ReadFile(path)
		if err != nil {
			return "", err
		}
		return string(namespace), nil
	})
	return getSANamespace()
}

// getHostPortToConnectionNameMap returns the map of HostPort to
// [Cluster]Connection name.
func (r *WorkerDeploymentReconciler) getHostPortToConnectionNameMap(
	ctx context.Context,
	ccOpts config.ConnectionOptions,
) (map[string]string, error) {
	controllerNS, err := r.getControllerNamespace()
	if err != nil {
		return nil, fmt.Errorf("failed to get controller namespace: %w", err)
	}

	cmName := ccOpts.HostPort.From.ConfigMap
	var cm corev1.ConfigMap
	nn := types.NamespacedName{
		Namespace: controllerNS,
		Name:      cmName,
	}
	err = r.Get(ctx, nn, &cm)
	if err != nil {
		return nil, fmt.Errorf("failed to get hostport config map: %w", err)
	}
	return cm.Data, nil
}

// getControllerConnection uses the supplied ConnectionOptions to inform the
// controller whether and how it should look up a ClusterConnection from the
// supplied Connection that was referenced from a WorkerDeployment.
//
// For security reasons, the controller may be configured to load Connection
// credentials in isolation from the Connection specified in
// WorkerDeployment.spec.workerOptions.connectionRef.
//
// When Config.Connection.Options is set, we load Connection credentials for
// the controller to use by following these rules:
//
//  1. Check to see if there is an entry in the configured HostPort map for the
//     HostPort of the Connection referenced in workerOptions.connectionRef.
//     a. If there is a matching entry, look up the ClusterConnection or
//     Connection that the entry points to.
//     b. If there is no matching entry:
//  1. If Config.Connection.Options.HostPort.Required is true, return an
//     error indicating the controller's Connection could not be loaded.
//  1. If Config.Connection.Options.HostPort.Required is false, return
//     the Connection referenced in workerOptions.connectionRef.
func (r *WorkerDeploymentReconciler) getControllerConnection(
	ctx context.Context,
	ccOpts config.ConnectionOptions,
	conn temporaliov1alpha1.Connection,
) (temporaliov1alpha1.ConnectionSpec, client.Object, error) {
	l := log.FromContext(ctx)

	hp := conn.Spec.HostPort
	required := ccOpts.HostPort.Required

	hpMap, err := r.getHostPortToConnectionNameMap(ctx, ccOpts)
	if err != nil {
		return temporaliov1alpha1.ConnectionSpec{}, nil, err
	}
	if hpMap == nil && required {
		return temporaliov1alpha1.ConnectionSpec{}, nil, fmt.Errorf(
			"no connection name found matching hostport %q", hp,
		)
	}

	if hpMap != nil {
		ccName, ok := hpMap[hp]
		if !ok {
			if required {
				return temporaliov1alpha1.ConnectionSpec{}, nil, fmt.Errorf(
					"no connection name found matching hostport %q", hp,
				)
			}
			// Fall back to returning the Connection referenced in
			// workerOptions.connectionRef.
			return conn.Spec, &conn, nil
		}

		l.V(4).Info(
			"found entry in controller hostport map",
			"controllerConnection", ccName,
			"hostPort", hp,
		)

		if strings.ContainsRune(ccName, '/') {
			// hostport map refers to a namespaced Connection.
			parts := strings.SplitN(ccName, "/", 2)
			ns := parts[0]
			name := parts[1]

			var ctrlConn temporaliov1alpha1.Connection

			nn := types.NamespacedName{
				Namespace: ns,
				Name:      name,
			}

			if err := r.Get(ctx, nn, &ctrlConn); err != nil {
				if apierrors.IsNotFound(err) {
					return temporaliov1alpha1.ConnectionSpec{}, nil, fmt.Errorf(
						"connection %q specified in "+
							"hostport map not found",
						ccName,
					)
				}
				return temporaliov1alpha1.ConnectionSpec{}, nil, err
			}
			l.V(4).Info(
				"controller connection mapped via hostport",
				"controllerConnection", ccName,
				"hostPort", hp,
			)
			return ctrlConn.Spec, &ctrlConn, nil
		}

		// hostport map refers to a ClusterConnection.
		var ctrlClusterConn temporaliov1alpha1.ClusterConnection

		nn := types.NamespacedName{
			Name: ccName,
		}

		if err := r.Get(ctx, nn, &ctrlClusterConn); err != nil {
			if apierrors.IsNotFound(err) {
				return temporaliov1alpha1.ConnectionSpec{}, nil, fmt.Errorf(
					"cluster connection %q specified in "+
						"hostport map not found",
					ccName,
				)
			}
			return temporaliov1alpha1.ConnectionSpec{}, nil, err
		}
		l.V(4).Info(
			"controller connection mapped via hostport",
			"controllerConnection", ccName,
			"hostPort", hp,
		)
		return ctrlClusterConn.Spec, &ctrlClusterConn, nil
	}
	// Fall back to returning the Connection referenced in
	// workerOptions.connectionRef.
	return conn.Spec, &conn, nil
}

// ensureConnectionFinalizer adds our finalizer to the Connection so it
// cannot be deleted while this WD still needs it for cleanup.
func (r *WorkerDeploymentReconciler) ensureConnectionFinalizer(
	ctx context.Context,
	l logr.Logger,
	conn client.Object,
) error {
	if !controllerutil.ContainsFinalizer(conn, finalizerName) {
		l.Info("Adding finalizer to connection", "connection", conn.GetName())
		controllerutil.AddFinalizer(conn, finalizerName)
		if err := r.Update(ctx, conn); err != nil {
			return fmt.Errorf("unable to add finalizer to connection %q: %w", conn.GetName(), err)
		}
	}
	return nil
}

// releaseConnectionFinalizerIfUnused removes our finalizer from the connection
// identified by ref, unless some other WorkerDeployment (other than
// selfNamespace/selfName) still references it. It is used both when a WD is
// deleted (ref = current connectionRef) and when a WD's connectionRef changes
// (ref = previously-observed connectionRef).
func (r *WorkerDeploymentReconciler) releaseConnectionFinalizerIfUnused(
	ctx context.Context,
	l logr.Logger,
	ref temporaliov1alpha1.ConnectionReference,
	selfNamespace, selfName string,
) error {
	isCluster := ref.IsCluster()

	// A namespace-scoped controller never placed a finalizer on a ClusterConnection, and
	// cannot read one to check. Nothing to release.
	if isCluster && r.Config.DisableClusterConnections {
		return nil
	}

	// Scope the "is it still used?" query correctly for the kind:
	//   - Namespaced Connection: only WDs in its own namespace can reference it,
	//     so restrict the list to selfNamespace.
	//   - ClusterConnection: a WD in ANY namespace can reference it, so we must
	//     list across all namespaces.
	var listOpts []client.ListOption
	if !isCluster {
		listOpts = append(listOpts, client.InNamespace(selfNamespace))
	}

	var wds temporaliov1alpha1.WorkerDeploymentList
	if err := r.List(ctx, &wds, listOpts...); err != nil {
		return fmt.Errorf("unable to list WorkerDeployments: %w", err)
	}

	for i := range wds.Items {
		wd := &wds.Items[i]
		// Skip self by namespace and name: under a cluster-wide list, two WDs in
		// different namespaces can share the same name, so name alone is not a
		// unique identity.
		if wd.Namespace == selfNamespace && wd.Name == selfName {
			continue
		}
		otherRef := wd.Spec.WorkerOptions.ConnectionRef
		// Same target only if BOTH name and (normalized) kind match, so a
		// namespaced Connection "foo" and a ClusterConnection "foo" are distinct.
		if ref.ResolvesTo(otherRef) {
			l.Info("Connection still referenced by another WorkerDeployment, keeping finalizer",
				"connection", ref.ResolvedName(), "clusterScoped", isCluster,
				"referencedBy", wd.Name, "referencedByNamespace", wd.Namespace)
			return nil
		}
	}

	// Fetch by the passed ref, not the WD's current connectionRef: during a
	// connectionRef change the WD's current ref points at the new connection, so
	// fetching by current ref would strip the finalizer off the wrong object.
	_, connObj, err := r.getConnectionByRef(ctx, ref, selfNamespace)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("unable to fetch connection %q: %w", ref.ResolvedName(), err)
	}

	if controllerutil.ContainsFinalizer(connObj, finalizerName) {
		l.Info("Removing finalizer from connection", "connection", ref.ResolvedName(), "clusterScoped", isCluster)
		controllerutil.RemoveFinalizer(connObj, finalizerName)
		if err := r.Update(ctx, connObj); err != nil {
			return fmt.Errorf("unable to remove finalizer from connection %q: %w", ref.ResolvedName(), err)
		}
	}

	return nil
}
