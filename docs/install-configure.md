This document explains how to install and configure Temporal Worker Controller.

# Install

<details open>
<summary>Install with Helm</summary>

> [!IMPORTANT]
> CRDs are shipped as a separate Helm chart so they can be upgraded
> independently of the controller. The CRD and controller Helm Chart share the
> same Chart Version.

Install the CRDs chart first:

```bash
export TWC_CHART_VERSION="<VERSION>"
helm install temporal-worker-controller-crds \
  oci://docker.io/temporalio/temporal-worker-controller-crds \
  --version <version>
```

> [!TIP]
> To find the latest Helm Chart version, run the
> [`hack/helm-chart-versions.sh`][helm-chart-versions] script.

then the controller chart:

```bash
export TWC_NAMESPACE="temporal-worker-controller"
helm install temporal-worker-controller \
  oci://docker.io/temporalio/temporal-worker-controller \
  --version <version> \
  --namespace "$TWC_NAMESPACE" \
  --create-namespace
```
</details>

> [!NOTE]
> See [docs/crd-management.md](docs/crd-management.md) for upgrade, rollback,
> and migration instructions involving the CRDs.

[helm-chart-versions]: ../hack/helm-chart-versions.sh

# Configure

Temporal Worker Controller is configured via command-line flags and environment
variables specified in the controller Deployment's Pod definition.

If you installed TWC using the Helm Chart, many of these command-line flags
have a corresponding field in the Helm Chart's [`values.yaml`][values-file]
file.

For example, the `--metrics-bind-address` command-line flag controls which port
the controller's metrics endpoint is published on. The `metrics.port` field in
the `values.yaml` file influences how that command-line flag is set in the
[`container.args`][metrics-port-arg] field of the controller's Deployment.

[values-file]: ../helm/temporal-worker-controller/values.yaml
[metrics-port]: https://github.com/temporalio/temporal-worker-controller/blob/4a08818ffbb0dbeb379130e8aea8444567f2f9e8/helm/temporal-worker-controller/values.yaml#L115
[metrics-port-arg]: https://github.com/temporalio/temporal-worker-controller/blob/4a08818ffbb0dbeb379130e8aea8444567f2f9e8/helm/temporal-worker-controller/templates/manager.yaml#L118-L122

In this section, we highlight the different ways you can configure Temporal
Worker Controller. We point to the associated Helm Chart `values.yaml` fields
that influence certain behaviour and note when an environment variable is
inspected for a value when a command-line flag is not supplied to the
controller.

## Metrics

You can configure Temporal Worker Controller to expose a `/metrics` endpoint
that exports core controller metrics.

Completely disable the metrics endpoint by setting the `metrics.enabled=false`
value when installing the controller with Helm.

The `--metrics-bind-address` command-line flag accepts a `host:port` string
address that the `/metrics` endpoint will be exposed on, defaulting to `:8080`.

Control which port the metrics endpoint uses with the `metrics.port` field in
the Helm Chart's `values.yaml` file.

By default, the `/metrics` endpoint uses HTTPS and access to it is gated by
[`kube-rbac-proxy`][kube-rbac-proxy] running as a sidecar in the controller's
Deployment Pod.

If you wish to expose the `/metrics` endpoint with no authentication or
authorization, set the `metrics.disableAuth=true` value when installing the
controller with Helm.

> [!CAUTION]
> Only disable authentication for the `/metrics` endpoint when network-level
> controls already restrict access.

[ctrlrt-metrics]: https://book-v1.book.kubebuilder.io/beyond_basics/controller_metrics
[kube-rbac-proxy]: https://github.com/brancz/kube-rbac-proxy

## Webhooks

Temporal Worker Controller runs two admission webhooks, one for the
`WorkerDeployment` custom resource and another for the `WorkerResourceTemplate`
[controller-generated resources](worker-resource-templates.md).

The WorkerDeployment webhook validates and defaults `WorkerDeployment` custom
resources on create and update.

Set the `webhook.enabled` (default: `false`) Helm chart value to enable this
webhook when installing or upgrading Temporal Worker Controller.

The WorkerResourceTemplate webhook validates `WorkerResourceTemplate` resources
on create, update and delete of a WorkerDeployment. This webhook cannot be
disabled.

### TLS configuration

Kubernetes requires all webhooks to serve HTTPS. Because the
WorkerResourceTemplate webhook is always on, the controller Pod **always**
needs a TLS certificate, regardless of the `webhook.enabled` setting. The
certificate is mounted from a Kubernetes Secret into the controller Pod.

The validating webhook requires TLS. Choose one of the following options.

<details open>
<summary>Option 1: `cert-manager` (recommended, default)</summary>

> [!IMPORTANT]
> cert-manager must be installed independently in the cluster before installing
> TWC.

When installing Temporal Worker Controller with `certmanager.enabled=true` (the
default), the Helm Chart creates an Issuer and Certificate resource.

`cert-manager` generates the TLS certificate and stores it in a Secret.

Install `cert-manager` (once per cluster):

```bash
helm install cert-manager jetstack/cert-manager \
  --namespace cert-manager \
  --create-namespace \
  --set crds.enabled=true
```

When installing Temporal Worker Controller, be sure to set the
`certmanager.enabled=true` value:

```bash
helm install temporal-worker-controller <chart> \
  --namespace temporal-system \
  --set certmanager.enabled=true
```
O
</details>

<details>
<summary>Option 2: Bring your own certificate</summary>

If you manage TLS certificates outside of `cert-manager`, create a Secret
containing the TLS private key and client certificate:

```bash
kubectl create secret tls my-webhook-cert \
  --cert=webhook.pem \
  --key=webhook-key.pem \
  --namespace temporal-system
```

When installing Temporal Worker Controller, set `certmanager.enabled=false`,
`webhook.certSecretName` and `certmanager.caBundle` values:

```bash
helm install temporal-worker-controller <chart> \
  --namespace temporal-system \
  --set certmanager.enabled=false \
  --set webhook.certSecretName=my-webhook-cert \
  --set certmanager.caBundle=$(base64 < ca.pem)
```

The `webhook.certSecretName` value (default: `webhook-server-cert`) controls
which Secret the controller pod mounts for TLS. The `caBundle` value tells the
Kubernetes API server which CA to trust when calling the webhook.

</details>

<details>
<summary>Option 3: Self-managed with `cert-manager`</summary>

If you use cert-manager but want to control the Secret name:

```bash
helm install temporal-worker-controller <chart> \
  --namespace temporal-system \
  --set certmanager.enabled=true \
  --set webhook.certSecretName=my-custom-cert-name
```

</details>

## Custom client authentication

Temporal Worker Controller loads client connections to a Temporal API endpoint
that are referenced in `WorkerDeployment.spec.workerOptions.connectionRef`.

The default client loader examines fields in the associated `ConnectionSpec` to
determine the hostport, the authentication mode and any TLS settings to use
when connecting to the Temporal API endpoint.

If this default behaviour doesn't meet your organization's needs, you can
programmatically control how Temporal Worker Controller loads Temporal SDK
client connections.

The `ConnectionSpec` struct contains a `MutualTLSSecretRef` and
`APIKeySecretRef` field, both of which are nullable. When both these fields are
null, TWC assumes a custom authentication mechanism will be used.

A `Connection` using custom auth (no Secret refs):

```yaml
apiVersion: temporal.io/v1alpha1
kind: Connection
metadata:
  name: production-temporal
spec:
  hostPort: "production.abc123.tmprl.cloud:7233"
```

> [!IMPORTANT]
> Programmatically controlling how Temporal Worker Controller loads Temporal
> SDK client connections requires that you inject custom code into the
> controller binary and "wrap" the binary, building a custom container image.

Customize Temporal SDK client construction using one of two mutually exclusive
methods:

- **Programmatically set SDK client options** —
  `WithConnectionClientOptionsFunc`: Pass a function that returns Temporal
`sdkclient.Options`. Use this option when you want your wrapper to use a custom
function that simply overrides the SDK client options. For example, if you want
to add a custom interceptor or a custom `HeadersProvider`. The pool still owns
Secret parsing, dialing, and the health check.
- **Full control** — `WithConnectionCreateClientFunc`: replace the entire
  client-creation path. The supplied function owns secret parsing, options
building, dialing, and health checking; the pool only caches the result. This
is the path for wrappers that need to reuse their own client constructor (e.g.
one that returns a ready-made client, not options).

<details>
<summary>Example: Programmatically add a custom interceptor</summary>

```go
r := controller.New(mgr,
    controller.WithConnectionClientOptionsFunc(func(opts sdkclient.Options) sdkclient.Options {
        opts.Interceptors = append(opts.Interceptors, myInterceptor)
        return opts
    }),
)
```

</details>

<details>
<summary>Example: Use a custom client constructor</summary>

```go
r := controller.New(mgr,
    controller.WithCreateClient(func(ctx context.Context, spec v1alpha1.ConnectionSpec, ns, k8sNs, identity string) (controller.CachedClient, error) {
        client, err := myClientConstructor(spec, ns, identity)
        if err != nil {
            return controller.CachedClient{}, err
        }
        return controller.CachedClient{
            Client:  client,
            IsValid: func() bool { return true }, // e.g. check cert/token expiry, not a readiness probe
        }, nil
    }),
)
```

</details>

> [!IMPORTANT]
> - Custom auth is derived from the absence of `mutualTLSSecretRef` and
>   `apiKeySecretRef`; there is no `customAuth` field on the `Connection` CRD.
> - `WithConnectionCreateClientFunc` and `WithConnectionClientOptionsFunc` are
>   mutually exclusive.
> - With `WithConnectionClientOptionsFunc` the supplied function should wrap,
>   not replace, the pool's `HeadersProvider` so the `temporal-namespace`
>   header continues to ride every RPC (including system-level RPCs like
>   `GetSystemInfo`).
> - The pool owns the client cache and eviction in both cases. Creation success
>   is determined by the `CreateClientFunc`'s error return — if it returns an
>   error, the client is never cached. On a cache hit, the pool calls the
>   cached client's `IsValid` closure; returning false triggers eviction and
>   re-dial (the built-in path uses this for mTLS cert-expiry invalidation). On
>   transport-class failures the reconciler evicts the cached client as usual
>   so the next reconcile re-dials.
