# AWS Durable Execution OpenTelemetry Conformance

Optional OpenTelemetry integration suites for
`aws-durable-execution-conformance-tests`. Installing this distribution adds
the `otel-invocation`, `otel-execution`, and `otel-long-running` suites to the
existing runner through a Python entry point; it does not install a second
conformance CLI.

## Install

```bash
pip install aws-durable-execution-conformance-tests-otel
```

The package requires a compatible `>=1.0,<2.0` core runner and owns all OTel
protocol dependencies, telemetry parsing, exporter profiles, backend adapters,
validators, and requirement resources. Core `0.2.0` introduced the extension
API used to discover these suites; this release of the package requires core
`1.x` and cannot be installed against core `0.2.x` or earlier.

The suites pair the same execution scenarios with view-specific telemetry
contracts. Invocation-view requirements assert spans emitted around each
Lambda invocation. Execution-view requirements assert the terminal `Workflow`
hierarchy and invocation links emitted across the durable execution.
The long-running suite applies both invocation and execution views to waits,
retry delays, callbacks, and chained invokes that can remain suspended for up
to one day. Python and JavaScript run both views; Java runs the invocation view
because its SDK does not provide `ExecutionOtelPlugin`. Dedicated daily X-Ray
workflows run at midnight PDT (07:00 UTC), launch a suite when no run is active,
and validate active executions on later runs. They can also be dispatched
manually to launch or check a run.

## Run

```bash
durable-execution-conformance \
  --template path/to/template.yaml \
  --language python \
  --suite otel-invocation otel-execution \
  --otel-exporter community \
  --otel-backend collector \
  --otel-endpoint https://otel-collector.example/v1/traces \
  --otel-backend-endpoint s3://example-telemetry/durable-execution
```

The SDK test template must accept the non-secret parameters `OtelLayerArn`,
`OtelExecWrapper`, `OtelServiceName`, `OtelTracesExporter`, and, for OTLP,
`OtelExporterEndpoint`, `OtelSecretEnvironmentNames`, and a `NoEcho`
`OtelExporterHeaders` parameter mapped to `OTEL_EXPORTER_OTLP_HEADERS`.
Templates that support the Lambda-hosted S3 collector also accept optional
`OtelCollectorLayerArn`, `OtelCollectorBucket`, and `OtelCollectorPrefix`
parameters.
`--otel-service-name` configures the OpenTelemetry resource identity used by
the deployed function and the backend lookup identity. The long-running
`launch` and `run` commands persist this value for their deferred validation.
Credentials and OTLP headers remain in environment variables or the CI secret
store; the runner redacts the secret parameter from commands and SAM output.

`TelemetryAssertions.span_assertions` can select one or an exact number of
canonical spans and assert any properties, nested attributes, parent
relationships, and timestamp ordering. Every span must start at or before it
ends, and every asserted parent must contain its child's complete timespan.
`before`, `after`, and `inside` compare a selected span with one other span.
`inside` can target a span that is not the selected span's parent.
`$linked: true` restricts the relation to spans linked by the selected span.
Complete-contract cases can require every plugin span and every attribute
under a stable prefix to be asserted. See the
[contribution guide](CONTRIBUTING.md#add-a-requirement) for the requirement
syntax and supported span fields.

## Support Matrix

| Exporter | Backend | Credentials |
|---|---|---|
| ADOT | X-Ray | AWS credential chain |
| Community layer | Datadog | `DATADOG_ACCESS_TOKEN`, `DATADOG_API_KEY` |
| Community layer | Dash0 | `DASH0_AUTH_TOKEN` |
| Community layer | AWS S3 collector | AWS credential chain |

Java, JavaScript/Node.js, and Python wrapper settings are included. Provide the
ADOT layer ARN with `--otel-layer-arn` or the runtime-specific
`ADOT_<RUNTIME>_LAYER_ARN` environment variable. The hosted integration
workflow discovers the latest Python layer from the ADOT release.

## Reusable Workflow

The hosted tests use one language-neutral reusable workflow for suites and
long-running views:

- `.github/workflows/opentelemetry-orchestrator.yml`

Language presets invoke it with repository and runtime metadata plus three
optional shell hooks. The orchestrator resolves test revisions once before
launching any workers:

- `setup_command` installs or selects any SDK toolchain.
- `contract_test_command` validates the example and template contract.
- `prepare_command` builds the handlers against the resolved SDK commit.

The hooks run in one Bash process after the optional SDK checkout, so setup
exports remain available to the contract and preparation commands. They receive
`SDK_CHECKOUT`, `SDK_REF`, and the shared workflow environment, including
`EXAMPLES_DIR`. A preset can therefore keep setup logic with its SDK:

```yaml
setup_command: bash "$SDK_CHECKOUT/.github/scripts/setup-conformance-toolchain.sh"
```

The shared workflows do not enumerate SDK toolchains. A future Go, Rust, or
other runtime can supply its own setup and build commands without changing the
orchestrators, workers, or setup action. For example, a Rust preset can select
its toolchain with `rustup`:

```yaml
jobs:
  conformance:
    uses: aws/aws-durable-execution-conformance-tests/.github/workflows/opentelemetry-orchestrator.yml@main
    with:
      language: rust
      resource_prefix: rs
      sdk_repository: example/aws-durable-execution-sdk-rust
      sdk_ref: ${{ github.sha }}
      setup_command: |
        rustup toolchain install stable --profile minimal
        rustup default stable
      prepare_command: >-
        cargo build --release --manifest-path "$EXAMPLES_DIR/Cargo.toml"
      adot_layer_arn: arn:aws:lambda:us-west-2:123456789012:layer:example-rust-adot:1
      collector_compatible_runtime: provided.al2023
      collector_otlp_endpoint: http://localhost:4318
    secrets: inherit
```

The runtime still needs conformance templates, exporter support, and test
handlers before its jobs can pass; adding that support does not require another
copy of the workflow.

## Dash0

The `dash0` backend queries Dash0's OTLP/JSON spans API with `POST /api/spans`.
Pass the regional API base URL through `--otel-backend-endpoint` or
`DASH0_API_URL`, and set `DASH0_AUTH_TOKEN` to a read-capable token. Set
`DASH0_DATASET` when traces are stored outside the organization's default
dataset. The backend first locates a correlated span by service name and
durable execution ARN, then retrieves every span in that trace with adaptive
sampling disabled.

The hosted Java, Python, and JavaScript suite workflows run Dash0 beside X-Ray
using the `us-west-2` Dash0 API and ingress endpoints. They use
`DASH0_AUTH_TOKEN` for both queries and the standard OTLP authorization header.

## Datadog

The `datadog` backend queries `POST /api/v2/spans/events/search`. Set
`DATADOG_ACCESS_TOKEN` to a read-capable OAuth access token. Pass a non-default
API base URL through `--otel-backend-endpoint`, or select another Datadog site
with `DD_SITE`. The backend locates a correlated span by service name and
durable execution ARN, follows cursor pagination, and accumulates newly indexed
spans across polling attempts. Conformance spans carry the execution ARN used
by this query, so the backend does not need a second full-trace search.

The shared Java, Python, and JavaScript suite workflow uses the generic
`https://otlp.datadoghq.com` OTLP base endpoint and runs Datadog beside X-Ray
and Dash0. The JavaScript community exporter also receives the signal-specific
`https://otlp.datadoghq.com/v1/traces` endpoint because its Lambda layer does
not append that path to the base endpoint. The workflow reads the API access
token from `DATADOG_ACCESS_TOKEN` and the intake API key from
`DATADOG_API_KEY`, formatting it as the `dd-api-key` OTLP header.

Before running a suite, the workflow uses `DATADOG_API_KEY` and the optional
`DATADOG_APPLICATION_KEY` to create or update a 100% APM retention filter for
`service:durable-execution-conformance`. Without the application key, that
filter must already exist in the Datadog account. Complete retention is
required because the suites validate every plugin span and cannot pass against
a sampled or partially indexed trace.

## AWS S3 Collector

The `collector` backend reads trace files written by the OpenTelemetry
Collector Contrib
[`awss3exporter`](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/exporter/awss3exporter).
This repository does not implement an exporter. The shared collector assets
live outside the SDK examples so those examples can consume the same builder
after they move to their SDK repositories. Start `otelcol-contrib` with the
included [configuration](collector/config.yaml):

```bash
AWS_REGION=us-west-2 \
OTEL_S3_BUCKET=example-telemetry \
OTEL_S3_PREFIX=durable-execution \
otelcol-contrib --config collector/config.yaml
```

The sample receives OTLP over HTTP or gRPC and uses the exporter's
`otlp_json` marshaler with gzip compression. The backend also supports
`otlp_proto` objects and the exporter's uncompressed, gzip, and zstd modes.
Pass the collector's reachable OTLP endpoint through `--otel-endpoint` and its
S3 destination through `--otel-backend-endpoint s3://bucket/prefix` (or
`OTEL_COLLECTOR_S3_URI`). The runner's AWS identity needs `s3:ListBucket` on
the bucket and `s3:GetObject` under the prefix; the collector identity needs
write access.

The stock OpenTelemetry Lambda collector layer does not include
`awss3exporter`. The shared
[`build-lambda-layer.sh`](collector/build-lambda-layer.sh) adds that
upstream component to a pinned `opentelemetry-lambda` checkout and builds a
custom extension layer containing `config-s3.yaml`. Separate Python, Java, and
JavaScript hosted workflows publish temporary language-compatible layer
versions, send each function's OTLP traffic to the local extension, query the
resulting S3 objects through the `collector` backend, and remove every
temporary stack, bucket, and layer version afterward.

## Python Examples

The package includes a self-contained
[Python SAM project](examples/python/README.md) that implements every OTel
requirement with the Python SDK and its OTel plugins. Its runtime requirements
install both packages from the Python SDK repository's latest `main`. The
folder is structured to move into the Python SDK's OTel package when this suite
stabilizes.

## Java Examples

The self-contained [Java SAM project](examples/java/README.md) implements the
invocation-view requirements with the Java SDK and its OTel plugin. Hosted
workflows build both artifacts from the Java SDK repository's latest `main`.
The project builds one shaded JAR containing all handlers and attaches the
`AWSOpenTelemetryDistroJava` layer with its Java agent disabled. The plugin
remains the sole tracer provider and selects Lambda's X-Ray daemon or an OTLP
gRPC endpoint from the deployment environment.

## JavaScript Examples

The self-contained
[JavaScript SAM project](examples/javascript/README.md), authored in
TypeScript, implements all OTel requirements on Node.js 22. It builds the
JavaScript SDK and OTel plugin from
their `main` branch, bundles the handlers, and exercises both
`InvocationOtelPlugin` and `ExecutionOtelPlugin` with the tracer provider
registered by the Lambda instrumentation layer.

## Third-Party Plugins

Additional profiles and backends register entry points in:

- `aws_durable_execution_conformance_tests_otel.exporters`
- `aws_durable_execution_conformance_tests_otel.backends`

Names must be unique. A backend factory exposes `name` and
`create(options, region=...)`; an exporter profile exposes `name`,
`supported_backends`, and `configure(options)`.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidance on adding OTel requirements,
SDK test handlers, provider-neutral assertions, and test coverage.
