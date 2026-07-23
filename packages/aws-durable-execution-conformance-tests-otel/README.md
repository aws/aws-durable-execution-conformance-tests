# AWS Durable Execution OpenTelemetry Conformance

Optional OpenTelemetry integration suite for
`aws-durable-execution-conformance-tests`. Installing this distribution adds
the `otel` suite to the existing runner through a Python entry point; it does
not install a second conformance CLI.

## Install

```bash
pip install aws-durable-execution-conformance-tests-otel
```

The package requires a compatible `>=0.2,<0.3` core runner and owns all OTel
protocol dependencies, telemetry parsing, exporter profiles, backend adapters,
validators, and requirement resources. Core `0.2.0` introduces the extension
API used to discover this suite; core `0.1.x` cannot load it.

## Run

```bash
durable-execution-conformance \
  --template path/to/template.yaml \
  --language python \
  --suite otel \
  --otel-exporter community \
  --otel-backend collector \
  --otel-endpoint https://otel-collector.example/v1/traces \
  --otel-backend-endpoint s3://example-telemetry/durable-execution
```

The SDK test template must accept the non-secret parameters `OtelLayerArn`,
`OtelExecWrapper`, `OtelServiceName`, `OtelTracesExporter`, and, for OTLP,
`OtelExporterEndpoint`, `OtelSecretEnvironmentNames`, and a `NoEcho`
`OtelExporterHeaders` parameter mapped to `OTEL_EXPORTER_OTLP_HEADERS`.
Credentials and OTLP headers remain in environment variables or the CI secret
store; the runner redacts the secret parameter from commands and SAM output.

`TelemetryAssertions.span_assertions` can select one or an exact number of
canonical spans and assert any properties, nested attributes, and parent
relationships. Complete-contract cases can require every plugin span and every
attribute under a stable prefix to be asserted. See the
[contribution guide](CONTRIBUTING.md#add-a-requirement) for the requirement
syntax and supported span fields.

## Support Matrix

| Exporter | Backend | Credentials |
|---|---|---|
| ADOT | X-Ray | AWS credential chain |
| Community layer | Datadog | `DD_API_KEY`, `DD_APPLICATION_KEY` |
| Community layer | Dash0 | `DASH0_AUTH_TOKEN` |
| Community layer | AWS S3 collector | AWS credential chain |

Java, JavaScript/Node.js, and Python wrapper settings are included. Provide the
ADOT layer ARN with `--otel-layer-arn` or the runtime-specific
`ADOT_<RUNTIME>_LAYER_ARN` environment variable. The hosted integration
workflow discovers the latest Python layer from the ADOT release.

## AWS S3 Collector Prototype

The `collector` backend reads trace files written by the OpenTelemetry
Collector Contrib
[`awss3exporter`](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/exporter/awss3exporter).
This repository does not implement an exporter. Start `otelcol-contrib` with
the included [configuration](examples/collector/config.yaml):

```bash
AWS_REGION=us-west-2 \
OTEL_S3_BUCKET=example-telemetry \
OTEL_S3_PREFIX=durable-execution \
otelcol-contrib --config examples/collector/config.yaml
```

The sample receives OTLP over HTTP or gRPC and uses the exporter's
`otlp_json` marshaler with gzip compression. The backend also supports
`otlp_proto` objects and the exporter's uncompressed, gzip, and zstd modes.
Pass the collector's reachable OTLP endpoint through `--otel-endpoint` and its
S3 destination through `--otel-backend-endpoint s3://bucket/prefix` (or
`OTEL_COLLECTOR_S3_URI`). The runner's AWS identity needs `s3:ListBucket` on
the bucket and `s3:GetObject` under the prefix; the collector identity needs
write access.

This is a query-side prototype and is not wired into the hosted integration
workflow.

## Python Examples

The package includes a self-contained
[Python SAM project](examples/python/README.md) that implements every OTel
requirement with the Python SDK and its OTel plugin. Its runtime requirements
track both packages directly from the SDK repository's `main` branch. The
folder is structured to move into the Python SDK's OTel package when this suite
stabilizes.

## Java Examples

The self-contained [Java SAM project](examples/java/README.md) implements the
same OTel requirements with the Java SDK and its OTel plugin. It builds one
shaded JAR containing all handlers and uses the legacy ADOT Java layer as a
collector-only extension so the plugin remains the sole tracer provider. The
current Java agent path is deferred until the
[SDK compatibility fix](https://github.com/aws/aws-durable-execution-sdk-java/pull/540)
is available in a release.

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
