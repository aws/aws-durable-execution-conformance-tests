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
  --otel-endpoint https://collector.example/v1/traces \
  --otel-backend-endpoint https://collector.example
```

The SDK test template must accept the non-secret parameters `OtelLayerArn`,
`OtelExecWrapper`, `OtelServiceName`, `OtelTracesExporter`, and, for OTLP,
`OtelExporterEndpoint`, `OtelSecretEnvironmentNames`, and a `NoEcho`
`OtelExporterHeaders` parameter mapped to `OTEL_EXPORTER_OTLP_HEADERS`.
Credentials and OTLP headers remain in environment variables or the CI secret
store; the runner redacts the secret parameter from commands and SAM output.

## Support Matrix

| Exporter | Backend | Credentials |
|---|---|---|
| ADOT | X-Ray | AWS credential chain |
| Community layer | Datadog | `DD_API_KEY`, `DD_APPLICATION_KEY` |
| Community layer | Dash0 | `DASH0_AUTH_TOKEN` |
| Community layer | Test collector | none |

Java, JavaScript/Node.js, and Python layer settings are included. Layer ARNs can
be overridden with `--otel-layer-arn` or the profile/runtime-specific
environment variables documented by `AdotExporterProfile` and
`CommunityExporterProfile`.

Run the deterministic collector with:

```bash
durable-execution-otel-collector --host 0.0.0.0 --port 4318
```

It accepts OTLP JSON or protobuf at `/v1/traces` and exposes its canonical,
in-memory trace lookup API at `/api/traces`.

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
