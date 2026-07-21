# AWS Durable Execution Conformance Tests

Language-neutral conformance requirements and a Python runner for AWS Durable
Execution SDKs.

## Workspace

This repository is a Hatch workspace containing two independently versioned
distributions:

```text
packages/
  aws-durable-execution-conformance-tests/
  aws-durable-execution-conformance-tests-otel/
```

The core distribution contains the runner, reports, and generic requirements.
The optional OTel distribution contributes the `otel` suite through the core
entry-point API and owns its protocol dependencies, exporter profiles, backend
adapters, models, parsers, validators, and requirement resources.

Installing only the core package does not install OpenTelemetry dependencies:

```bash
pip install aws-durable-execution-conformance-tests
```

Install the optional suite to make `--suite otel` available through the same
CLI:

```bash
pip install aws-durable-execution-conformance-tests-otel
```

The OTel `0.1.x` line requires core `>=0.2,<0.3`.

## Development

Install Hatch and run all commands from the repository root:

```bash
hatch run test:all
hatch run test:cov
hatch run types:check
hatch fmt --check packages
hatch run yaml:lint
hatch run dist:build
hatch run dist:check
```

Package-specific test commands are `hatch run test:core` and
`hatch run test:otel`. Each child package can also be built independently:

Run `hatch build` from either child package directory to build just that
distribution.

## Running Conformance

The runner accepts a SAM template whose functions map to requirement IDs with
`TestingMetadata.TestDescription`:

```bash
hatch run validate \
  --template path/to/template.yaml \
  --language python \
  --region us-west-2 \
  --suite step \
  --report console json
```

It builds and deploys the template, invokes each mapped function, validates the
execution result and history, and emits console, JSON, or JUnit reports.

## OpenTelemetry

The OTel package supports these v1 combinations:

| Exporter profile | Backend adapter |
|---|---|
| ADOT | X-Ray |
| OpenTelemetry community layer | Datadog |
| OpenTelemetry community layer | Dash0 |
| OpenTelemetry community layer | AWS S3 collector |

Java, JavaScript/Node.js, and Python profiles are included. Unsupported
combinations fail during argument validation, before SAM build or deployment.

```bash
hatch run validate \
  --template path/to/template.yaml \
  --language python \
  --suite otel \
  --otel-exporter community \
  --otel-backend collector \
  --otel-endpoint https://otel-collector.example/v1/traces \
  --otel-backend-endpoint s3://example-telemetry/durable-execution
```

Credentials are read only from the environment:

- Datadog: `DD_API_KEY`, `DD_APPLICATION_KEY`
- Dash0: `DASH0_AUTH_TOKEN`
- OTLP headers: `OTEL_EXPORTER_OTLP_HEADERS`
- S3 collector: the AWS credential chain
- X-Ray: the AWS credential chain

Secret values are redacted from diagnostics and artifacts. See the
[OTel package README](packages/aws-durable-execution-conformance-tests-otel/README.md)
for the template parameter contract and the prototype OpenTelemetry Collector
Contrib `awss3exporter` configuration.

The self-contained
[Python examples](packages/aws-durable-execution-conformance-tests-otel/examples/python/README.md)
map the current OTel requirements to handlers built from the Python SDK and
OTel plugin `main` branch.

## Extension API

Core extensions register the
`aws_durable_execution_conformance_tests.extensions` entry-point group. An
extension declares a compatible core version range and contributes named
requirement resource roots, CLI configuration, non-secret deployment
parameters, and post-execution validation hooks. Suite names and requirement
IDs must be globally unique.

Future OTel exporter profiles and backends register:

- `aws_durable_execution_conformance_tests_otel.exporters`
- `aws_durable_execution_conformance_tests_otel.backends`

Load failures, incompatible versions, unknown plugins, and collisions are
reported as actionable CLI errors.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Security issues should be reported
through the [AWS vulnerability reporting page](https://aws.amazon.com/security/vulnerability-reporting/).

## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
