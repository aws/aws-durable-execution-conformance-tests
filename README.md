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
The optional OTel distribution contributes the `otel-invocation` and
`otel-execution` suites through the core entry-point API and owns its protocol
dependencies, exporter profiles, backend adapters, models, parsers, validators,
and requirement resources.

Installing only the core package does not install OpenTelemetry dependencies:

```bash
pip install aws-durable-execution-conformance-tests
```

Install the optional distribution to make the OTel suites available through
the same CLI:

```bash
pip install aws-durable-execution-conformance-tests-otel
```

The OTel `0.2.x` line requires core `>=1.0,<2.0`.

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
  --suite otel-invocation otel-execution otel-long-running \
  --otel-exporter community \
  --otel-backend collector \
  --otel-endpoint https://otel-collector.example/v1/traces \
  --otel-backend-endpoint s3://example-telemetry/durable-execution
```

Credentials are read only from the environment:

- Datadog search API: `DATADOG_ACCESS_TOKEN`
- Datadog OTLP intake in hosted workflows: `DATADOG_API_KEY`
- Optional automated Datadog retention setup: `DATADOG_APPLICATION_KEY`
- Dash0: `DASH0_AUTH_TOKEN`
- OTLP headers: `OTEL_EXPORTER_OTLP_HEADERS`
- S3 collector: the AWS credential chain
- X-Ray: the AWS credential chain

When `DATADOG_APPLICATION_KEY` is not configured, the Datadog account must
already have a 100% APM retention filter for
`service:durable-execution-conformance`.

Secret values are redacted from diagnostics and artifacts. See the
[OTel package README](packages/aws-durable-execution-conformance-tests-otel/README.md)
for the template parameter contract and the prototype OpenTelemetry Collector
Contrib `awss3exporter` configuration.

The self-contained
[Python examples](packages/aws-durable-execution-conformance-tests-otel/examples/python/README.md)
map the current OTel requirements to deployable SDK handlers.

### Reuse the GitHub Actions workflow

SDK repositories can call the hosted
[`opentelemetry-orchestrator.yml`](.github/workflows/opentelemetry-orchestrator.yml)
workflow instead of copying its suite and long-running jobs. Pin the workflow
to a full commit SHA so all callers use a reviewed revision. The
[Python SDK workflow](https://github.com/aws/aws-durable-execution-sdk-python/blob/main/.github/workflows/opentelemetry-conformance-tests.yml)
is a complete caller reference; its reusable job is equivalent to:

```yaml
permissions: {}

jobs:
  opentelemetry:
    permissions:
      actions: write
      contents: read
      id-token: write
    uses: aws/aws-durable-execution-conformance-tests/.github/workflows/opentelemetry-orchestrator.yml@397d523d01bdf97ff8461ab749ddaa445bbf67ca
    with:
      language: python
      resource_prefix: p
      sdk_repository: aws/aws-durable-execution-sdk-python
      sdk_ref: ${{ github.event.pull_request.head.sha || github.sha }}
      conformance_test_ref: ${{ inputs.conformance_test_ref || 'main' }}
      checkout_sdk: false
      contract_test_command: >-
        hatch run test:all
        packages/aws-durable-execution-conformance-tests-otel/tests/test_python_examples.py
      adot_release_repository: aws-observability/aws-otel-python-instrumentation
      collector_compatible_runtime: python3.13
      collector_otlp_endpoint: http://localhost:4318
      suite_timeout_minutes: 30
      phase: ${{ inputs.phase || 'short' }}
      delay_seconds: ${{ inputs.delay_seconds || '82800' }}
      aws_region: ${{ inputs.aws_region || 'us-west-2' }}
    secrets:
      CONFORMANCE_TEST_ROLE_ARN: ${{ secrets.TEST_ROLE_ARN }}
      CONFORMANCE_TEST_ACCOUNT_ID: ${{ secrets.TEST_ACCOUNT_ID }}
      CONFORMANCE_TEST_LAMBDA_EXECUTION_ROLE_ARN: ${{ secrets.TEST_LAMBDA_EXECUTION_ROLE_ARN }}
      DASH0_AUTH_TOKEN: ${{ secrets.DASH0_AUTH_TOKEN }}
      DATADOG_ACCESS_TOKEN: ${{ secrets.DATADOG_ACCESS_TOKEN }}
      DATADOG_API_KEY: ${{ secrets.DATADOG_API_KEY }}
      DATADOG_APPLICATION_KEY: ${{ secrets.DATADOG_APPLICATION_KEY }}
```

Use `phase: short` for pull requests and pushes. For day-scale tests, expose
`launch` and `check` as `workflow_dispatch` choices and pass the same
`delay_seconds`, region, and conformance revision to both runs. The workflow
stores launch state as an artifact in the caller repository, so the checking
run needs `actions: write`.

Set `checkout_sdk: true` when `setup_command` or `prepare_command` needs an SDK
checkout. Set it to `false` when the language example resolves the SDK through
`sdk_repository` and `sdk_ref`, as Python does. Supply either
`adot_release_repository` for layer discovery or a fixed `adot_layer_arn`.
`DATADOG_APPLICATION_KEY` is optional only when the Datadog account already has
the required 100% retention filter.

See the
[OTel reusable workflow guide](packages/aws-durable-execution-conformance-tests-otel/README.md#reusable-workflow)
for language setup and preparation hooks.

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
