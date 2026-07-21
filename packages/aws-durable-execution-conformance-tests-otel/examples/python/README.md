# Python OpenTelemetry Conformance Examples

This SAM project implements the OpenTelemetry conformance scenarios with the
AWS Durable Execution SDK for Python and its OpenTelemetry plugin:

- [`aws-durable-execution-sdk-python`](https://pypi.org/project/aws-durable-execution-sdk-python/)
- [`aws-durable-execution-sdk-python-otel`](https://pypi.org/project/aws-durable-execution-sdk-python-otel/)

The project is intentionally self-contained so this directory can move into the
Python SDK's OpenTelemetry package once the suite is complete.

The runner discovers each requirement mapping from
`TestingMetadata.TestDescription`. The same `otel-N` prefix appears in the
handler module and deployed function name so cases are easy to correlate across
the template and source tree.

## Scenarios

| Requirement | Handler | Behavior |
|---|---|---|
| `otel-1` | `otel_1_success.handler` | Completes a successful durable step. |
| `otel-2` | `otel_2_wait_resume.handler` | Waits, resumes in another invocation, then completes a step. |
| `otel-3` | `otel_3_retry.handler` | Fails the first step attempt and succeeds on the retry. |
| `otel-4` | `otel_4_terminal_failure.handler` | Fails a step without retrying and terminates the execution. |

Runtime dependencies in [`src/requirements.txt`](src/requirements.txt) install
both packages directly from the SDK repository's `main` branch because the
OpenTelemetry plugin is evolving quickly. Pin the two Git requirements to the
same commit when a reproducible build is needed.

## Run Against X-Ray

Install the conformance runner and the OTel suite, configure AWS credentials,
then run:

```bash
pip install \
  aws-durable-execution-conformance-tests \
  aws-durable-execution-conformance-tests-otel

durable-execution-conformance \
  --template packages/aws-durable-execution-conformance-tests-otel/examples/python/template.yaml \
  --language python \
  --suite otel \
  --parameter-overrides LambdaExecutionRoleArn=arn:aws:iam::123456789012:role/example \
  --otel-exporter adot \
  --otel-layer-arn "$ADOT_LAYER_ARN" \
  --otel-backend xray
```

Set `ADOT_LAYER_ARN` to the current regional ARN from the
[ADOT Python release](https://github.com/aws-observability/aws-otel-python-instrumentation/releases/latest).
The runner supplies all OTel SAM parameters. The execution role must allow
Durable Execution, logs, and X-Ray access.

## Run Against The AWS S3 Collector

Start OpenTelemetry Collector Contrib with the package's
[`awss3exporter` configuration](../collector/config.yaml) on an HTTPS endpoint
reachable from the deployed Lambda functions. Pass its OTLP ingest endpoint
separately from the S3 location queried by the conformance backend:

```bash
durable-execution-conformance \
  --template packages/aws-durable-execution-conformance-tests-otel/examples/python/template.yaml \
  --language python \
  --suite otel \
  --parameter-overrides LambdaExecutionRoleArn=arn:aws:iam::123456789012:role/example \
  --otel-exporter community \
  --otel-backend collector \
  --otel-endpoint https://otel-collector.example/v1/traces \
  --otel-backend-endpoint s3://example-telemetry/durable-execution
```

`localhost` is not reachable from a Lambda function deployed by SAM. Use a
network-accessible collector endpoint for an end-to-end run. The collector
writes official `awss3exporter` OTLP files; this repository does not provide a
custom S3 exporter. The runner's AWS identity must be able to list the bucket
and read objects under the configured prefix.

## Build Only

SAM uses `src/Makefile` to install both packages from the SDK repository's
`main` branch. The explicit Makefile build avoids SAM's package metadata
inspection, which does not support these Git monorepo subdirectory dependencies.
It also resolves binary dependencies for Lambda's `manylinux2014_x86_64`
platform when building from macOS:

```bash
sam build \
  --template-file packages/aws-durable-execution-conformance-tests-otel/examples/python/template.yaml
```
