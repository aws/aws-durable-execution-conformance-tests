# Python OpenTelemetry Conformance Examples

This SAM project implements the OpenTelemetry conformance scenarios with the
AWS Durable Execution SDK for Python and its OpenTelemetry plugin:

- [`aws-durable-execution-sdk-python`](https://pypi.org/project/aws-durable-execution-sdk-python/)
- [`aws-durable-execution-sdk-python-otel`](https://pypi.org/project/aws-durable-execution-sdk-python-otel/)

The project is intentionally self-contained so this directory can move into the
Python SDK's OpenTelemetry package once the suite is complete.

The runner discovers each requirement mapping from the `otel-N` prefix on the
deployed Lambda function name. The same prefix appears in the handler module
name so cases are easy to correlate across the template and source tree.

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
  --otel-backend xray
```

The runner supplies the ADOT layer ARN and all OTel SAM parameters. The
execution role must allow Durable Execution, logs, and X-Ray access.

## Run Against The Test Collector

Start `durable-execution-otel-collector` on an HTTPS endpoint reachable from
the deployed Lambda functions. Pass the OTLP ingest endpoint separately from
the collector query endpoint:

```bash
durable-execution-conformance \
  --template packages/aws-durable-execution-conformance-tests-otel/examples/python/template.yaml \
  --language python \
  --suite otel \
  --parameter-overrides LambdaExecutionRoleArn=arn:aws:iam::123456789012:role/example \
  --otel-exporter community \
  --otel-backend collector \
  --otel-endpoint https://collector.example/v1/traces \
  --otel-backend-endpoint https://collector.example
```

`localhost` is not reachable from a Lambda function deployed by SAM. Use a
network-accessible collector endpoint for an end-to-end run.

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
