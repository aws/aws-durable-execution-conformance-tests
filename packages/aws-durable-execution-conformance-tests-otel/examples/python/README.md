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
| `otel-1` | `otel_1_success.handler` | Verifies every successful step and attempt span. |
| `otel-2` | `otel_2_wait_resume.handler` | Verifies every wait, resume, and post-resume step span. |
| `otel-3` | `otel_3_retry.handler` | Verifies failed and successful retry attempts across invocations. |
| `otel-4` | `otel_4_terminal_failure.handler` | Verifies complete telemetry for a terminal execution failure. |
| `otel-5` | `otel_5_child_context.handler` | Verifies every child-context and nested-step span. |
| `otel-6` | `otel_6_parallel.handler` | Verifies every parallel context, branch, step, and attempt span. |
| `otel-7` | `otel_7_map.handler` | Verifies every map context, iteration, step, and attempt span. |
| `otel-8` | `otel_8_handled_failure.handler` | Verifies complete failed-step and recovery telemetry. |
| `otel-9` | `otel_9_wait_for_condition.handler` | Verifies every condition polling attempt and continuation. |
| `otel-10` | `otel_10_wait_for_callback.handler` | Verifies callback context, callback, and submitter spans. |
| `otel-11` | `otel_11_chained_invoke.handler` | Verifies chained-invoke continuation spans. |
| `otel-12` | `otel_12_child_context_failure.handler` | Verifies a failed child-context span. |
| `otel-13` | `otel_13_parallel_failure.handler` | Verifies failed parallel-branch telemetry. |
| `otel-14` | `otel_14_map_failure.handler` | Verifies failed map-iteration telemetry. |
| `otel-15` | `otel_15_wait_interrupted.handler` | Verifies an interrupted wait when execution times out. |
| `otel-16` | `otel_16_wait_for_condition_failure.handler` | Verifies failed condition-check telemetry. |
| `otel-17` | `otel_17_wait_for_callback_failure.handler` | Verifies external callback-failure telemetry. |
| `otel-18` | `otel_18_chained_invoke_failure.handler` | Verifies failed chained-invoke telemetry. |
| `otel-19` | `otel_19_execution_failure.handler` | Verifies telemetry for a direct handler failure. |

Runtime dependencies in [`src/requirements.txt`](src/requirements.txt) install
both packages directly from the SDK repository's `main` branch because the
OpenTelemetry plugin is evolving quickly. Pin the two Git requirements to the
same commit when a reproducible build is needed.

The template declares `otel-3` and `otel-9` as native expected failures while
[SDK PR #568](https://github.com/aws/aws-durable-execution-sdk-python/pull/568)
is pending. Both cases still run every assertion and become `EXPECTED_FAILED`
only when their exact known error is observed. Any other error remains
`FAILED`, while a passing case becomes `UNEXPECTED_PASSED`; either outcome
fails the workflow so the declaration cannot hide a regression or silently
outlive the SDK defect.

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
