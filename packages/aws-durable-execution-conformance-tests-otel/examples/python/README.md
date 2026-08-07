# Python OpenTelemetry Conformance Examples

This SAM project implements the OpenTelemetry conformance scenarios with the
AWS Durable Execution SDK for Python and its OpenTelemetry plugin:

- [`aws-durable-execution-sdk-python`](https://pypi.org/project/aws-durable-execution-sdk-python/)
- [`aws-durable-execution-sdk-python-otel`](https://pypi.org/project/aws-durable-execution-sdk-python-otel/)

The project is intentionally self-contained so this directory can move into the
Python SDK's OpenTelemetry package once the suite is complete.

The runner discovers each requirement mapping from
`TestingMetadata.TestDescription`. The 19 invocation and 19 execution
requirements reuse the same scenario handlers and select their plugin through
deployment configuration.

## Scenarios

| Requirement | Handler | Behavior |
|---|---|---|
| `otel-invocation-1` | `otel_1_success.handler` | Verifies every successful step and attempt span. |
| `otel-invocation-2` | `otel_2_wait_resume.handler` | Verifies every wait, resume, and post-resume step span. |
| `otel-invocation-3` | `otel_3_retry.handler` | Verifies failed and successful retry attempts across invocations. |
| `otel-invocation-4` | `otel_4_terminal_failure.handler` | Verifies complete telemetry for a terminal execution failure. |
| `otel-invocation-5` | `otel_5_child_context.handler` | Verifies every child-context and nested-step span. |
| `otel-invocation-6` | `otel_6_parallel.handler` | Verifies every parallel context, branch, step, and attempt span. |
| `otel-invocation-7` | `otel_7_map.handler` | Verifies every map context, iteration, step, and attempt span. |
| `otel-invocation-8` | `otel_8_handled_failure.handler` | Verifies complete failed-step and recovery telemetry. |
| `otel-invocation-9` | `otel_9_wait_for_condition.handler` | Verifies every condition polling attempt and continuation. |
| `otel-invocation-10` | `otel_10_wait_for_callback.handler` | Verifies callback context, callback, and submitter spans. |
| `otel-invocation-11` | `otel_11_chained_invoke.handler` | Verifies chained-invoke continuation spans. |
| `otel-invocation-12` | `otel_12_child_context_failure.handler` | Verifies a failed child-context span. |
| `otel-invocation-13` | `otel_13_parallel_failure.handler` | Verifies failed parallel-branch telemetry. |
| `otel-invocation-14` | `otel_14_map_failure.handler` | Verifies failed map-iteration telemetry. |
| `otel-invocation-15` | `otel_15_wait_interrupted.handler` | Verifies an interrupted wait when execution times out. |
| `otel-invocation-16` | `otel_16_wait_for_condition_failure.handler` | Verifies failed condition-check telemetry. |
| `otel-invocation-17` | `otel_17_wait_for_callback_failure.handler` | Verifies external callback-failure telemetry. |
| `otel-invocation-18` | `otel_18_chained_invoke_failure.handler` | Verifies failed chained-invoke telemetry. |
| `otel-invocation-19` | `otel_19_execution_failure.handler` | Verifies telemetry for a direct handler failure. |
| `otel-execution-1` | `otel_1_success.handler` | Verifies the execution-view workflow, step, and attempt hierarchy. |
| `otel-execution-2` | `otel_2_wait_resume.handler` | Verifies the execution view across a resumed invocation. |
| `otel-execution-3` | `otel_3_retry.handler` | Verifies the execution view across retry attempts. |
| `otel-execution-4` | `otel_4_terminal_failure.handler` | Verifies the failed workflow, step, and attempt hierarchy. |
| `otel-execution-5` | `otel_5_child_context.handler` | Verifies child-context and nested-step parentage. |
| `otel-execution-6` | `otel_6_parallel.handler` | Verifies parallel context, branch, step, and attempt parentage. |
| `otel-execution-7` | `otel_7_map.handler` | Verifies map context, iteration, step, and attempt parentage. |
| `otel-execution-8` | `otel_8_handled_failure.handler` | Verifies failed and recovery operations under a successful workflow. |
| `otel-execution-9` | `otel_9_wait_for_condition.handler` | Verifies condition polling attempts across invocations. |
| `otel-execution-10` | `otel_10_wait_for_callback.handler` | Verifies callback, submitter, and attempt parentage. |
| `otel-execution-11` | `otel_11_chained_invoke.handler` | Verifies source and target workflow roots for a chained invoke. |
| `otel-execution-12` | `otel_12_child_context_failure.handler` | Verifies a failed child context under a failed workflow. |
| `otel-execution-13` | `otel_13_parallel_failure.handler` | Verifies a failed parallel branch under its operation. |
| `otel-execution-14` | `otel_14_map_failure.handler` | Verifies a failed map iteration under its operation. |
| `otel-execution-15` | `otel_15_wait_interrupted.handler` | Verifies a pending invocation when workflow spans do not complete. |
| `otel-execution-16` | `otel_16_wait_for_condition_failure.handler` | Verifies a failed condition operation and attempt. |
| `otel-execution-17` | `otel_17_wait_for_callback_failure.handler` | Verifies failed callback telemetry under one workflow. |
| `otel-execution-18` | `otel_18_chained_invoke_failure.handler` | Verifies source and target failed workflow roots. |
| `otel-execution-19` | `otel_19_execution_failure.handler` | Verifies a failed invocation without a completed workflow. |

Runtime dependencies in [`src/requirements.txt`](src/requirements.txt) install
both packages from the commit in `PYTHON_SDK_REF`. The hosted workflow resolves
the latest `main` commit once per run so every SAM function uses the same core
and OTel plugin revision.

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
  --suite otel-invocation otel-execution \
  --parameter-overrides LambdaExecutionRoleArn=arn:aws:iam::123456789012:role/example \
  --otel-exporter adot \
  --otel-layer-arn "$ADOT_LAYER_ARN" \
  --otel-service-name durable-execution-conformance \
  --otel-backend xray
```

Set `ADOT_LAYER_ARN` to the current regional ARN from the
[ADOT Python release](https://github.com/aws-observability/aws-otel-python-instrumentation/releases/latest).
The runner supplies all OTel SAM parameters. The execution role must allow
Durable Execution, logs, and X-Ray access.

## Run Against the AWS S3 Collector

The hosted S3 workflow builds and publishes a temporary OpenTelemetry Lambda
collector extension with `awss3exporter`, then creates a run-scoped S3 bucket.
The Python community instrumentation layer sends OTLP HTTP traffic to that
extension on `localhost:4318`, and the conformance backend reads the official
OTLP objects back from S3 for assertion:

```bash
durable-execution-conformance \
  --template packages/aws-durable-execution-conformance-tests-otel/examples/python/template.yaml \
  --language python \
  --suite otel-invocation otel-execution \
  --parameter-overrides \
    LambdaExecutionRoleArn=arn:aws:iam::123456789012:role/example \
    OtelCollectorLayerArn="$COLLECTOR_LAYER_ARN" \
    OtelCollectorBucket="$OTEL_S3_BUCKET" \
    OtelCollectorPrefix=traces \
  --otel-exporter community \
  --otel-endpoint http://localhost:4318 \
  --otel-backend collector \
  --otel-backend-endpoint "s3://$OTEL_S3_BUCKET/traces"
```

The collector config is packaged at
`/opt/collector-config/config-s3.yaml`. The function role needs prefix-scoped
S3 write access; the runner identity needs list, read, and cleanup access.

## Build Only

SAM uses `src/Makefile` to install both packages from one resolved SDK commit.
The explicit Makefile build avoids SAM's package metadata inspection, which
does not support these Git monorepo subdirectory dependencies. It also resolves
binary dependencies for Lambda's `manylinux2014_x86_64` platform when building
from macOS:

```bash
export PYTHON_SDK_REF=$(
  git ls-remote \
    https://github.com/aws/aws-durable-execution-sdk-python.git \
    refs/heads/main |
    awk '{print $1}'
)

sam build \
  --template-file packages/aws-durable-execution-conformance-tests-otel/examples/python/template.yaml
```
