# TypeScript OpenTelemetry Conformance Examples

This SAM project implements all OpenTelemetry conformance scenarios with the
AWS Durable Execution SDK for JavaScript and its OpenTelemetry plugin:

- [`@aws/durable-execution-sdk-js`](https://www.npmjs.com/package/@aws/durable-execution-sdk-js)
- [`@aws/durable-execution-sdk-js-otel`](https://www.npmjs.com/package/@aws/durable-execution-sdk-js-otel)

The project runs on Node.js 22 and bundles one CommonJS entry point per
scenario. The template maps `otel-1` through `otel-19` with
`TestingMetadata.TestDescription`; `otel-11` and `otel-18` also deploy durable
chained-invoke targets.

The OTel package's `InvocationOtelPlugin` API is newer than its latest npm
artifact, so `scripts/install-sdk-main.sh` builds and installs both SDK packages
from the repository's `main` branch before the examples are compiled.

## Scenarios

| Requirement | Handler | Behavior |
|---|---|---|
| `otel-1` | `otel_1_success.handler` | Successful step and attempt. |
| `otel-2` | `otel_2_wait_resume.handler` | Wait, resume, and post-resume step. |
| `otel-3` | `otel_3_retry.handler` | Failed and successful retry attempts. |
| `otel-4` | `otel_4_terminal_failure.handler` | Terminal step failure. |
| `otel-5` | `otel_5_child_context.handler` | Child context with a nested step. |
| `otel-6` | `otel_6_parallel.handler` | Parallel context, branches, and steps. |
| `otel-7` | `otel_7_map.handler` | Map context, iterations, and steps. |
| `otel-8` | `otel_8_handled_failure.handler` | Handled failed step and recovery step. |
| `otel-9` | `otel_9_wait_for_condition.handler` | Two condition polling attempts. |
| `otel-10` | `otel_10_wait_for_callback.handler` | Callback context, callback, and submitter. |
| `otel-11` | `otel_11_chained_invoke.handler` | Successful chained invoke. |
| `otel-12` | `otel_12_child_context_failure.handler` | Failed child context. |
| `otel-13` | `otel_13_parallel_failure.handler` | Failed parallel branch. |
| `otel-14` | `otel_14_map_failure.handler` | Failed map iteration. |
| `otel-15` | `otel_15_wait_interrupted.handler` | Wait interrupted by execution timeout. |
| `otel-16` | `otel_16_wait_for_condition_failure.handler` | Failed condition check. |
| `otel-17` | `otel_17_wait_for_callback_failure.handler` | External callback failure. |
| `otel-18` | `otel_18_chained_invoke_failure.handler` | Failed chained invoke. |
| `otel-19` | `otel_19_execution_failure.handler` | Direct handler failure. |

## Run Against X-Ray

Install the conformance packages, configure AWS credentials, and use the latest
AWS Distro for OpenTelemetry JavaScript layer:

```bash
pip install \
  aws-durable-execution-conformance-tests \
  aws-durable-execution-conformance-tests-otel

npm ci \
  --prefix packages/aws-durable-execution-conformance-tests-otel/examples/typescript
npm run install-sdk-main \
  --prefix packages/aws-durable-execution-conformance-tests-otel/examples/typescript
npm run build \
  --prefix packages/aws-durable-execution-conformance-tests-otel/examples/typescript

durable-execution-conformance \
  --template packages/aws-durable-execution-conformance-tests-otel/examples/typescript/template.yaml \
  --language javascript \
  --suite otel \
  --parameter-overrides LambdaExecutionRoleArn=arn:aws:iam::123456789012:role/example \
  --otel-exporter adot \
  --otel-layer-arn "$ADOT_JS_LAYER_ARN" \
  --otel-service-name invocation \
  --otel-backend xray
```

Set `ADOT_JS_LAYER_ARN` to the current regional ARN from the
[ADOT JavaScript release](https://github.com/aws-observability/aws-otel-js-instrumentation/releases/latest).
The template enables `/opt/otel-instrument`; the plugin uses the tracer provider
registered by that layer.

## Build Only

Node.js 22 or newer is required:

```bash
cd packages/aws-durable-execution-conformance-tests-otel/examples/typescript
npm ci
npm run install-sdk-main
npm run typecheck
npm run build
sam build --template-file template.yaml
```
