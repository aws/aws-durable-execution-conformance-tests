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

## Run Against the S3 Collector

The hosted workflow builds a custom OpenTelemetry Lambda collector extension
with `awss3exporter`, publishes it in the test account, and creates a
run-scoped S3 bucket. It then runs all 19 scenarios with the community
JavaScript instrumentation layer and queries the exported OTLP objects through
the conformance package's `collector` backend.

After building the handlers and collector layer, the equivalent runner command
is:

```bash
durable-execution-conformance \
  --template packages/aws-durable-execution-conformance-tests-otel/examples/typescript/template.yaml \
  --language javascript \
  --suite otel \
  --parameter-overrides \
    LambdaExecutionRoleArn=arn:aws:iam::123456789012:role/example \
    OtelCollectorLayerArn="$COLLECTOR_LAYER_ARN" \
    OtelCollectorBucket="$OTEL_S3_BUCKET" \
    OtelCollectorPrefix=traces \
  --otel-exporter community \
  --otel-endpoint http://localhost:4318 \
  --otel-service-name invocation \
  --otel-backend collector \
  --otel-backend-endpoint "s3://$OTEL_S3_BUCKET/traces"
```

The template adds both the JavaScript instrumentation layer selected by the
runner and `COLLECTOR_LAYER_ARN`. The collector layer's
`/opt/collector-config/config-s3.yaml` listens on localhost, writes
gzip-compressed OTLP JSON to the run prefix, and uses the function's AWS
credentials for S3.

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
