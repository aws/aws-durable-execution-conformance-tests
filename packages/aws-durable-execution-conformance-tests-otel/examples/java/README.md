# Java OpenTelemetry Conformance Examples

This SAM project implements all OpenTelemetry conformance scenarios with the
AWS Durable Execution SDK for Java and its experimental OpenTelemetry plugin:

- [`aws-durable-execution-sdk-java`](https://central.sonatype.com/artifact/software.amazon.lambda.durable/aws-durable-execution-sdk-java)
- [`aws-durable-execution-sdk-java-plugin-otel`](https://central.sonatype.com/artifact/software.amazon.lambda.durable/aws-durable-execution-sdk-java-plugin-otel)

The project uses Java 21 at runtime, compiles to Java 17 bytecode, and packages
every handler in one shaded JAR. The template maps `otel-1` through `otel-19`
with `TestingMetadata.TestDescription`; `otel-11` and `otel-18` also deploy
durable chained-invoke targets.

The hosted Java workflow checks out the Java SDK repository's latest `main`,
installs its SDK and OTel plugin artifacts, and overrides the Maven project's
default released SDK version for the conformance run.

## Scenarios

| Requirement | Handler | Behavior |
|---|---|---|
| `otel-1` | `Otel1Success` | Successful step and attempt. |
| `otel-2` | `Otel2WaitResume` | Wait, resume, and post-resume step. |
| `otel-3` | `Otel3Retry` | Failed and successful retry attempts. |
| `otel-4` | `Otel4TerminalFailure` | Terminal step failure. |
| `otel-5` | `Otel5ChildContext` | Child context with a nested step. |
| `otel-6` | `Otel6Parallel` | Parallel context, branches, and steps. |
| `otel-7` | `Otel7Map` | Map context, iterations, and steps. |
| `otel-8` | `Otel8HandledFailure` | Handled failed step and recovery step. |
| `otel-9` | `Otel9WaitForCondition` | Two condition polling attempts. |
| `otel-10` | `Otel10WaitForCallback` | Callback context, callback, and submitter. |
| `otel-11` | `Otel11ChainedInvoke` | Successful chained invoke. |
| `otel-12` | `Otel12ChildContextFailure` | Failed child context. |
| `otel-13` | `Otel13ParallelFailure` | Failed parallel branch. |
| `otel-14` | `Otel14MapFailure` | Failed map iteration. |
| `otel-15` | `Otel15WaitInterrupted` | Wait interrupted by execution timeout. |
| `otel-16` | `Otel16WaitForConditionFailure` | Failed condition check. |
| `otel-17` | `Otel17WaitForCallbackFailure` | External callback failure. |
| `otel-18` | `Otel18ChainedInvokeFailure` | Failed chained invoke. |
| `otel-19` | `Otel19ExecutionFailure` | Direct handler failure. |

## Run Against X-Ray

Install the conformance packages, configure AWS credentials, and use the ADOT
Java layer documented by the Java SDK:

```bash
pip install \
  aws-durable-execution-conformance-tests \
  aws-durable-execution-conformance-tests-otel

durable-execution-conformance \
  --template packages/aws-durable-execution-conformance-tests-otel/examples/java/template.yaml \
  --language java \
  --suite otel \
  --parameter-overrides LambdaExecutionRoleArn=arn:aws:iam::123456789012:role/example \
  --otel-exporter adot \
  --otel-layer-arn "$ADOT_JAVA_LAYER_ARN" \
  --otel-service-name invocation \
  --otel-backend xray
```

The execution role must allow Durable Execution, logs, and X-Ray writes.

The template accepts `OtelExecWrapper` for the runner's shared parameter
contract but intentionally does not set `AWS_LAMBDA_EXEC_WRAPPER`, so the
attached `AWSOpenTelemetryDistroJava` layer does not start its Java agent. The
Java SDK plugin creates the only tracer provider and ADOT's X-Ray UDP exporter
sends durable spans directly to the X-Ray daemon available in Lambda.

## Run Against the AWS S3 Collector

The hosted S3 workflow publishes a temporary OpenTelemetry Lambda collector
extension and run-scoped bucket. When `OTEL_EXPORTER_OTLP_ENDPOINT` is set, the
Java SDK plugin exports spans over OTLP gRPC to the extension on
`localhost:4317`; otherwise it retains the X-Ray UDP exporter used above.

```bash
durable-execution-conformance \
  --template packages/aws-durable-execution-conformance-tests-otel/examples/java/template.yaml \
  --language java \
  --suite otel \
  --parameter-overrides \
    LambdaExecutionRoleArn=arn:aws:iam::123456789012:role/example \
    OtelCollectorLayerArn="$COLLECTOR_LAYER_ARN" \
    OtelCollectorBucket="$OTEL_S3_BUCKET" \
    OtelCollectorPrefix=traces \
  --otel-exporter community \
  --otel-endpoint http://localhost:4317 \
  --otel-service-name invocation \
  --otel-backend collector \
  --otel-backend-endpoint "s3://$OTEL_S3_BUCKET/traces"
```

The collector writes gzip-compressed OTLP JSON objects. The conformance backend
queries and merges those S3 objects before evaluating the span assertions.

## Build Only

The Maven project uses released SDK artifacts and produces one shaded Lambda
JAR:

```bash
mvn -B package \
  --file packages/aws-durable-execution-conformance-tests-otel/examples/java/pom.xml

sam build \
  --template-file packages/aws-durable-execution-conformance-tests-otel/examples/java/template.yaml
```
