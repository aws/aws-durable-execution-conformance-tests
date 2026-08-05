# Java OpenTelemetry Conformance Examples

This SAM project implements all OpenTelemetry conformance scenarios with the
AWS Durable Execution SDK for Java and its experimental OpenTelemetry plugin:

- [`aws-durable-execution-sdk-java`](https://central.sonatype.com/artifact/software.amazon.lambda.durable/aws-durable-execution-sdk-java)
- [`aws-durable-execution-sdk-java-plugin-otel`](https://central.sonatype.com/artifact/software.amazon.lambda.durable/aws-durable-execution-sdk-java-plugin-otel)

The project uses Java 21 at runtime, compiles to Java 17 bytecode, and packages
every handler in one shaded JAR. The template maps `otel-invocation-1` through
`otel-invocation-19` to `InvocationOtelPlugin` and `otel-execution-1` through
`otel-execution-19` to `ExecutionOtelPlugin`. Cases 11 and 18 also deploy
durable chained-invoke targets for each view.

The hosted Java workflow checks out the Java SDK repository's latest `main`,
installs its SDK and OTel plugin artifacts, and overrides the Maven project's
default released SDK version for both conformance views.

## Scenarios

| Requirement | Handler | Behavior |
|---|---|---|
| `otel-invocation-1` | `Otel1Success` | Successful step and attempt. |
| `otel-invocation-2` | `Otel2WaitResume` | Wait, resume, and post-resume step. |
| `otel-invocation-3` | `Otel3Retry` | Failed and successful retry attempts. |
| `otel-invocation-4` | `Otel4TerminalFailure` | Terminal step failure. |
| `otel-invocation-5` | `Otel5ChildContext` | Child context with a nested step. |
| `otel-invocation-6` | `Otel6Parallel` | Parallel context, branches, and steps. |
| `otel-invocation-7` | `Otel7Map` | Map context, iterations, and steps. |
| `otel-invocation-8` | `Otel8HandledFailure` | Handled failed step and recovery step. |
| `otel-invocation-9` | `Otel9WaitForCondition` | Two condition polling attempts. |
| `otel-invocation-10` | `Otel10WaitForCallback` | Callback context, callback, and submitter. |
| `otel-invocation-11` | `Otel11ChainedInvoke` | Successful chained invoke. |
| `otel-invocation-12` | `Otel12ChildContextFailure` | Failed child context. |
| `otel-invocation-13` | `Otel13ParallelFailure` | Failed parallel branch. |
| `otel-invocation-14` | `Otel14MapFailure` | Failed map iteration. |
| `otel-invocation-15` | `Otel15WaitInterrupted` | Wait interrupted by execution timeout. |
| `otel-invocation-16` | `Otel16WaitForConditionFailure` | Failed condition check. |
| `otel-invocation-17` | `Otel17WaitForCallbackFailure` | External callback failure. |
| `otel-invocation-18` | `Otel18ChainedInvokeFailure` | Failed chained invoke. |
| `otel-invocation-19` | `Otel19ExecutionFailure` | Direct handler failure. |

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
  --suite otel-invocation otel-execution \
  --parameter-overrides LambdaExecutionRoleArn=arn:aws:iam::123456789012:role/example \
  --otel-exporter adot \
  --otel-layer-arn "$ADOT_JAVA_LAYER_ARN" \
  --otel-service-name invocation \
  --otel-backend xray
```

The execution role must allow Durable Execution, logs, and X-Ray writes.

The template starts the ADOT Java agent and loads the Java SDK OTel plugin JAR
through `OTEL_JAVAAGENT_EXTENSIONS`. The plugin's no-argument constructor uses
the agent-initialized global tracer provider, and ADOT sends its OTLP spans
through the collector to X-Ray.

## Run Against the AWS S3 Collector

The hosted S3 workflow publishes a temporary OpenTelemetry Lambda collector
extension and run-scoped bucket. When `OTEL_EXPORTER_OTLP_ENDPOINT` is set, the
Java SDK plugin exports spans over OTLP gRPC to the extension on
`localhost:4317` using its explicit tracer-provider builder.

```bash
durable-execution-conformance \
  --template packages/aws-durable-execution-conformance-tests-otel/examples/java/template.yaml \
  --language java \
  --suite otel-invocation otel-execution \
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

Build and install the Java SDK repository's latest `main`, then export that
project version for both the direct Maven build and SAM's nested Maven build:

```bash
git clone --depth 1 --branch main \
  https://github.com/aws/aws-durable-execution-sdk-java.git \
  /tmp/aws-durable-execution-sdk-java

export JAVA_SDK_VERSION=$(
  mvn -B -q \
    --file /tmp/aws-durable-execution-sdk-java/pom.xml \
    -Dstyle.color=never \
    -Dexpression=project.version \
    -DforceStdout \
    help:evaluate
)

mvn -B -q \
  --file /tmp/aws-durable-execution-sdk-java/pom.xml \
  --projects sdk,otel-plugin \
  --also-make \
  -DskipTests \
  install

mvn -B package \
  --file packages/aws-durable-execution-conformance-tests-otel/examples/java/pom.xml

sam build \
  --template-file packages/aws-durable-execution-conformance-tests-otel/examples/java/template.yaml
```
