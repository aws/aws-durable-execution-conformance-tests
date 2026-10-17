// SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
//
// SPDX-License-Identifier: Apache-2.0
/**
 * Shared wiring for the Workflow Insight conformance examples.
 *
 * The sink exporter is selected from the environment exactly the way the OTel
 * examples select their plugin mode: an env var (`INSIGHT_SINK`) chooses between
 * the S3 exporter (`s3`) and the default Lambda-log / CloudWatch exporter
 * (`cloudwatch`). Every handler builds its plugin with `workflowInsight(...)`
 * using the SDK's real exporters — no fake exporter, no synthetic emission.
 */

import {
  DurableContext,
  DurableLambdaHandler,
  withDurableExecution,
} from "@aws/durable-execution-sdk-js";
import {
  InsightExporter,
  LambdaLogExporter,
  S3Exporter,
  workflowInsight,
  WorkflowInsightConfig,
} from "@aws/durable-execution-sdk-js-insight";

export type InsightWorkflow<TResult> = (
  event: unknown,
  context: DurableContext,
) => Promise<TResult>;

/**
 * Builds the sink exporter from the environment.
 *
 * - `INSIGHT_SINK=s3`  → {@link S3Exporter} writing to `INSIGHT_S3_BUCKET`
 *   under `INSIGHT_S3_PREFIX`. Flat (`partitioning: "none"`) so the runner's
 *   S3 sink can list every record for an execution under one prefix. This
 *   exporter emits the canonical `operations` array (OPERATIONS_ARRAY).
 * - `INSIGHT_SINK=cloudwatch` (default) → the built-in {@link LambdaLogExporter},
 *   which needs no extra IAM and emits the `operationsByName` map
 *   (OPERATIONS_BY_NAME) to the function's own CloudWatch log group.
 *
 * `maxRecordSizeBytes`, when provided, overrides the exporter's default size
 * limit so the size limiter engages against a genuinely oversized record
 * (used by the truncation scenario).
 */
export function createSinkExporter(
  overrides: { maxRecordSizeBytes?: number } = {},
): InsightExporter {
  const sink = process.env.INSIGHT_SINK ?? "cloudwatch";
  const sizeConfig =
    overrides.maxRecordSizeBytes !== undefined
      ? { maxRecordSizeBytes: overrides.maxRecordSizeBytes }
      : {};

  if (sink === "s3") {
    const bucket = process.env.INSIGHT_S3_BUCKET;
    if (!bucket) {
      throw new Error("INSIGHT_S3_BUCKET is required for the s3 sink");
    }
    const prefix = process.env.INSIGHT_S3_PREFIX;
    return new S3Exporter({
      bucket,
      ...(prefix ? { prefix } : {}),
      partitioning: "none",
      ...sizeConfig,
    });
  }

  if (sink !== "cloudwatch") {
    throw new Error(`Unsupported INSIGHT_SINK '${sink}' (expected s3 or cloudwatch)`);
  }

  return new LambdaLogExporter(sizeConfig);
}

/**
 * Wraps a workflow with `withDurableExecution` + `workflowInsight(config)`,
 * injecting the env-selected sink exporter. `maxRecordSizeBytes` is lifted out
 * of the insight config and applied to the exporter (the plugin has no such
 * field — truncation is per-exporter). `childOperationsDepth` is threaded onto
 * the core SDK's `pluginsConfig` so `full-tree` records survive suspend/resume.
 */
export function createInsightHandler<TResult>(
  config: Omit<WorkflowInsightConfig, "exporters"> & {
    maxRecordSizeBytes?: number;
  },
  workflow: InsightWorkflow<TResult>,
  options: { childOperationsDepth?: number } = {},
): DurableLambdaHandler {
  const { maxRecordSizeBytes, ...insightConfig } = config;
  const insight = workflowInsight({
    ...insightConfig,
    exporters: [createSinkExporter({ maxRecordSizeBytes })],
  });

  return withDurableExecution(workflow, {
    plugins: [insight],
    ...(options.childOperationsDepth !== undefined
      ? { pluginsConfig: { childOperationsDepth: options.childOperationsDepth } }
      : {}),
  });
}
