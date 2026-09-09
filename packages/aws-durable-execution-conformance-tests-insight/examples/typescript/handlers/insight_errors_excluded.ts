// SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
//
// SPDX-License-Identifier: Apache-2.0
/**
 * insight-10: `content.operations.includeErrors: false`. A named step fails with
 * no retry, so the execution ends FAILED (the execution-level `error` is still
 * present), but the per-operation `error` is excluded from the operation record.
 */

import { createInsightHandler } from "./common";
import { InsightTestError } from "./errors";

export const handler = createInsightHandler(
  { content: { operations: { includeErrors: false } } },
  async (_event, context) =>
    context.step(
      "failing-step",
      async () => {
        throw new InsightTestError("Intentional step failure");
      },
      { retryStrategy: () => ({ shouldRetry: false }) },
    ),
);
