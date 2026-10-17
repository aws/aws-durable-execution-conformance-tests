// SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
//
// SPDX-License-Identifier: Apache-2.0
/**
 * insight-4: `emitMode: on-failure` with a failing execution. A named step fails
 * with no retry; on-failure emits exactly one FAILED record for the terminal
 * failure.
 */

import { createInsightHandler } from "./common";
import { InsightTestError } from "./errors";

export const handler = createInsightHandler(
  { emitMode: "on-failure" },
  async (_event, context) =>
    context.step(
      "failing-step",
      async () => {
        throw new InsightTestError("Intentional execution failure");
      },
      { retryStrategy: () => ({ shouldRetry: false }) },
    ),
);
