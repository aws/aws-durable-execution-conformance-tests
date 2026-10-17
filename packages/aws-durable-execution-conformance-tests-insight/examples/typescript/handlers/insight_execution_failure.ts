// SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
//
// SPDX-License-Identifier: Apache-2.0
/**
 * insight-2: Failed execution with default config. A named step fails with no
 * retry, so the operation ends FAILED and the failure propagates to a FAILED
 * execution whose record carries `error.name`.
 */

import { createInsightHandler } from "./common";
import { InsightTestError } from "./errors";

export const handler = createInsightHandler({}, async (_event, context) =>
  context.step(
    "failing-step",
    async () => {
      throw new InsightTestError("Intentional step failure");
    },
    { retryStrategy: () => ({ shouldRetry: false }) },
  ),
);
