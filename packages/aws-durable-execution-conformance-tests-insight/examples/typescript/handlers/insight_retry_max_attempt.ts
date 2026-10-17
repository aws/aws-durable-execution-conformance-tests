// SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
//
// SPDX-License-Identifier: Apache-2.0
/**
 * insight-18: `operationsByName` summary `maxAttempt`. Same real `retryStrategy`
 * idiom as insight-6 — the step throws on its first attempt (keyed off
 * `stepContext.attempt`) and succeeds on the second, so the SDK drives one real
 * retry. This scenario is asserted through the by-name view (OPERATIONS_BY_NAME
 * sink): `retried-step` aggregates to `count: 1`, `failedCount: 0` (the
 * operation ultimately SUCCEEDED), and `maxAttempt: 2` (highest attempt seen).
 * No hand-rolled retry loop — the SDK's retryStrategy is the only retry driver.
 */

import { createInsightHandler } from "./common";

export const handler = createInsightHandler({}, async (_event, context) =>
  context.step(
    "retried-step",
    async (stepContext) => {
      if (stepContext.attempt === 1) {
        throw new Error("Intentional first-attempt failure");
      }
      return "retried";
    },
    {
      retryStrategy: (_error, attempt) =>
        attempt < 2
          ? { shouldRetry: true, delay: { seconds: 1 } }
          : { shouldRetry: false },
    },
  ),
);
