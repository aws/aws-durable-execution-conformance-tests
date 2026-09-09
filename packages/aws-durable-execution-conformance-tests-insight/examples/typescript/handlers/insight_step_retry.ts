// SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
//
// SPDX-License-Identifier: Apache-2.0
/**
 * insight-6: Default config with a real `retryStrategy`. The step throws on its
 * first attempt (keyed off `stepContext.attempt`) and succeeds on the second, so
 * the operation record's `attempt` reflects the retry and the by-name summary's
 * `maxAttempt` is 2. No hand-rolled retry loop — the SDK drives the retry.
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
