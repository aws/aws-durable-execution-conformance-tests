// SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
//
// SPDX-License-Identifier: Apache-2.0
/** Retried execution-view operation for OTel requirement otel-22. */

import { createScenarioHandler } from "./common";

export const handler = createScenarioHandler("retry", async (_event, context) =>
  context.step(
    "otel-retry",
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
