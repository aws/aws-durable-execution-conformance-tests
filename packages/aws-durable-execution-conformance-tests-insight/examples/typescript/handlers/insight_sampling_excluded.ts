// SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
//
// SPDX-License-Identifier: Apache-2.0
/**
 * insight-8: `samplingRate: 0` samples every execution out, so a successful run
 * emits no records and makes no exporter calls (record_count: 0).
 */

import { createInsightHandler } from "./common";

export const handler = createInsightHandler(
  { samplingRate: 0 },
  async (event, context) =>
    context.step("greet", async () => `Hello, ${String(event)}!`),
);
