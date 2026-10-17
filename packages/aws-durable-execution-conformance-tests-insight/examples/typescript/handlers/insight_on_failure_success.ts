// SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
//
// SPDX-License-Identifier: Apache-2.0
/**
 * insight-3: `emitMode: on-failure` with a succeeding execution. on-failure only
 * emits on a terminal FAILED status, so a successful run emits no record at all
 * (record_count: 0).
 */

import { createInsightHandler } from "./common";

export const handler = createInsightHandler(
  { emitMode: "on-failure" },
  async (event, context) =>
    context.step("greet", async () => `Hello, ${String(event)}!`),
);
