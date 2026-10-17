// SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
//
// SPDX-License-Identifier: Apache-2.0
/**
 * insight-7: Default config; the same operation name is used for several
 * top-level steps. Step ids are sequence-based (not name-derived), so repeating
 * the name "task" yields three distinct operations that share one name. In the
 * `operationsByName` summary these aggregate to `count: 3`, and because the name
 * occurs more than once the summary drops `result`/`error` (no single
 * representative value).
 */

import { createInsightHandler } from "./common";

const REPEATS = 3;

export const handler = createInsightHandler({}, async (_event, context) => {
  const results: number[] = [];
  for (let i = 0; i < REPEATS; i++) {
    results.push(await context.step("task", async () => i));
  }
  return results;
});
