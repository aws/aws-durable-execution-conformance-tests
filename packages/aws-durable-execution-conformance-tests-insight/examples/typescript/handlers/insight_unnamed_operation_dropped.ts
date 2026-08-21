// SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
//
// SPDX-License-Identifier: Apache-2.0
/**
 * insight-17: Unnamed operations are dropped from the record. The workflow runs
 * one named step (`named-step`) and one deliberately UNNAMED step (the real SDK
 * `context.step(fn)` overload, with no name argument). The plugin skips any
 * operation without a name (`if (!op.name) continue`), so the emitted
 * `operations` array contains exactly one entry — `named-step`. No flag is
 * fabricated; the drop is the plugin's real name-filter reacting to a genuinely
 * unnamed operation.
 */

import { createInsightHandler } from "./common";

export const handler = createInsightHandler({}, async (_event, context) => {
  const named = await context.step("named-step", async () => "named");
  // Real unnamed-step overload (no name argument) — dropped from the record.
  const unnamed = await context.step(async () => "unnamed");
  return { named, unnamed };
});
