// SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
//
// SPDX-License-Identifier: Apache-2.0
/**
 * insight-12: Phase 1 truncation only (drop operation results, keep every
 * operation). Three named steps each return a genuinely oversized (~2 KB)
 * result and are opted into the record via overrides. The exporter's
 * `maxRecordSizeBytes` is tuned so the results do NOT all fit but the three
 * (result-stripped) operations DO — so the SDK's size limiter drops results
 * oldest-first (Phase 1) and never reaches whole-operation dropping (Phase 2).
 *
 * Byte arithmetic behind maxRecordSizeBytes: 4096 (S3 / OPERATIONS_ARRAY sink,
 * render = identity, so the measured shape is JSON.stringify(record)):
 *   - base identity fields (recordType/schemaVersion/arn/name/status/times/
 *     input/output + the "operations":[] framing) ≈ 700 B.
 *   - an operation record WITHOUT result ≈ 198 B; WITH a marker
 *     `,"truncated":true` ≈ 215 B; a ~2 KB result field ≈ 2012 B, so an
 *     operation WITH result ≈ 2210 B.
 *   - record-level `,"truncated":true` ≈ 17 B.
 * Limiter walk (candidate size, oldest-first: bulk-1, bulk-2, bulk-3):
 *   - full record ≈ 700 + 3×2210 + 2 ≈ 7332 B  → over 4096, drop bulk-1 result.
 *   - {bulk-1 stripped, bulk-2+bulk-3 results} ≈ 700+17+215+2210+2210+2 ≈ 5354 B
 *       → over 4096, drop bulk-2 result.
 *   - {bulk-1,bulk-2 stripped, bulk-3 result} ≈ 700+17+215+215+2210+2 ≈ 3359 B
 *       → 3359 ≤ 4096, FITS → stop. bulk-3 keeps its result; Phase 2 never runs.
 * So 4096 sits inside the [~3360, ~5350) window (drop 2 results, keep the 3rd),
 * with wide margin. Nothing about the flags is fabricated — they come from the
 * real limiter reacting to a real oversized record. Value confirmed live.
 */

import { createInsightHandler } from "./common";

const OVERSIZED = "x".repeat(2000);

export const handler = createInsightHandler(
  {
    // Large enough that all three result-stripped operations fit, small enough
    // that the three ~2 KB results do not — forcing Phase 1 (drop results) but
    // stopping before Phase 2 (drop whole operations). See arithmetic above.
    maxRecordSizeBytes: 4096,
    content: {
      operations: {
        overrides: [
          { operationName: "bulk-1", result: (result) => result },
          { operationName: "bulk-2", result: (result) => result },
          { operationName: "bulk-3", result: (result) => result },
        ],
      },
    },
  },
  async (_event, context) => {
    await context.step("bulk-1", async () => OVERSIZED);
    await context.step("bulk-2", async () => OVERSIZED);
    await context.step("bulk-3", async () => OVERSIZED);
    return "done";
  },
);
