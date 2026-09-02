// SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
//
// SPDX-License-Identifier: Apache-2.0
/**
 * insight-16: Phase 2 truncation (drop WHOLE operations, oldest-first). Three
 * named steps run; `bulk-1` and `bulk-2` return genuinely oversized (~2 KB)
 * results opted into the record, while `bulk-3` returns a small value with NO
 * result override (so it is never a truncation candidate and stays untruncated).
 * The exporter's `maxRecordSizeBytes` is tuned so that even after Phase 1 drops
 * both opted-in results the record is still over the limit, forcing Phase 2 to
 * drop whole operations oldest-first — `bulk-1` first — while the newest op
 * `bulk-3` survives.
 *
 * Byte arithmetic behind maxRecordSizeBytes: 1100 (S3 / OPERATIONS_ARRAY sink,
 * render = identity; same per-field sizing as insight-12):
 *   - base identity fields ≈ 700 B; record-level `,"truncated":true` ≈ 17 B;
 *     a result-dropped op (`,"truncated":true`, no result) ≈ 215 B; an op with
 *     no result and no marker ≈ 198 B; `,"droppedOperations":N` ≈ 22 B.
 *   - full record ≈ 700 + 2×2210 (bulk-1,bulk-2 w/ result) + 198 (bulk-3) + 2
 *       ≈ 5320 B.
 * Limiter walk:
 *   - Phase 1 drops bulk-1 then bulk-2 results (each marked truncated). Record
 *       ≈ 700+17 + 215 + 215 + 198 + 2 ≈ 1347 B  → still over 1100.
 *   - Phase 2 drops bulk-1 (oldest) whole:
 *       ≈ 700+17+22 + bulk-2(215) + bulk-3(198) + 1 ≈ 1153 B  → still over 1100.
 *       drops bulk-2 whole:
 *       ≈ 700+17+22 + bulk-3(198) ≈ 937 B  → 937 ≤ 1100, FITS → stop.
 * Result: droppedOperations ≥ 1 (bulk-1 dropped → count 0), bulk-3 retained
 * (count 1) and NOT truncated (it never had a result), Phase 3 never reached so
 * droppedInput/droppedOutput are absent. The assertion only requires bulk-1
 * count 0 + bulk-3 count 1 + droppedOperations ≥ 1, so whether Phase 2 stops
 * after dropping only bulk-1 or also bulk-2 is immaterial — either way bulk-3
 * survives. Safe window ≈ [~940 (bulk-3 must fit) , ~1347 (Phase 2 must run)).
 * Value confirmed live.
 */

import { createInsightHandler } from "./common";

const OVERSIZED = "x".repeat(2000);

export const handler = createInsightHandler(
  {
    // Below the post-Phase-1 record size so whole operations must be dropped,
    // above bulk-3-alone so the newest operation survives. See arithmetic above.
    maxRecordSizeBytes: 1100,
    content: {
      operations: {
        overrides: [
          { operationName: "bulk-1", result: (result) => result },
          { operationName: "bulk-2", result: (result) => result },
        ],
      },
    },
  },
  async (_event, context) => {
    await context.step("bulk-1", async () => OVERSIZED);
    await context.step("bulk-2", async () => OVERSIZED);
    await context.step("bulk-3", async () => "ok");
    return "done";
  },
);
