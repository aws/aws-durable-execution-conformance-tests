// SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
//
// SPDX-License-Identifier: Apache-2.0
/**
 * insight-15: `operationDetail: full-tree`, one named operation of each core
 * kind so the record covers every type/subType pair:
 *   - step            → STEP / Step
 *   - wait            → WAIT / Wait
 *   - wait-for-callback → CONTEXT / WaitForCallback
 *   - child context   → CONTEXT / RunInChildContext (+ nested STEP/Step child)
 *
 * The wait and callback suspend the execution; the child context runs last so
 * its nested step is created in the resuming invocation. `childOperationsDepth`
 * is set as a belt-and-suspenders so a preserved child survives resume in
 * full-tree mode regardless of ordering. The callback token is delivered by the
 * conformance harness (real wait-for-callback API — no self-completion hack).
 */

import { createInsightHandler } from "./common";

export const handler = createInsightHandler(
  { operationDetail: "full-tree" },
  async (_event, context) => {
    await context.step("cover-step", async () => "step-done");
    await context.wait("cover-wait", { seconds: 1 });
    await context.waitForCallback("cover-callback", async () => undefined);
    return context.runInChildContext("cover-child", async (childContext) =>
      childContext.step("cover-child-step", async () => "child-done"),
    );
  },
  { childOperationsDepth: 1 },
);
