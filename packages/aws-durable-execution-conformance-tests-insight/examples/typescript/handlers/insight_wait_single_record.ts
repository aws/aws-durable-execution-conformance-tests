// SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
//
// SPDX-License-Identifier: Apache-2.0
/**
 * insight-5: Default config (`on-complete`) across a real wait/suspend/resume.
 * The execution suspends on the timer and resumes, but on-complete emits only
 * the single terminal record — no intermediate RUNNING record.
 */

import { createInsightHandler } from "./common";

export const handler = createInsightHandler({}, async (_event, context) => {
  await context.wait("pause", { seconds: 1 });
  return context.step("after-wait", async () => "done");
});
