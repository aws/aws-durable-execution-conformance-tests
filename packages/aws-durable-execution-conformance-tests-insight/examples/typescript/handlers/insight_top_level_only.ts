// SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
//
// SPDX-License-Identifier: Apache-2.0
/**
 * insight-13: Default `operationDetail: top-level`. A parallel context with two
 * named branches (each running a named step) produces one top-level CONTEXT/
 * Parallel operation; every child (branches and their steps have a `parentId`)
 * is dropped, so the record contains only the parent context and no child ops.
 */

import { createInsightHandler } from "./common";

export const handler = createInsightHandler({}, async (_event, context) => {
  const result = await context.parallel<string>(
    "parallel-work",
    [
      {
        name: "branch-a",
        func: async (branch) => branch.step("branch-a-step", async () => "a"),
      },
      {
        name: "branch-b",
        func: async (branch) => branch.step("branch-b-step", async () => "b"),
      },
    ],
    { maxConcurrency: 1 },
  );
  return result.getResults();
});
