// SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
//
// SPDX-License-Identifier: Apache-2.0
/**
 * insight-14: `operationDetail: full-tree` in a single invocation with no
 * suspend. A named child context runs a named nested step; both appear in the
 * record and the child step's `parentId` equals the child context's `id`.
 * Because the whole tree is built in one invocation, no child preservation
 * (`childOperationsDepth`) is needed.
 */

import { createInsightHandler } from "./common";

export const handler = createInsightHandler(
  { operationDetail: "full-tree" },
  async (_event, context) =>
    context.runInChildContext("parent-context", async (childContext) =>
      childContext.step("child-step", async () => "child-complete"),
    ),
);
