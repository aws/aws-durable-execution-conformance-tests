// SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
//
// SPDX-License-Identifier: Apache-2.0
/**
 * insight-11: Operation results are omitted by default; a `content.operations`
 * override opts the "compute" operation in with an identity transform (the real
 * opt-in API). The transform receives the checkpointed, JSON-parsed result, so
 * the emitted operation `result` equals the checkpointed value (42).
 */

import { createInsightHandler } from "./common";

export const handler = createInsightHandler(
  {
    content: {
      operations: {
        overrides: [{ operationName: "compute", result: (result) => result }],
      },
    },
  },
  async (_event, context) => context.step("compute", async () => 42),
);
