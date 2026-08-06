// SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
//
// SPDX-License-Identifier: Apache-2.0
/**
 * insight-9: `content.input: false` and `content.output: false` omit the
 * execution input and output from the record via configuration. Because the
 * fields are omitted (not size-dropped), `droppedInput`/`droppedOutput` are
 * absent — distinguishing a config omission from a truncation drop.
 */

import { createInsightHandler } from "./common";

export const handler = createInsightHandler(
  { content: { input: false, output: false } },
  async (event, context) =>
    context.step("greet", async () => `Hello, ${String(event)}!`),
);
