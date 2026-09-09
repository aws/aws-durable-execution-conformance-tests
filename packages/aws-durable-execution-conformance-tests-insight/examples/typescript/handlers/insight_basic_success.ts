// SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
//
// SPDX-License-Identifier: Apache-2.0
/**
 * insight-1: Basic successful execution with default config. One named STEP/Step
 * operation on its first attempt; input and output are echoed into the record.
 */

import { createInsightHandler } from "./common";

export const handler = createInsightHandler({}, async (event, context) =>
  context.step("greet", async () => `Hello, ${String(event)}!`),
);
