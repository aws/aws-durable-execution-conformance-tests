// SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
//
// SPDX-License-Identifier: Apache-2.0
/** Successful execution scenario for OTel requirement otel-1. */

import { createScenarioHandler } from "./common";

export const handler = createScenarioHandler(
  "success",
  async (_event, context) =>
    context.step("otel-success", async () => "success"),
);
