// SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
//
// SPDX-License-Identifier: Apache-2.0

import {
  DurableContext,
  DurableExecutionHandler,
  DurableLambdaHandler,
  withDurableExecution,
} from "@aws/durable-execution-sdk-js";
import { InvocationOtelPlugin } from "@aws/durable-execution-sdk-js-otel";

export interface ScenarioEvent {
  scenario: string;
  [key: string]: unknown;
}

type Workflow<TResult> = (
  event: ScenarioEvent,
  context: DurableContext,
) => Promise<TResult>;

const plugin = new InvocationOtelPlugin({ useDefaultTracerProvider: true });

export function createScenarioHandler<TResult>(
  expectedScenario: string,
  workflow: Workflow<TResult>,
): DurableLambdaHandler {
  const handler: DurableExecutionHandler<ScenarioEvent, TResult> = async (
    event,
    context,
  ) => {
    requireScenario(event, expectedScenario);
    return workflow(event, context);
  };
  return withDurableExecution(handler, { plugins: [plugin] });
}

export function createTargetHandler<TResult>(
  workflow: Workflow<TResult>,
): DurableLambdaHandler {
  return withDurableExecution(workflow, { plugins: [plugin] });
}

function requireScenario(event: ScenarioEvent, expected: string): void {
  if (event.scenario !== expected) {
    throw new Error(
      `Expected scenario ${expected}, received ${String(event.scenario)}`,
    );
  }
}
