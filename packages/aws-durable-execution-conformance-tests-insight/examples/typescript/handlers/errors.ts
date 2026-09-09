// SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
//
// SPDX-License-Identifier: Apache-2.0
/**
 * Shared typed error for the failure scenarios (§8b.2).
 *
 * The failure handlers throw a real Error subclass with a stable, non-generic
 * `name` so the requirements can assert an exact `error.name` instead of the
 * banned `error.name: '*'` wildcard. This is a genuine customer error type — no
 * flag is fabricated and nothing is done to force a particular record value.
 */
export class InsightTestError extends Error {
  override readonly name = "InsightTestError";
}
