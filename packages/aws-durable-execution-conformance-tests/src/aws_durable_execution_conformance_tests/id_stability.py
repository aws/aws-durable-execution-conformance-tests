#
# SPDX-License-Identifier: Apache-2.0
"""Cross-run task-ID stability check for DAG-20 (DagIdStability).

The standard ``validate`` pipeline invokes each test description exactly
ONCE and matches its history with ``EventHistoryMatcher``, which is keyed on
``EventId`` -- a per-run sequence position assigned by the platform in
COMPLETION order. That makes a cross-run assertion ("this task has the same
Id in two independent runs with different completion orders") inexpressible
through the normal matcher: ``EventId`` itself is not comparable across two
runs whose completion order differs.

This script sidesteps that limitation entirely rather than extending the
matcher: it invokes the SAME deployed DagIdStability function TWICE --
``Input.swap=false`` then ``Input.swap=true``, forcing task ``a`` and ``b``
to swap which one completes first -- and, for each run, builds a
``{task_name: Id}`` map directly from the raw captured events (every event
already carries ``Id`` and ``Name``; see ``EventHistoryMatcher`` for the
existing per-run matching this deliberately does NOT reuse). It then asserts
every task name maps to the IDENTICAL ``Id`` value in both runs -- the direct
proof that ids are derived from the task name, not from completion order or a
monotonic counter.

Requires a stack with the DagIdStability function already deployed (e.g. via
``hatch run validate --no-cleanup ... --suite dag``, which leaves the stack up
when given ``--no-cleanup``).

Usage:
    hatch run id-stability --stack-name conformance-tests-xyz --region us-west-2
"""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

from aws_durable_execution_conformance_tests.clients import AwsClients
from aws_durable_execution_conformance_tests.sam import Invoker
from aws_durable_execution_conformance_tests.validate import (
    get_execution_history,
    get_execution_status,
)

_FUNCTION_LOGICAL_ID = "DagIdStability"
_TERMINAL_STATUSES = frozenset({"SUCCEEDED", "FAILED", "TIMED_OUT", "CANCELLED"})
_POLL_INTERVAL_SECONDS = 2.0
_POLL_TIMEOUT_SECONDS = 60.0


def _task_ids_from_events(events: list[dict[str, Any]]) -> dict[str, str]:
    """Build a ``{task_name: Id}`` map directly from raw captured events.

    Every event carries both ``Id`` (the task's underlying operation id) and
    ``Name`` (the task name). A task's id is stable across its own
    StepStarted/StepSucceeded pair (and any others), so the LAST write for a
    given name wins and is always consistent; this deliberately does not go
    through ``EventHistoryMatcher`` at all, since that matcher is keyed on
    ``EventId`` (a per-run sequence position), not task name.
    """
    ids: dict[str, str] = {}
    for event in events:
        name = event.get("Name")
        task_id = event.get("Id")
        if name and task_id and event.get("SubType") in ("Step", "Dag"):
            ids[name] = task_id
    return ids


def _invoke_and_wait(
    invoker: Invoker,
    aws_clients: AwsClients,
    swap: bool,
    tmp_dir: str,
) -> dict[str, Any]:
    """Async-invoke DagIdStability with the given swap value and poll to terminal.

    Returns the final execution history dict.
    """
    event_payload = {"Input": {"swap": swap}}
    event_file = str(Path(tmp_dir) / f"idstability_swap_{swap}_event.json")
    with open(event_file, "w") as f:
        json.dump(event_payload, f)

    inv_result = invoker.invoke_async(
        function_name=_FUNCTION_LOGICAL_ID,
        event_file_path=event_file,
    )
    response = json.loads(inv_result.output)
    execution_arn = response.get("DurableExecutionArn")
    if not execution_arn:
        raise RuntimeError(f"No DurableExecutionArn in async invocation response: {inv_result.output[:200]}")

    print(f"  swap={swap}: invoked, execution_arn={execution_arn}")

    deadline = time.time() + _POLL_TIMEOUT_SECONDS
    while time.time() < deadline:
        history = get_execution_history(execution_arn, aws_clients["lambda"])
        if history is None:
            raise RuntimeError(f"swap={swap}: failed to retrieve execution history")
        status = get_execution_status(history)
        if status in _TERMINAL_STATUSES:
            if status != "SUCCEEDED":
                raise RuntimeError(f"swap={swap}: execution ended {status}, expected SUCCEEDED")
            print(f"  swap={swap}: SUCCEEDED ({len(history.get('Events', history.get('events', [])))} events)")
            return history
        time.sleep(_POLL_INTERVAL_SECONDS)

    raise RuntimeError(f"swap={swap}: execution did not reach a terminal state within {_POLL_TIMEOUT_SECONDS}s")


def run_id_stability_check(stack_name: str, region: str) -> int:
    """Invoke DagIdStability twice with swap flipped and diff task ids.

    Returns:
        0 if every shared task name maps to the identical Id in both runs,
        1 otherwise (with mismatches printed to stderr).
    """
    aws_clients = AwsClients.create(region=region)
    invoker = Invoker(stack_name=stack_name, region=region, lambda_client=aws_clients["lambda"])

    with tempfile.TemporaryDirectory() as tmp_dir:
        print("--- Run 1: swap=false (a finishes first) ---")
        history_1 = _invoke_and_wait(invoker, aws_clients, swap=False, tmp_dir=tmp_dir)
        print("--- Run 2: swap=true (b finishes first) ---")
        history_2 = _invoke_and_wait(invoker, aws_clients, swap=True, tmp_dir=tmp_dir)

    events_1 = history_1.get("Events", history_1.get("events", []))
    events_2 = history_2.get("Events", history_2.get("events", []))
    ids_1 = _task_ids_from_events(events_1)
    ids_2 = _task_ids_from_events(events_2)

    expected_names = {"idstabilitydag", "root", "a", "b", "afterA", "afterB", "merge"}
    missing_1 = expected_names - ids_1.keys()
    missing_2 = expected_names - ids_2.keys()
    if missing_1 or missing_2:
        print(f"ERROR: run 1 missing task ids for {missing_1}; run 2 missing for {missing_2}", file=sys.stderr)
        return 1

    mismatches = [name for name in sorted(expected_names) if ids_1[name] != ids_2[name]]
    print("\n--- Task id comparison ---")
    for name in sorted(expected_names):
        marker = "✅" if ids_1[name] == ids_2[name] else "❌"
        print(f"  {marker} {name}: run1={ids_1[name]!r} run2={ids_2[name]!r}")

    if mismatches:
        print(
            f"\nFAILED: {len(mismatches)} task(s) had a DIFFERENT Id across the two runs "
            f"despite identical names: {mismatches}. This means task ids depend on "
            f"completion order (a counter-based regression), not just the task name.",
            file=sys.stderr,
        )
        return 1

    print(
        f"\nPASSED: all {len(expected_names)} task ids are IDENTICAL across both runs "
        f"despite 'a' and 'b' swapping completion order -- ids are name-based, not "
        f"completion-order-based."
    )
    return 0


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Verify DagIdStability (10-20) task ids are identical across two runs with swapped completion order.",
    )
    parser.add_argument("--stack-name", required=True, help="CloudFormation stack name with DagIdStability deployed.")
    parser.add_argument("--region", required=True, help="AWS region.")
    args = parser.parse_args(argv)

    exit_code = run_id_stability_check(stack_name=args.stack_name, region=args.region)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
