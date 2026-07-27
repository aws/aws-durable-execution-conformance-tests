# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Unit tests for the validate module.

Covers ``discover_suites`` and ``parse_not_implemented``; additional
pure-function tests for ``validate`` belong here too.
"""

from __future__ import annotations

from pathlib import Path

from aws_durable_execution_conformance_tests.callback import CallbackAction
from aws_durable_execution_conformance_tests.history import load_yaml_file
from aws_durable_execution_conformance_tests.validate import (
    _validate_event_count,
    discover_suites,
    discover_test_files,
    find_matching_action,
    parse_not_implemented,
)

# --- discover_suites --------------------------------------------------------


def _make_requirement(dir_path: Path, name: str) -> None:
    """Create a minimal requirement YAML file inside dir_path."""
    dir_path.mkdir(parents=True, exist_ok=True)
    (dir_path / name).write_text("---\ndescription: stub\n")


def test_callback_action_name_accepts_regex_matcher() -> None:
    action = CallbackAction(
        callback_name="${/^otel-callback(?: create callback id|-callback)$/}",
        operation="success",
    )

    assert find_matching_action(
        {"Name": "otel-callback-callback"},
        [action],
        set(),
    ) == (action, 0)


def test_discovers_folders_with_yaml(tmp_path: Path) -> None:
    """Only folders containing at least one YAML are returned."""
    _make_requirement(tmp_path / "step", "1-1.yaml")
    _make_requirement(tmp_path / "callback", "4-1.yaml")

    assert discover_suites(tmp_path) == ["callback", "step"]


def test_results_are_sorted(tmp_path: Path) -> None:
    """Suites are returned in sorted (deterministic) order."""
    for suite in ("wait", "invoke", "child", "step"):
        _make_requirement(tmp_path / suite, "x.yaml")

    assert discover_suites(tmp_path) == ["child", "invoke", "step", "wait"]


def test_discovers_non_operation_suites(tmp_path: Path) -> None:
    """Capability and integration suites are discovered like operation suites."""
    _make_requirement(tmp_path / "serdes", "10-1.yaml")
    _make_requirement(tmp_path / "otel", "11-1.yaml")

    assert discover_suites(tmp_path) == ["otel", "serdes"]


def test_ignores_empty_dirs(tmp_path: Path) -> None:
    """Directories with no YAML files are excluded."""
    _make_requirement(tmp_path / "step", "1-1.yaml")
    (tmp_path / "empty").mkdir()
    (tmp_path / "only_txt").mkdir()
    (tmp_path / "only_txt" / "notes.txt").write_text("not a requirement")

    assert discover_suites(tmp_path) == ["step"]


def test_finds_yaml_in_nested_subdirs(tmp_path: Path) -> None:
    """A YAML nested below the suite folder still counts."""
    nested = tmp_path / "map" / "sub"
    _make_requirement(nested, "9-1.yaml")

    assert discover_suites(tmp_path) == ["map"]


def test_ignores_top_level_files(tmp_path: Path) -> None:
    """Files directly under tests_dir are not mistaken for suites."""
    _make_requirement(tmp_path / "step", "1-1.yaml")
    (tmp_path / ".yamllint.yaml").write_text("rules: {}\n")

    assert discover_suites(tmp_path) == ["step"]


def test_missing_directory_returns_empty(tmp_path: Path) -> None:
    """A non-existent tests_dir yields an empty list, not an error."""
    assert discover_suites(tmp_path / "does_not_exist") == []


def test_empty_directory_returns_empty(tmp_path: Path) -> None:
    """An existing but empty tests_dir yields an empty list."""
    assert discover_suites(tmp_path) == []


def test_accepts_string_path(tmp_path: Path) -> None:
    """The helper accepts a str path as well as a Path."""
    _make_requirement(tmp_path / "step", "1-1.yaml")

    assert discover_suites(str(tmp_path)) == ["step"]


# --- parse_not_implemented --------------------------------------------------


def _write_template(tmp_path: Path, body: str) -> str:
    path = tmp_path / "template.yaml"
    path.write_text(body)
    return str(path)


def test_not_implemented_top_level_block(tmp_path: Path) -> None:
    template = _write_template(
        tmp_path,
        """
TestingMetadata:
  NotImplemented:
    - id: "8-13"
      reason: "toleratedFailurePercentage rejected at build()"
    - id: "8-22"
      reason: "same as 8-13"
Resources:
  ParallelBasic:
    Type: AWS::Serverless::Function
    TestingMetadata:
      TestDescription: ["8-1"]
""",
    )
    assert parse_not_implemented(template) == {
        "8-13": "toleratedFailurePercentage rejected at build()",
        "8-22": "same as 8-13",
    }


def test_not_implemented_on_resource(tmp_path: Path) -> None:
    template = _write_template(
        tmp_path,
        """
Resources:
  MapItemNamer:
    Type: AWS::Serverless::Function
    TestingMetadata:
      NotImplemented:
        - id: "9-14"
          reason: "MapConfig has no itemNamer field"
""",
    )
    assert parse_not_implemented(template) == {"9-14": "MapConfig has no itemNamer field"}


def test_not_implemented_first_reason_wins_on_duplicate(tmp_path: Path) -> None:
    template = _write_template(
        tmp_path,
        """
TestingMetadata:
  NotImplemented:
    - id: "8-13"
      reason: "first"
    - id: "8-13"
      reason: "second"
""",
    )
    assert parse_not_implemented(template) == {"8-13": "first"}


def test_not_implemented_missing_reason_defaults_empty(tmp_path: Path) -> None:
    template = _write_template(
        tmp_path,
        """
TestingMetadata:
  NotImplemented:
    - id: "8-13"
""",
    )
    assert parse_not_implemented(template) == {"8-13": ""}


def test_not_implemented_null_reason_defaults_empty(tmp_path: Path) -> None:
    # Explicit null value (key present, no value) must not become the string "None".
    template = _write_template(
        tmp_path,
        """
TestingMetadata:
  NotImplemented:
    - id: "8-13"
      reason:
""",
    )
    assert parse_not_implemented(template) == {"8-13": ""}


def test_not_implemented_absent_returns_empty(tmp_path: Path) -> None:
    template = _write_template(
        tmp_path,
        """
Resources:
  ParallelBasic:
    Type: AWS::Serverless::Function
    TestingMetadata:
      TestDescription: ["8-1"]
""",
    )
    assert parse_not_implemented(template) == {}


def test_not_implemented_ignores_entries_without_id(tmp_path: Path) -> None:
    template = _write_template(
        tmp_path,
        """
TestingMetadata:
  NotImplemented:
    - reason: "no id, ignored"
    - id: "8-13"
      reason: "kept"
""",
    )
    assert parse_not_implemented(template) == {"8-13": "kept"}


def test_not_implemented_tolerates_cfn_intrinsic_tags(tmp_path: Path) -> None:
    # The template loader must handle !GetAtt etc. without choking.
    template = _write_template(
        tmp_path,
        """
TestingMetadata:
  NotImplemented:
    - id: "8-13"
      reason: "gap"
Resources:
  ParallelBasic:
    Type: AWS::Serverless::Function
    Properties:
      Role: !GetAtt DurableFunctionRole.Arn
    TestingMetadata:
      TestDescription: ["8-1"]
""",
    )
    assert parse_not_implemented(template) == {"8-13": "gap"}


# --- _validate_event_count (ExpectedEventCount) -----------------------------
#
# ExpectedEventCount is the direct guard against a wrapper-per-task regression
# (flat N+1 -> 2N+1 operations) that the EventId-keyed history matcher cannot
# see. The cases below mirror the four the guard must cover: the count matches,
# the count mismatches, the history is omitted (no assertion requested), and
# the history is omitted but the count is present (assertion still enforced).


def _events(n: int) -> list[dict]:
    """Build a list of n minimal event dicts."""
    return [{"EventId": i} for i in range(1, n + 1)]


def test_event_count_matches() -> None:
    """Matching count with a full history present yields no errors."""
    description = {
        "ExpectedEventCount": 5,
        "ExpectedExecutionHistory": [{"EventId": 2}],
    }
    assert _validate_event_count(description, _events(5)) == []


def test_event_count_mismatches() -> None:
    """A mismatched count reports the expected and actual values."""
    description = {"ExpectedEventCount": 5}
    errors = _validate_event_count(description, _events(3))
    assert len(errors) == 1
    assert "ExpectedEventCount=5" in errors[0]
    assert "got 3" in errors[0]


def test_event_count_history_omitted_no_count_is_noop() -> None:
    """History omitted and no count key: no assertion, behaves as before.

    This is the additive guarantee for the nine existing non-DAG suites, none
    of which carry ExpectedEventCount.
    """
    assert _validate_event_count({}, _events(9)) == []
    assert _validate_event_count({"ExpectedResult": {"ExecutionStatus": "SUCCEEDED"}}, _events(9)) == []


def test_event_count_history_omitted_with_count_present() -> None:
    """Count present without any ExpectedExecutionHistory is still enforced."""
    matching = {"ExpectedEventCount": 17}
    assert _validate_event_count(matching, _events(17)) == []

    mismatching = {"ExpectedEventCount": 17}
    errors = _validate_event_count(mismatching, _events(19))
    assert len(errors) == 1
    assert "ExpectedEventCount=17" in errors[0]
    assert "got 19" in errors[0]


def test_event_count_zero_is_asserted_not_skipped() -> None:
    """An explicit 0 is a real assertion, not treated as absent."""
    assert _validate_event_count({"ExpectedEventCount": 0}, []) == []
    assert len(_validate_event_count({"ExpectedEventCount": 0}, _events(1))) == 1


def test_event_count_non_dict_description_is_noop() -> None:
    """Legacy bare-list history form never carries the key: no assertion."""
    assert _validate_event_count([{"EventId": 2}], _events(3)) == []


# --- 10-15 large-payload scenario loads and parses -------------------------
#
# The runner discovers requirements by file stem and parses them with
# load_yaml_file. These tests confirm the large-payload scenario is discoverable
# in the dag suite and that its parsed shape matches the contract post-envelope-
# convergence: an async description whose digest-equality result is one
# language-neutral assertion, PLUS a pinned ExpectedExecutionHistory that asserts
# the OFFLOADED container payload is the converged aggregate envelope WITHOUT
# `tasks` (its absence is the offload signal). It keeps ExpectedEventCount 26
# (MEASURED from the first cloud history, not derived), which the pinned envelope
# does not change because the envelope rides inside the existing container event.
# If the pinned envelope, the digest constants, or the count drifted, these
# assertions catch it before a cloud run.

_REQUIREMENTS_DIR = (
    Path(__file__).resolve().parents[1] / "test-requirements"
)


def test_dag_suite_discovers_10_15() -> None:
    """The dag suite exposes the new 10-15 requirement by its file stem."""
    files = discover_test_files(_REQUIREMENTS_DIR, suite="dag")

    assert "10-15" in files
    assert files["10-15"].endswith("dag/10-15.yaml")


def test_10_15_pins_offloaded_envelope_large_payload() -> None:
    """10-15 loads and pins the offloaded aggregate envelope (WITHOUT tasks)."""
    files = discover_test_files(_REQUIREMENTS_DIR, suite="dag")
    data = load_yaml_file(files["10-15"])

    # Async, and now carrying a pinned history for the offloaded container event.
    assert data["AsyncInvoke"] is True

    history = data["ExpectedExecutionHistory"]
    ctx_succeeded = [
        e
        for e in history
        if e.get("EventType") == "ContextSucceeded" and e.get("SubType") == "Dag"
    ]
    assert len(ctx_succeeded) == 1
    event = ctx_succeeded[0]
    assert event["EventId"] == 19
    assert event["Name"] == "bigdag"

    # The payload is JSON-decoded via the ${JSON} directive, then the converged
    # aggregate envelope is pinned WITHOUT tasks (tasks absence is the offload
    # signal) and the deleted-by-convergence fields are asserted absent.
    envelope = event["ContextSucceededDetails"]["Result"]["Payload"]["${JSON}"]
    assert envelope["type"] == "DagResult"
    assert envelope["totalCount"] == 8
    assert envelope["successCount"] == 8
    assert envelope["completionReason"] == "ALL_COMPLETED"
    assert envelope["startedTaskNames"] == []
    assert envelope["failedTaskNames"] == []
    assert envelope["tasks"] == "${ABSENT}"
    assert envelope["completedCount"] == "${ABSENT}"
    assert envelope["terminalTaskNames"] == "${ABSENT}"
    assert envelope["summary"] == "${ABSENT}"

    # ExpectedEventCount stays MEASURED at 26 (the pinned envelope rides inside
    # the existing container event and adds no events); it is still enforced.
    assert data["ExpectedEventCount"] == 26
    assert _validate_event_count(data, _events(26)) == []
    assert _validate_event_count(data, _events(999)) != []

    # The digest equality remains the complementary language-neutral assertion.
    result = data["ExpectedResult"]["Result"]
    assert result["reason"] == "ALL_COMPLETED"
    assert result["counts"] == [8, 0, 0, 8]
    assert result["digestBefore"] == "8:409600:abcdefgh"
    assert result["digestAfter"] == "8:409600:abcdefgh"
    assert result["match"] is True


# --- 10-16 DAG retry scenario loads and pins the container envelope --------
#
# 10-16 proves per-task retry works INSIDE a DAG (flaky fails twice then
# succeeds; after consumes the recovered result). It deliberately pins ONLY the
# container envelope (the single ContextSucceeded Dag event) and sets NO
# ExpectedEventCount, because a retried step's event sequence / count is a
# platform detail that moves with attempt counts and may differ per language.
# These tests confirm the scenario is discoverable, that its outcome and
# converged envelope are pinned as the contract requires, and that it does NOT
# pin a full history or an event count.


def test_dag_suite_discovers_10_16() -> None:
    """The dag suite exposes the new 10-16 requirement by its file stem."""
    files = discover_test_files(_REQUIREMENTS_DIR, suite="dag")

    assert "10-16" in files
    assert files["10-16"].endswith("dag/10-16.yaml")


def test_10_16_pins_retry_outcome_and_container_envelope() -> None:
    """10-16 pins {flaky:3, after:6} and the converged ALL_COMPLETED envelope."""
    files = discover_test_files(_REQUIREMENTS_DIR, suite="dag")
    data = load_yaml_file(files["10-16"])

    # Outcome: only reachable if flaky SUCCEEDED (returned attempt 3) and after
    # ran (returned 3 * 2 = 6) rather than being skipped.
    assert data["ExpectedResult"]["ExecutionStatus"] == "SUCCEEDED"
    assert data["ExpectedResult"]["Result"] == {"flaky": 3, "after": 6}

    # A retried step's event sequence differs per language and moves with attempt
    # counts, so the scenario must NOT pin an event count.
    assert "ExpectedEventCount" not in data

    # Exactly one event is pinned: the container envelope (ContextSucceeded Dag).
    history = data["ExpectedExecutionHistory"]
    assert len(history) == 1
    event = history[0]
    assert event["EventType"] == "ContextSucceeded"
    assert event["SubType"] == "Dag"
    assert event["Name"] == "retrydag"

    # The converged DagResult envelope: 2/2/0/0, ALL_COMPLETED, no failed tasks,
    # and the convergence-deleted fields asserted absent.
    envelope = event["ContextSucceededDetails"]["Result"]["Payload"]["${JSON}"]
    assert envelope["type"] == "DagResult"
    assert envelope["totalCount"] == 2
    assert envelope["successCount"] == 2
    assert envelope["failureCount"] == 0
    assert envelope["skippedCount"] == 0
    assert envelope["completionReason"] == "ALL_COMPLETED"
    assert envelope["failedTaskNames"] == []
    assert envelope["completedCount"] == "${ABSENT}"
    assert envelope["terminalTaskNames"] == "${ABSENT}"
    assert envelope["summary"] == "${ABSENT}"

    # Both tasks SUCCEEDED with their results; flaky recovered rather than
    # failing, and after ran rather than skipping.
    tasks = {t["name"]: t for t in envelope["tasks"]}
    assert set(tasks) == {"flaky", "after"}
    assert tasks["flaky"]["status"] == "SUCCEEDED"
    assert tasks["flaky"]["result"] == 3
    assert tasks["flaky"]["skipReason"] is None
    assert tasks["after"]["status"] == "SUCCEEDED"
    assert tasks["after"]["result"] == 6
    assert tasks["after"]["skipReason"] is None


# --- 10-17 nested-DAG large-payload scenario loads and pins the outer envelope -
#
# 10-17 covers the untested intersection of NESTING and LARGE PAYLOADS: a nested
# DAG whose inner aggregate exceeds the 256KB limit, so BOTH the inner and the
# outer container offload, and both are replayed across a suspend. The decisive
# proof is the digest equality (digestBefore == digestAfter == "6:307200:abcdef"
# with match: true), which shows the inner per-task detail survived the offload
# of both containers; innerReason / innerCounts are the canonical inner-aggregate
# fields asserted alongside it. All four SDK handlers return this identical
# outcome (verified by reading them).
#
# The graph shape has now CONVERGED: TypeScript, Python, Java and Go all keep
# digestBefore/wait/digestAfter at the handler level, so the outer "outernested"
# container holds exactly ONE task (the nested "inner" dag) in every SDK. With
# one shared shape the OUTER container envelope is language-neutral and is PINNED
# (like 10-15 / 10-16): totalCount == successCount == 1, ALL_COMPLETED, and —
# because the outer embeds the inner's ~307KB result in full and so offloads —
# `tasks` DROPPED (its absence is the offload signal) plus the convergence-
# deleted fields absent. Only the single outer ContextSucceeded event is pinned;
# its EventId (17) is DERIVED from the flat name-based model, not measured.
#
# It still pins NO ExpectedEventCount, because no cloud run exists yet so any
# count would be a guess (it must be MEASURED first, exactly as 10-15's 26 was).
# These tests confirm the scenario is discoverable, that it pins the shared
# outcome and the converged outer envelope, and that it carries NO event count.


def test_dag_suite_discovers_10_17() -> None:
    """The dag suite exposes the new 10-17 requirement by its file stem."""
    files = discover_test_files(_REQUIREMENTS_DIR, suite="dag")

    assert "10-17" in files
    assert files["10-17"].endswith("dag/10-17.yaml")


def test_10_17_pins_nested_offload_outcome_and_outer_envelope() -> None:
    """10-17 pins the shared digest outcome and the converged offloaded OUTER envelope."""
    files = discover_test_files(_REQUIREMENTS_DIR, suite="dag")
    data = load_yaml_file(files["10-17"])

    # Async, and now carrying a pinned history for the outer container event.
    assert data["AsyncInvoke"] is True

    # The event count is now pinned, and it was MEASURED rather than derived:
    # the first cloud run of this scenario captured exactly 24 events in all
    # four SDKs (js, python, java, go — independent runs, identical count).
    assert data["ExpectedEventCount"] == 24

    # Exactly ONE event is pinned: the converged OUTER container envelope
    # (ContextSucceeded Dag for "outernested"). No inner-container event, no
    # per-event topology.
    history = data["ExpectedExecutionHistory"]
    ctx_succeeded = [
        e
        for e in history
        if e.get("EventType") == "ContextSucceeded" and e.get("SubType") == "Dag"
    ]
    assert len(ctx_succeeded) == 1
    event = ctx_succeeded[0]
    assert event["EventId"] == 17
    assert event["Name"] == "outernested"

    # The payload is JSON-decoded via ${JSON}, then the converged aggregate
    # envelope is pinned. The outer holds exactly ONE task (the nested "inner"
    # dag) in all four SDKs, so totalCount == successCount == 1. It offloads
    # (embeds the inner ~307KB result), so `tasks` is DROPPED — its absence is
    # the offload signal — and the convergence-deleted fields are absent.
    envelope = event["ContextSucceededDetails"]["Result"]["Payload"]["${JSON}"]
    assert envelope["type"] == "DagResult"
    assert envelope["totalCount"] == 1
    assert envelope["successCount"] == 1
    assert envelope["failureCount"] == 0
    assert envelope["skippedCount"] == 0
    assert envelope["completionReason"] == "ALL_COMPLETED"
    assert envelope["startedTaskNames"] == []
    assert envelope["failedTaskNames"] == []
    assert envelope["tasks"] == "${ABSENT}"
    assert envelope["completedCount"] == "${ABSENT}"
    assert envelope["terminalTaskNames"] == "${ABSENT}"
    assert envelope["summary"] == "${ABSENT}"

    # The decisive, language-neutral outcome — identical across all four SDKs.
    assert data["ExpectedResult"]["ExecutionStatus"] == "SUCCEEDED"
    result = data["ExpectedResult"]["Result"]
    assert result["reason"] == "ALL_COMPLETED"
    # innerReason would read ALL_COMPLETED even under the bug (from a fabricated
    # result), which is why the digest — not the reason — is the decisive check.
    assert result["innerReason"] == "ALL_COMPLETED"
    # innerCounts is [total, failed, skipped, succeeded]; the inner aggregate is
    # the canonical part restored from the offloaded inner envelope.
    assert result["innerCounts"] == [6, 0, 0, 6]
    # Digest equality is the proof the inner per-task detail survived the offload
    # of BOTH containers: it is recomputed from the replayed inner DagResult and
    # must equal the pre-suspend value byte-for-byte.
    assert result["digestBefore"] == "6:307200:abcdef"
    assert result["digestAfter"] == "6:307200:abcdef"
    assert result["match"] is True

    # The result is EXACTLY the six shared fields — the runner compares the
    # execution result by strict equality, so no extra key may leak in.
    assert set(result) == {
        "reason",
        "innerReason",
        "innerCounts",
        "digestBefore",
        "digestAfter",
        "match",
    }
