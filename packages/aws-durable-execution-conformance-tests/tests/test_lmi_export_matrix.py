# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Every runtime must exercise both export behaviors in independent matrix slots."""

from dataclasses import replace

import pytest

from aws_durable_execution_conformance_tests.lmi.catalog import EXPORT_MODES, RUNTIMES, requirements
from aws_durable_execution_conformance_tests.lmi.matrix import (
    describe_case,
    duplicate_coverage,
    missing_cases,
    required_cases,
)

SCENARIOS = ("export-success", "export-failure-recovery", "held-export")


@pytest.mark.parametrize("runtime", RUNTIMES)
def test_every_runtime_requires_all_twenty_four_export_cells(runtime):
    cells = required_cases(runtime, "lifecycle-otel", "export-lifecycle")
    assert len(cells) == 24
    assert {cell.key for cell in cells} == {
        ("lmi-lifecycle-16", scenario, native, view, mode)
        for scenario in SCENARIOS
        for native in (1, 2)
        for view in ("invocation", "execution")
        for mode in EXPORT_MODES
    }
    assert missing_cases(cells, [cell.key for cell in cells]) == []
    assert duplicate_coverage(cell.key for cell in cells) == set()


def test_modes_are_behavior_requirements_not_language_preferences():
    matrices = [required_cases(runtime, "lifecycle-otel", "export-lifecycle") for runtime in RUNTIMES]
    assert matrices[0] == matrices[1] == matrices[2]
    assert EXPORT_MODES == ("sync", "async-bounded-flush")


@pytest.mark.parametrize("slot", range(24))
def test_each_missing_mode_view_configuration_or_scenario_blocks_coverage(slot):
    cells = required_cases("python", "lifecycle-otel", "export-lifecycle")
    omitted = cells[slot]
    observed = [cell.key for cell in cells if cell != omitted]
    assert missing_cases(cells, observed) == [omitted]


def test_duplicate_synchronous_cases_cannot_substitute_for_async_flush_cases():
    cells = required_cases("javascript", "lifecycle-otel", "export-lifecycle")
    sync = [cell.key for cell in cells if cell.export_mode == "sync"]
    absent = missing_cases(cells, sync + sync)
    assert len(absent) == 12
    assert {cell.export_mode for cell in absent} == {"async-bounded-flush"}
    assert duplicate_coverage(sync + sync) == set(sync)


def test_mode_identity_is_independent_of_otel_view_and_native_configuration():
    cells = required_cases("java", "lifecycle-otel", "export-lifecycle")
    chosen = next(cell for cell in cells if cell.export_mode == "async-bounded-flush")
    for wrong in (
        replace(chosen, export_mode="sync"),
        replace(chosen, export_mode=None),
        replace(chosen, export_mode="async"),
        replace(chosen, otel_view="execution"),
        replace(chosen, native_concurrency=2),
        replace(chosen, native_concurrency=8),
        replace(chosen, scenario="held-export"),
    ):
        assert not chosen.matches(wrong.key)


def test_export_slots_use_agreed_immutable_behavior_budgets_and_case_scoped_roles():
    cells = required_cases("python", "lifecycle-otel", "export-lifecycle")
    for cell in cells:
        expected_budgets = {"observationGraceMs", "exportCompletionMs"}
        if cell.export_mode == "async-bounded-flush":
            expected_budgets.add("flushWaitMs")
        assert set(cell.budget_names) == expected_budgets
        assert cell.roles == (("victim", "warm") if cell.scenario == "export-failure-recovery" else ("victim",))
        assert cell.minimum_native_concurrency == 1
        assert cell.exclusive == (cell.scenario != "export-success")
        assert describe_case(cell).endswith(f"/export={cell.export_mode}")


@pytest.mark.parametrize("runtime", RUNTIMES)
def test_export_profile_is_opt_in_and_cannot_disable_otel(runtime):
    assert set(requirements("lifecycle-otel", runtime=runtime, coverage_profile="export-lifecycle")) == {
        "lmi-lifecycle-16"
    }
    assert "lmi-lifecycle-16" not in requirements("lifecycle-otel", runtime=runtime)
    with pytest.raises(ValueError, match="requires lifecycle-otel"):
        required_cases(runtime, "lifecycle", "export-lifecycle")


def test_existing_profiles_keep_their_coverage_counts_and_no_export_mode_obligation():
    assert len(requirements("lifecycle")) == 6
    assert len(requirements("lifecycle-otel")) == 8
    for capability, count in (("lifecycle", 28), ("lifecycle-otel", 76)):
        cells = required_cases("python", capability, "python-lifecycle")
        assert len(cells) == count
        assert all(cell.export_mode is None and cell.key[-1] is None for cell in cells)
        assert missing_cases(cells, [cell.key for cell in cells]) == []
    standard = required_cases("java", "lifecycle")
    assert all(cell.export_mode is None for cell in standard)
