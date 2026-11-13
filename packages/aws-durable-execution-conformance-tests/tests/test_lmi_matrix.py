# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""The bundled catalog contains only contracts shared by supported runtimes."""

from dataclasses import replace
from importlib.resources import files

import pytest

from aws_durable_execution_conformance_tests.lmi.catalog import COVERAGE_PROFILES, RUNTIMES, requirements
from aws_durable_execution_conformance_tests.lmi.matrix import (
    describe_case,
    duplicate_coverage,
    missing_cases,
    required_cases,
)


@pytest.mark.parametrize("runtime", RUNTIMES)
def test_standard_catalog_is_identical_for_all_supported_runtimes(runtime):
    catalog = requirements("lifecycle", runtime=runtime)
    assert set(catalog) == {f"lmi-lifecycle-{index}" for index in range(1, 7)}
    cases = required_cases(runtime, "lifecycle")
    assert len(cases) == 7
    assert all(case.native_concurrency is None for case in cases)
    assert all(case.roles for case in cases)


def test_every_packaged_requirement_is_shared_and_retains_its_stable_id():
    assert COVERAGE_PROFILES == ("standard",)
    catalog = requirements("lifecycle")
    expected = {f"lmi-lifecycle-{index}" for index in range(1, 7)}
    assert set(catalog) == expected
    root = files("aws_durable_execution_conformance_tests.lmi").joinpath("requirements/lmi-lifecycle")
    assert {
        resource.name.removesuffix(".yaml") for resource in root.iterdir() if resource.name.endswith(".yaml")
    } == expected
    for definition in catalog.values():
        assert set(definition["runtimes"]) == set(RUNTIMES)
        assert "pythonLifecycle" not in definition
        assert definition["mandatory"] is True


@pytest.mark.parametrize("profile", COVERAGE_PROFILES)
def test_language_does_not_select_a_different_shared_matrix(profile):
    matrices = [required_cases(runtime, "lifecycle", profile) for runtime in RUNTIMES]
    assert matrices[0] == matrices[1] == matrices[2]


def test_standard_profile_keeps_minimum_native_concurrency_semantics():
    overlap = next(case for case in required_cases("javascript", "lifecycle") if case.scenario == "native-overlap")
    assert overlap.matches((overlap.requirement_id, overlap.scenario, 2))
    assert overlap.matches((overlap.requirement_id, overlap.scenario, 8))
    assert not overlap.matches((overlap.requirement_id, overlap.scenario, 1))
    assert not overlap.matches((overlap.requirement_id, overlap.scenario, True))
    assert describe_case(overlap) == "lmi-lifecycle-1/native-overlap/native-any-applicable"


def test_a_repeated_success_case_cannot_cover_the_normal_failure_case():
    cases = required_cases("java", "lifecycle")
    failed = next(case for case in cases if case.scenario == "normal-failure")
    observed = [(case.requirement_id, case.scenario, 2) for case in cases if case != failed]
    succeeded = next(key for key in observed if key[1] == "normal-return")
    assert missing_cases(cases, [*observed, succeeded]) == [failed]
    assert duplicate_coverage([*observed, succeeded]) == {succeeded}


def test_caller_owned_exact_native_configurations_remain_distinct():
    standard = required_cases("python", "lifecycle")[0]
    c1 = replace(standard, native_concurrency=1, minimum_native_concurrency=1)
    c2 = replace(standard, native_concurrency=2)
    assert missing_cases([c1, c2], [c2.key, c2.key]) == [c1]
    assert describe_case(c1).endswith("native-1")


@pytest.mark.parametrize("runtime", RUNTIMES)
def test_python_only_profile_requires_an_sdk_owned_definition(runtime):
    with pytest.raises(ValueError, match="Unknown coverage profile"):
        required_cases(runtime, "lifecycle", "python-lifecycle")


@pytest.mark.parametrize("option,value", [("capability", "unknown"), ("coverage_profile", "typo"), ("runtime", "rust")])
def test_unknown_applicability_options_fail_closed(option, value):
    options = {"capability": "lifecycle", "runtime": "javascript", "coverage_profile": "standard"}
    options[option] = value
    with pytest.raises(ValueError):
        requirements(**options)
