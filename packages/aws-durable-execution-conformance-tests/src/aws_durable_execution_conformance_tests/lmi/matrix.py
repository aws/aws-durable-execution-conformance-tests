# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Required scenario/configuration slots for explicit LMI coverage profiles."""

from collections import Counter
from collections.abc import Iterable, Sequence
from dataclasses import dataclass

from aws_durable_execution_conformance_tests.lmi.catalog import requirements

CaseKey = tuple[str, str, int | None]


@dataclass(frozen=True)
class CaseRequirement:
    """One mandatory slot; native None preserves the original profile policy."""

    requirement_id: str
    scenario: str
    native_concurrency: int | None
    roles: tuple[str, ...]
    exclusive: bool
    minimum_native_concurrency: int
    source_tests: tuple[str, ...] = ()
    budget_names: tuple[str, ...] = ()

    @property
    def key(self) -> CaseKey:
        return self.requirement_id, self.scenario, self.native_concurrency

    def matches(self, observed: CaseKey) -> bool:
        """Match the scenario and its required native configuration."""
        requirement_id, scenario, concurrency = observed
        return (
            requirement_id == self.requirement_id
            and scenario == self.scenario
            and type(concurrency) is int
            and (
                concurrency == self.native_concurrency
                if self.native_concurrency is not None
                else concurrency >= self.minimum_native_concurrency
            )
        )


def required_cases(runtime: str, capability: str, coverage_profile: str = "standard") -> list[CaseRequirement]:
    """Expand requirement metadata into mandatory scenario/configuration slots.

    Standard contracts use the same minimum-capacity cases for every runtime.
    """
    catalog = requirements(capability, runtime=runtime, coverage_profile=coverage_profile)
    result: list[CaseRequirement] = []
    for requirement_id, requirement in catalog.items():
        result.append(
            CaseRequirement(
                requirement_id,
                requirement["scenario"],
                None,
                tuple(requirement["roles"]),
                requirement["scheduling"] == "exclusive",
                requirement["minimumNativeConcurrency"],
                budget_names=tuple(requirement.get("behaviorBudgets", ())),
            )
        )
    keys = [case.key for case in result]
    if len(keys) != len(set(keys)):
        raise ValueError("Requirement metadata contains duplicate coverage slots")
    return result


def missing_cases(required: Sequence[CaseRequirement], observed_keys: Iterable[CaseKey]) -> list[CaseRequirement]:
    """Return uncovered mandatory slots; duplicate successes cannot fill gaps."""
    observed = tuple(observed_keys)
    return [case for case in required if not any(case.matches(key) for key in observed)]


def duplicate_coverage(observed_keys: Iterable[CaseKey]) -> set[CaseKey]:
    """Identify repeated slots without conflating repeats with broader coverage."""
    return {key for key, count in Counter(observed_keys).items() if count > 1}


def describe_case(case: CaseRequirement) -> str:
    concurrency = str(case.native_concurrency) if case.native_concurrency is not None else "any-applicable"
    return f"{case.requirement_id}/{case.scenario}/native-{concurrency}"
