# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Required scenario/configuration slots for explicit LMI coverage profiles."""

from collections import Counter
from collections.abc import Iterable, Sequence
from dataclasses import dataclass

from aws_durable_execution_conformance_tests.lmi.catalog import EXPORT_MODES, requirements

CaseKey = tuple[str, str, int | None, str | None, str | None]


@dataclass(frozen=True)
class CaseRequirement:
    """One mandatory slot; native None preserves the original profile policy."""

    requirement_id: str
    scenario: str
    native_concurrency: int | None
    otel_view: str | None
    roles: tuple[str, ...]
    exclusive: bool
    minimum_native_concurrency: int
    source_tests: tuple[str, ...] = ()
    budget_names: tuple[str, ...] = ()
    export_mode: str | None = None

    @property
    def key(self) -> CaseKey:
        return self.requirement_id, self.scenario, self.native_concurrency, self.otel_view, self.export_mode

    def matches(self, observed: CaseKey) -> bool:
        """Native configuration, OTel view and exporter mode are independent slots."""
        requirement_id, scenario, concurrency, view, mode = observed
        return (
            requirement_id == self.requirement_id
            and scenario == self.scenario
            and view == self.otel_view
            and mode == self.export_mode
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
    Export lifecycle coverage requires both exporter modes across three scenarios,
    two views and two native configurations, independently of runtime language.
    """
    catalog = requirements(capability, runtime=runtime, coverage_profile=coverage_profile)
    result: list[CaseRequirement] = []
    for requirement_id, requirement in catalog.items():
        if coverage_profile == "standard":
            result.extend(
                CaseRequirement(
                    requirement_id,
                    scenario,
                    None,
                    None,
                    tuple(requirement["roles"]),
                    requirement["scheduling"] == "exclusive",
                    requirement["minimumNativeConcurrency"],
                )
                for scenario in requirement["requiredScenarios"]
            )
            continue
        if coverage_profile == "export-lifecycle":
            export = requirement["exportLifecycle"]
            for definition in export["scenarios"]:
                for concurrency in (1, 2):
                    for export_view in ("invocation", "execution"):
                        for mode in EXPORT_MODES:
                            result.append(
                                CaseRequirement(
                                    requirement_id=requirement_id,
                                    scenario=definition["scenario"],
                                    native_concurrency=concurrency,
                                    otel_view=export_view,
                                    roles=tuple(definition["roles"]),
                                    exclusive=definition["exclusive"],
                                    minimum_native_concurrency=definition["minimumNativeConcurrency"],
                                    source_tests=tuple(definition.get("sourceTests", ())),
                                    budget_names=tuple(definition["budgetNames"])
                                    + tuple(export.get("modeBudgetNames", {}).get(mode, ())),
                                    export_mode=mode,
                                )
                            )
            continue
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
    description = f"{case.requirement_id}/{case.scenario}/native-{concurrency}/{case.otel_view or 'no-otel'}"
    return f"{description}/export={case.export_mode}" if case.export_mode is not None else description
