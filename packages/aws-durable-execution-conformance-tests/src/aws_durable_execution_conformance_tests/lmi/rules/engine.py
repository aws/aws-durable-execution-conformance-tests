# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Evaluate one lifecycle case's frames with the scenario rules."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Literal

from aws_durable_execution_conformance_tests.lmi.rules import common as common_rules
from aws_durable_execution_conformance_tests.lmi.rules import deadline as deadline_rules
from aws_durable_execution_conformance_tests.lmi.rules import disposal as disposal_rules
from aws_durable_execution_conformance_tests.lmi.rules import overlap as overlap_rules
from aws_durable_execution_conformance_tests.lmi.rules import recovery as recovery_rules
from aws_durable_execution_conformance_tests.lmi.rules import replay as replay_rules
from aws_durable_execution_conformance_tests.lmi.rules import retirement as retirement_rules
from aws_durable_execution_conformance_tests.lmi.rules import warm_reuse as warm_reuse_rules
from aws_durable_execution_conformance_tests.lmi.rules.frames import CaseFrames
from aws_durable_execution_conformance_tests.lmi.rules.values import REQUIREMENT_IDS
from aws_durable_execution_conformance_tests.lmi.rules.view import CaseView


def validate_evidence(
    evidence: Mapping[str, Any],
    *,
    phase: Literal["observations", "final"] = "final",
    retired_requests: Mapping[str, set[str]] | None = None,
) -> list[str]:
    """Validate all observations, then optionally case-owned retirement.

    The coordinator calls ``observations`` before releasing faults and retains
    those errors even if emergency release later succeeds. ``final`` additionally
    requires complete retirement evidence.
    """
    if not isinstance(evidence, Mapping):
        return ["INCOMPLETE: evidence must be an object"]
    if phase not in ("observations", "final"):
        return ["INCOMPLETE: unknown evidence validation phase"]
    validator = CaseView.from_frames(CaseFrames.single(evidence))
    try:
        common_rules.common(validator)
    except (KeyError, TypeError, ValueError, OverflowError) as error:
        validator.error(f"malformed evidence: {error}")
    # Do not interpret malformed structures as behavior observations. Retirement
    # is still checked independently: pre-existing observation errors cannot
    # suppress quiescence checks before the scheduler grants another lease.
    if not validator.errors:
        try:
            _validate_observations(validator, evidence)
        except (KeyError, TypeError, ValueError, OverflowError) as error:
            validator.error(f"malformed evidence: {error}")
    if phase == "final":
        try:
            retirement_rules.retirement(validator, retired_requests=retired_requests)
        except (KeyError, TypeError, ValueError, OverflowError) as error:
            validator.error(f"malformed retirement evidence: {error}")
    return validator.errors


def validate_common_evidence(
    evidence: Mapping[str, Any],
    *,
    phase: Literal["observations", "final"] = "final",
    requirement_ids: tuple[str, ...] = REQUIREMENT_IDS,
) -> list[str]:
    """Validate shared ownership and retirement for an explicit scenario catalog.

    Callers must also invoke their catalog's behavior validator. This helper does
    not validate a scenario merely because its ownership envelope is complete.
    """
    if not isinstance(evidence, Mapping):
        return ["INCOMPLETE: evidence must be an object"]
    if phase not in ("observations", "final"):
        return ["INCOMPLETE: unknown evidence validation phase"]
    validator = CaseView.from_frames(CaseFrames.single(evidence), requirement_ids, standard_behavior=False)
    try:
        common_rules.common(validator)
    except (KeyError, TypeError, ValueError, OverflowError) as error:
        validator.error(f"malformed evidence: {error}")
    if phase == "final":
        try:
            retirement_rules.retirement(validator)
        except (KeyError, TypeError, ValueError, OverflowError) as error:
            validator.error(f"malformed retirement evidence: {error}")
    return validator.errors


def _validate_observations(validator: CaseView, evidence: Mapping[str, Any]) -> None:
    requirement = evidence.get("requirementId")
    checks = {
        "lmi-lifecycle-1": lambda: overlap_rules.overlap(validator),
        "lmi-lifecycle-2": lambda: disposal_rules.disposal(validator, "SUCCEEDED"),
        "lmi-lifecycle-3": lambda: deadline_rules.deadline(validator),
        "lmi-lifecycle-4": lambda: recovery_rules.recovery(validator),
        "lmi-lifecycle-5": lambda: replay_rules.replay(validator),
        "lmi-lifecycle-6": lambda: warm_reuse_rules.warm_reuse(validator),
        "lmi-lifecycle-7": lambda: disposal_rules.disposal(validator, "FAILED"),
    }
    if requirement in checks:
        checks[requirement]()
