# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Evaluate one lifecycle case's frames with the scenario rules."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Literal

from aws_durable_execution_conformance_tests.lmi.rules import common as common_rules
from aws_durable_execution_conformance_tests.lmi.rules import cutoff as cutoff_rules
from aws_durable_execution_conformance_tests.lmi.rules import deadline as deadline_rules
from aws_durable_execution_conformance_tests.lmi.rules import disposal as disposal_rules
from aws_durable_execution_conformance_tests.lmi.rules import overlap as overlap_rules
from aws_durable_execution_conformance_tests.lmi.rules import recovery as recovery_rules
from aws_durable_execution_conformance_tests.lmi.rules import replay as replay_rules
from aws_durable_execution_conformance_tests.lmi.rules import retirement as retirement_rules
from aws_durable_execution_conformance_tests.lmi.rules import warm_reuse as warm_reuse_rules
from aws_durable_execution_conformance_tests.lmi.rules.frames import CaseFrames
from aws_durable_execution_conformance_tests.lmi.rules.values import REQUIREMENT_IDS, mappings
from aws_durable_execution_conformance_tests.lmi.rules.view import CaseView


def validate_evidence(evidence: Mapping[str, Any], *, phase: Literal["observations", "final"] = "final") -> list[str]:
    """Validate all observations, then optionally case-owned retirement.

    ``observations`` checks the envelope and the scenario's observations.
    ``final`` additionally requires complete retirement evidence. A retry active
    at the observation cutoff or a deferred service request needs independent
    retirement evidence, which only adjudication has (``audit_cutoff``), so this
    single-document check reports it incomplete.
    """
    if not isinstance(evidence, Mapping):
        return ["INCOMPLETE: evidence must be an object"]
    if phase not in ("observations", "final"):
        return ["INCOMPLETE: unknown evidence validation phase"]
    view = establish(evidence)
    if phase == "final":
        retire(view, evidence)
        try:
            active, deferred = cutoff_rules.cutoff_obligations(evidence)
        except (KeyError, TypeError, ValueError, OverflowError) as error:
            view.error(f"malformed retirement evidence: {error}")
        else:
            if active or deferred:
                view.error(_CUTOFF_AUDIT_REQUIRED)
    return view.errors


_CUTOFF_AUDIT_REQUIRED = "active-at-cutoff or deferred request evidence requires an independent retirement audit"


def audit_cutoff(evidence: Mapping[str, Any], retirement_evidence: Mapping[str, Any]) -> list[str]:
    """Prove frozen evidence's observation-cutoff obligations against independent retirement.

    Returns no findings when no retry was active at the cutoff and no service
    request was deferred past it. Raises ``ValueError`` for a malformed deferral
    inventory.
    """
    active, deferred = cutoff_rules.cutoff_obligations(evidence)
    if not active and not deferred:
        return []
    if not isinstance(retirement_evidence, Mapping) or not retirement_evidence:
        return [f"INCOMPLETE: {_CUTOFF_AUDIT_REQUIRED}"]
    errors = retirement_rules.validate_retirement_evidence(retirement_evidence)
    if errors:
        return errors
    frozen = {attempt.get("requestId") for attempt in mappings(evidence.get("attempts"))}
    retired = {attempt.get("requestId") for attempt in mappings(retirement_evidence.get("attempts"))}
    if retired != frozen | deferred.keys():
        errors.append("INCOMPLETE: independent retirement requests must match the frozen and deferred inventories")
    errors.extend(
        f"INCOMPLETE: {finding}"
        for finding in (
            cutoff_rules.active_pre_cutoff_errors(evidence, retirement_evidence)
            + cutoff_rules.active_retirement_errors(evidence, retirement_evidence)
        )
    )
    return errors


def establish(evidence: Mapping[str, Any]) -> CaseView:
    """Check the envelope and scenario observations of frozen evidence; return the view."""
    view = CaseView.from_frames(CaseFrames.single(evidence))
    try:
        common_rules.common(view)
    except (KeyError, TypeError, ValueError, OverflowError) as error:
        view.error(f"malformed evidence: {error}")
    # Do not interpret malformed structures as behavior observations. Retirement
    # is still checked independently: pre-existing observation errors cannot
    # suppress quiescence checks before the scheduler grants another lease.
    if not view.errors:
        try:
            _validate_observations(view, evidence)
        except (KeyError, TypeError, ValueError, OverflowError) as error:
            view.error(f"malformed evidence: {error}")
    return view


def retire(
    view: CaseView,
    final_evidence: Mapping[str, Any],
    *,
    retired_requests: Mapping[str, set[str]] | None = None,
) -> list[str]:
    """Audit case-owned retirement after ``establish``; return the findings it adds.

    ``final_evidence`` is the established evidence, or a copy of it whose
    ``retirement`` record alone was replaced, as adjudication's
    projection does. The established indexes stay valid because no envelope or
    scenario rule reads ``retirement``.
    """
    start = len(view.errors)
    view.value = final_evidence
    try:
        retirement_rules.retirement(view, retired_requests=retired_requests)
    except (KeyError, TypeError, ValueError, OverflowError) as error:
        view.error(f"malformed retirement evidence: {error}")
    return view.errors[start:]


def validate_common_evidence(
    evidence: Mapping[str, Any],
    *,
    phase: Literal["observations", "final"] = "final",
    requirement_ids: tuple[str, ...] = REQUIREMENT_IDS,
) -> list[str]:
    """Validate shared ownership and retirement for an explicit scenario catalog.

    Callers must also invoke their catalog's behavior validator. This helper does
    not validate a scenario merely because its ownership envelope is complete.
    It does not check observation-cutoff obligations (retries active at the
    cutoff, deferred service requests); adjudication proves them with
    ``audit_cutoff`` against the capture's independent retirement evidence.
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
    # Scenario rules must not read ``retirement``: ``retire`` audits a projected
    # retirement record on the view these rules established.
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
