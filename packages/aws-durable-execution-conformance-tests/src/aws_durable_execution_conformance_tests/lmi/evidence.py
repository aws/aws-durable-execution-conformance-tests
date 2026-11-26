# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Version 1 lifecycle evidence contract and fail-closed observation validators.

See ``evidence-schema.md`` next to this module. Times are milliseconds on one
coordinator-normalized observation clock, not uncorrelated process wall clocks.
The schema is an adapter contract, not proof that an untrusted producer is honest.
"""

from __future__ import annotations

from aws_durable_execution_conformance_tests.lmi.rules import common as _common
from aws_durable_execution_conformance_tests.lmi.rules import deadline as _deadline
from aws_durable_execution_conformance_tests.lmi.rules import disposal as _disposal
from aws_durable_execution_conformance_tests.lmi.rules import overlap as _overlap
from aws_durable_execution_conformance_tests.lmi.rules import recovery as _recovery
from aws_durable_execution_conformance_tests.lmi.rules import replay as _replay
from aws_durable_execution_conformance_tests.lmi.rules import retirement as _retirement
from aws_durable_execution_conformance_tests.lmi.rules import warm_reuse as _warm_reuse
from aws_durable_execution_conformance_tests.lmi.rules.common import (
    _has_matching_sdk_request as _has_matching_sdk_request,
)
from aws_durable_execution_conformance_tests.lmi.rules.common import normalize_retirement_events
from aws_durable_execution_conformance_tests.lmi.rules.engine import _validate_observations as _validate_observations
from aws_durable_execution_conformance_tests.lmi.rules.engine import validate_common_evidence, validate_evidence
from aws_durable_execution_conformance_tests.lmi.rules.retirement import (
    validate_observed_retirement,
    validate_retirement_evidence,
)
from aws_durable_execution_conformance_tests.lmi.rules.values import _CALL_METHODS as _CALL_METHODS
from aws_durable_execution_conformance_tests.lmi.rules.values import _WORK_INTERVALS as _WORK_INTERVALS
from aws_durable_execution_conformance_tests.lmi.rules.values import (
    COMPLETED_OPERATION_STATUSES,
    HISTORY_STATUSES,
    RECOVERY_POLICIES,
    REQUIREMENT_IDS,
    RUNTIMES,
    SCHEMA_VERSION,
    TERMINAL_STATUSES,
    EvidenceValidator,
    is_json_integer,
    same_json,
)
from aws_durable_execution_conformance_tests.lmi.rules.view import CaseView

__all__ = [
    "COMPLETED_OPERATION_STATUSES",
    "HISTORY_STATUSES",
    "RECOVERY_POLICIES",
    "REQUIREMENT_IDS",
    "RUNTIMES",
    "SCHEMA_VERSION",
    "TERMINAL_STATUSES",
    "EvidenceValidator",
    "is_json_integer",
    "normalize_retirement_events",
    "same_json",
    "validate_common_evidence",
    "validate_evidence",
    "validate_observed_retirement",
    "validate_retirement_evidence",
]


class _Evidence(CaseView):
    """Compatibility facade over the rule modules in ``lmi.rules``.

    Each scenario rule is a module function taking the case view. The methods
    below keep ordinary ``_Evidence(...).rule()`` calls working. Rules call one
    another directly, so overriding one method in a subclass does not change the
    others.
    """

    optional_shapes = _common.optional_shapes
    common = _common.common
    owned_record = _common.owned_record
    histories = _common.histories
    no_late_dispatch = _common.no_late_dispatch
    overlap = _overlap.overlap
    overlap_activity = _overlap.overlap_activity
    disposal = _disposal.disposal
    require_probes = _disposal.require_probes
    observation_grace = _deadline.observation_grace
    deadline = _deadline.deadline
    recovery = _recovery.recovery
    replay = _replay.replay
    warm_reuse = _warm_reuse.warm_reuse
    last_activity = _retirement.last_activity
    observed_retirement_work = _retirement.observed_retirement_work
    retirement = _retirement.retirement
