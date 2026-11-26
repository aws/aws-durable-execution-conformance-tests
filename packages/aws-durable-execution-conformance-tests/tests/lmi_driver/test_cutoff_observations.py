# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""An undispatched rejection settles retirement but never a deferred cutoff obligation."""

from aws_durable_execution_conformance_tests.lmi.evidence import (
    validate_evidence,
    validate_retirement_evidence,
)


def test_deferred_undispatched_rejection_never_satisfies_the_deferred_obligation(tmp_path):
    from aws_durable_execution_conformance_tests.lmi.rules.cutoff import deferred_retirement_errors

    from .support import build_evidence as normalize
    from .test_source_seals import cloud_source, rejected_completion

    ctx, cloud = cloud_source(tmp_path)
    ctx.update_attempt(ctx.attempts[0]["attemptId"], executionArn="execution")
    cloud.invocations[0].update(role="victim", resume_roles=())
    cloud.raw_histories["execution"].append(rejected_completion("rejected", 4, 1.1, 1.3))
    cloud.source_uris.add((tmp_path / "histories/marker.json").resolve().as_uri())

    frozen = normalize(ctx, cloud)
    assert frozen["collection"]["deferredServiceRequests"] == [{"requestId": "rejected", "executionArn": "execution"}]

    final = normalize(ctx, cloud, phase="final")
    settlement = next(item for item in final["retirement"]["resources"] if item["id"] == "rejected")
    assert settlement["outcome"] == "UNDISPATCHED"
    assert settlement["startedAtMs"] == 1100

    # Physical retirement, the audit collection runs, settles it.
    assert validate_retirement_evidence(final) == []

    # Requirement 1 still holds: behavior never accepts the settlement for the
    # deferred obligation, and the final document reports it incomplete.
    assert deferred_retirement_errors(frozen, final) == ["Retirement omitted deferred service request rejected"]
    assert any(
        "service rejected native request rejected before its handler ran" in error for error in validate_evidence(final)
    )
