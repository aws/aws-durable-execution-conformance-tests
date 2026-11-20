# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Evidence frames: the snapshots a lifecycle case is judged from.

A case is judged from one or more snapshots of the same run, and each frame
records where it came from. The frozen frame is the observation evidence a
verdict is established from. Cleanup and retirement frames may refute scenario
claims, or add facts that need their own correlation, but never supply
observation proof the frozen frame lacks. The retirement frame is the source of
settlement and quiescence proof. Observation frames are the raw snapshots taken
before observations froze. Today ``validate_evidence`` and
``validate_common_evidence`` use one frozen frame, and
``validate_retirement_evidence`` audits its snapshot directly. The offline
evaluator's cleanup re-checks read the frozen evidence and later snapshots
through ``reconcile.ReconcileView``; each scenario module holds both its
single-snapshot rules and its ``reconcile_*`` re-checks.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any


class Source(StrEnum):
    """Where a frame came from."""

    FROZEN = "frozen"
    OBSERVATION = "observation"
    CLEANUP = "cleanup"
    RETIREMENT = "retirement"


@dataclass(frozen=True)
class Frame:
    """One evidence snapshot. Rules read ``value`` and never mutate it."""

    source: Source
    label: str
    value: Mapping[str, Any]


@dataclass(frozen=True)
class CaseFrames:
    """All frames for one case, frozen frame first."""

    frozen: Frame

    @classmethod
    def single(cls, evidence: Mapping[str, Any]) -> CaseFrames:
        """Frames for one version 1 lifecycle evidence document (see ``evidence-schema.md``)."""
        return cls(Frame(Source.FROZEN, "frozen", evidence))
