# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Durability of ``artifacts.save``: the parent directory must be fsynced too."""

import os
import stat

import pytest

from aws_durable_execution_conformance_tests.lmi import artifacts


@pytest.mark.parametrize("exclusive", [False, True])
def test_save_fsyncs_the_parent_directory_after_the_target_exists(tmp_path, monkeypatch, exclusive):
    """A host crash right after ``save`` returns must not come back with the
    journal record missing or stale: the directory entry has to be durable too,
    and only once the target name (link or replace) is actually in place.
    """
    target = tmp_path / "journal.json"
    real_fsync = os.fsync
    # (is_directory, target_exists_at_call_time) for every os.fsync call save() makes.
    events: list[tuple[bool, bool]] = []

    def fsync(descriptor: int) -> None:
        is_directory = stat.S_ISDIR(os.fstat(descriptor).st_mode)
        events.append((is_directory, target.exists()))
        real_fsync(descriptor)

    monkeypatch.setattr(artifacts.os, "fsync", fsync)

    artifacts.save(target, {"value": 1}, exclusive=exclusive)

    directory_events = [exists for is_directory, exists in events if is_directory]
    assert directory_events, events
    assert all(directory_events), events

    file_events = [exists for is_directory, exists in events if not is_directory]
    assert file_events, events
    assert not any(file_events), events


def test_save_fsyncs_ancestors_it_creates(tmp_path, monkeypatch):
    """Ancestors ``save`` itself has to ``mkdir`` need their own entry synced too;
    an already-existing ancestor does not need to be touched again.
    """
    deep = tmp_path / "a" / "b" / "c"
    synced: list[os.PathLike] = []
    real_sync = artifacts._sync_directory

    def recording_sync(path):
        synced.append(path)
        real_sync(path)

    monkeypatch.setattr(artifacts, "_sync_directory", recording_sync)

    artifacts.save(deep / "journal.json", {"value": 1})

    # tmp_path already existed; a, b and c did not, so each of their parents
    # (b, a, tmp_path -- innermost first, mirroring _make_durable_directory)
    # needs a sync, plus the final sync of c (the file's own directory) itself.
    assert synced == [tmp_path / "a" / "b", tmp_path / "a", tmp_path, deep]
    synced.clear()

    # A second save into the same, now-existing directory creates nothing new.
    artifacts.save(deep / "second.json", {"value": 2})
    assert synced == [deep]
