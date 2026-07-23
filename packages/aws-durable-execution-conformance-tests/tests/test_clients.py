# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Tests for pre-created AWS validation clients."""

from __future__ import annotations

from typing import Any

import aws_durable_execution_conformance_tests.clients as clients_module
import pytest
from aws_durable_execution_conformance_tests.clients import AwsClients


def test_creates_each_client_serially_from_one_explicit_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created_services: list[str] = []
    session_regions: list[str] = []

    class _Session:
        def client(self, service_name: str) -> Any:
            created_services.append(service_name)
            return object()

    def _session(*, region_name: str) -> _Session:
        session_regions.append(region_name)
        return _Session()

    monkeypatch.setattr(clients_module.boto3, "Session", _session)

    clients = AwsClients.create(
        "us-west-2",
        additional_services=("xray", "s3", "lambda", "s3"),
    )

    assert session_regions == ["us-west-2"]
    assert created_services == ["lambda", "cloudformation", "logs", "s3", "xray"]
    assert list(clients) == created_services
