# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Pre-created AWS clients shared by concurrent validation workers."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any

import boto3
from botocore.config import Config

CORE_AWS_SERVICES = ("lambda", "cloudformation", "logs")
AWS_CLIENT_CONFIG = Config(retries={"mode": "adaptive", "max_attempts": 10})


@dataclass(frozen=True)
class AwsClients(Mapping[str, Any]):
    """Immutable service-name mapping of low-level boto3 clients."""

    _clients: Mapping[str, Any]

    @classmethod
    def create(
        cls,
        region: str,
        additional_services: Iterable[str] = (),
    ) -> AwsClients:
        """Create all requested clients serially for later sharing by workers."""
        session = boto3.Session(region_name=region)
        additional = sorted(set(additional_services).difference(CORE_AWS_SERVICES))
        service_names = (*CORE_AWS_SERVICES, *additional)
        return cls(
            MappingProxyType(
                {service_name: session.client(service_name, config=AWS_CLIENT_CONFIG) for service_name in service_names}
            )
        )

    def __getitem__(self, service_name: str) -> Any:
        return self._clients[service_name]

    def __iter__(self) -> Iterator[str]:
        return iter(self._clients)

    def __len__(self) -> int:
        return len(self._clients)
