# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Shared deployment and source collection failures."""


class ProvisioningError(RuntimeError):
    """Unsupported configuration, unavailable capacity, or failed deployment."""


class CollectionError(RuntimeError):
    """Evidence is missing/unavailable; distinct from an SDK regression assertion."""


class TransientCollectionError(CollectionError):
    """Retryable source failure while collecting a bounded snapshot."""


class CollectionBudgetExpired(TimeoutError):
    """The optional observation worker ran out of the collection phase budget."""
