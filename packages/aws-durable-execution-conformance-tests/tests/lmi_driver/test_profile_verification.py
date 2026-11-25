# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Preparation verifies each selected target independently."""

from unittest.mock import Mock, call

import pytest

from aws_durable_execution_conformance_tests.lmi.cloud import Cloud
from aws_durable_execution_conformance_tests.lmi.runtime import CONCURRENCIES, QUALIFIER

from .support import RUNTIME


@pytest.fixture
def verification(tmp_path):
    deployment = {
        **RUNTIME.to_manifest(),
        "region": "us-west-2",
        "run": "run",
        "bucket": "evidence",
        "commit": "sdk",
        "runtime": "python3.14",
        "role": "role",
        "provider": "provider",
        "codeSha256": "code",
        "invocationTimeout": 60,
        "executionTimeout": 240,
        "functions": {
            key: f"arn:aws:lambda:us-west-2:123456789012:function:lmi-{key}:{QUALIFIER}" for key in CONCURRENCIES
        },
        "concurrencies": dict(CONCURRENCIES),
    }
    lam = Mock()
    verify_profile = Mock()

    def factory(manifest, configuration=None, **options):
        return Cloud(
            manifest,
            configuration,
            **options,
            verify_profile=verify_profile,
            client_factory=lambda *_: lam,
        )

    return deployment, lam, verify_profile, factory, tmp_path


@pytest.mark.parametrize("configuration", CONCURRENCIES)
def test_selected_profile_reads_only_its_function(verification, configuration):
    deployment, lam, verify_profile, factory, tmp_path = verification
    cloud = factory(deployment, configuration, artifacts_dir=tmp_path)
    cloud.verify()
    arn = deployment["functions"][configuration]
    lam.get_function_configuration.assert_called_once_with(FunctionName=arn)
    lam.get_function_scaling_config.assert_called_once_with(FunctionName=arn.rsplit(":", 1)[0], Qualifier=QUALIFIER)
    verify_profile.assert_called_once_with(
        lam.get_function_configuration.return_value,
        lam.get_function_scaling_config.return_value,
        deployment,
        configuration,
    )


def test_unscoped_verification_checks_both_profiles(verification):
    deployment, lam, verify_profile, factory, tmp_path = verification
    factory(deployment, artifacts_dir=tmp_path).verify()
    assert lam.get_function_configuration.call_args_list == [
        call(FunctionName=arn) for arn in deployment["functions"].values()
    ]
    assert [args.args[-1] for args in verify_profile.call_args_list] == list(CONCURRENCIES)
