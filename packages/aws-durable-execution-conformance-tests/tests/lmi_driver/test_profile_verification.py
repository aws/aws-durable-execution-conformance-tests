# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Preparation verifies each selected target independently."""

import asyncio
from unittest.mock import Mock, call

import pytest
from botocore.exceptions import ClientError

from aws_durable_execution_conformance_tests.lmi.adapter import SharedAdapter
from aws_durable_execution_conformance_tests.lmi.cloud import Cloud
from aws_durable_execution_conformance_tests.lmi.coordinator import Profile
from aws_durable_execution_conformance_tests.lmi.errors import ProvisioningError
from aws_durable_execution_conformance_tests.lmi.runtime import CONCURRENCIES, QUALIFIER


@pytest.fixture
def verification(tmp_path):
    deployment = {
        "region": "us-west-2",
        "run": "run",
        "bucket": "evidence",
        "commit": "sdk",
        "functions": {
            key: f"arn:aws:lambda:us-west-2:123456789012:function:lmi-{key}:{QUALIFIER}" for key in CONCURRENCIES
        },
        "concurrencies": dict(CONCURRENCIES),
    }
    lam = Mock()
    lam.put_object.return_value = {"ResponseMetadata": {"RetryAttempts": 0}}
    lam.get_object.side_effect = ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
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


@pytest.mark.parametrize("failure", ["service", "configuration"])
def test_failed_profile_does_not_fail_healthy_adapter_preparation(verification, failure):
    deployment, lam, verify_profile, factory, tmp_path = verification
    broken = deployment["functions"]["c1"]

    def get_configuration(*, FunctionName):
        if FunctionName == broken and failure == "service":
            raise ProvisioningError("c1 service unavailable")
        return {"FunctionArn": FunctionName}

    def validate(config, scaling, manifest, key):
        if key == "c1" and failure == "configuration":
            raise ProvisioningError("c1 configuration mismatch")

    lam.get_function_configuration.side_effect = get_configuration
    verify_profile.side_effect = validate
    adapter = SharedAdapter(
        {"sdk": {"deployment": deployment, "artifacts_dir": str(tmp_path)}},
        evidence_builder=Mock(),
        cloud_factory=factory,
        driver_factory=Mock(),
        configuration_identity=lambda manifest, key: key,
    )

    async def prepare():
        for key, native in CONCURRENCIES.items():
            profile = Profile(key, deployment["functions"][key], native, native, "sdk", key)
            if key == "c1":
                with pytest.raises(ProvisioningError, match="c1"):
                    await adapter.prepare(profile)
            else:
                await adapter.prepare(profile)

    asyncio.run(prepare())
    assert lam.get_function_configuration.call_args_list == [
        call(FunctionName=arn) for arn in deployment["functions"].values()
    ]
