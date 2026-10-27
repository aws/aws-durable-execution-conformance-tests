# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0

import pytest

from .requirement_assertions import assert_span_error, span_assertion_path


@pytest.mark.parametrize("reverse", [False, True])
def test_error_identity_uses_nested_selector_fields_not_yaml_order(reverse: bool) -> None:
    initial = {"select": {"name": "Invocation", "attributes": {"first": True, "arn": "child"}}}
    resumed = {"select": {"name": "Invocation", "attributes": {"first": False, "arn": "child"}}}
    assertions = {"span_assertions": [resumed, initial] if reverse else [initial, resumed]}
    selector = {"name": "Invocation", "attributes": {"first": False}}
    index = 0 if reverse else 1
    assert span_assertion_path(assertions, selector) == f"span_assertions[{index}]"
    assert_span_error(
        [f"span_assertions[{index}].select matched no spans"], assertions, selector, "select matched no spans"
    )


@pytest.mark.parametrize(
    "errors",
    [
        [],
        ["span_assertions[1].select matched no spans"],
        ["span_assertions[0].expect.before matched no spans"],
        ["span_assertions[0].select matched 2 spans; it must select exactly one"],
    ],
)
def test_unrelated_diagnostic_cannot_satisfy_the_expected_rule(errors: list[str]) -> None:
    assertions = {"span_assertions": [{"select": {"name": "resumed"}}, {"select": {"name": "other"}}]}
    with pytest.raises(AssertionError, match=r"Missing .*select matched no spans") as failure:
        assert_span_error(errors, assertions, {"name": "resumed"}, "select matched no spans")
    assert repr(errors) in str(failure.value)


@pytest.mark.parametrize("selectors", [[], [{"name": "Invocation"}, {"name": "Invocation"}]])
def test_missing_or_ambiguous_selector_identity_fails(selectors: list[dict]) -> None:
    assertions = {"span_assertions": [{"select": selector} for selector in selectors]}
    with pytest.raises(AssertionError, match="Expected one assertion"):
        assert_span_error(
            ["span_assertions[0].select matched no spans"],
            assertions,
            {"name": "Invocation"},
            "select matched no spans",
        )
