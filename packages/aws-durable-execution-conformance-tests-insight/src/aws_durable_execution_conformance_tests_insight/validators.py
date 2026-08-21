# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""The ``InsightAssertions`` matcher.

Implements the requirement-vs-record contract:

* ``record_count`` / ``min_record_count`` / ``max_record_count`` -- record
  cardinality (``record_count: 0`` is the absence assertion);
* ``records[]`` -- ordered, index-aligned, partial ``expect`` mappings plus
  ``absent`` key lists, each optionally carrying ``operations[]``
  (``select`` + ``count`` + ``expect`` + ``absent``) and/or
  ``operations_by_name`` (name -> ``expect`` + ``absent``).

Value semantics: ``'*'`` matches any present value; ``/regex/`` (or
``${/regex/}``) is a regex search; ``$any_of: [...]`` is a set of alternatives;
``${NAME}`` binds on first match and must stay consistent within the
requirement. Mappings match partially; sequences match in order with exact
length. ``absent`` asserts a key is **not present** -- a present ``null`` is a
violation. Core-resolved placeholders and ``EXECUTION_ARN`` are expected to be
substituted before matching. Every failure yields a human-readable string;
an empty list means the records satisfy the assertions.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Any

from aws_durable_execution_conformance_tests_insight.model import InsightRecord, RecordQuery

_WRAPPED_REGEX = re.compile(r"^\$\{/(.*)/\}$", re.DOTALL)
_BARE_REGEX = re.compile(r"^/(.*)/$", re.DOTALL)
_PLACEHOLDER = re.compile(r"^\$\{([A-Za-z_][A-Za-z0-9_]*)\}$")


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_sequence(value: Any) -> bool:
    return isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray))


def _as_regex(value: Any) -> re.Pattern[str] | None:
    if not isinstance(value, str):
        return None
    match = _WRAPPED_REGEX.match(value) or _BARE_REGEX.match(value)
    if not match:
        return None
    try:
        return re.compile(match.group(1))
    except re.error:
        return None


def _as_placeholder(value: Any) -> str | None:
    if not isinstance(value, str) or _as_regex(value) is not None:
        return None
    match = _PLACEHOLDER.match(value)
    return match.group(1) if match else None


class _InsightMatcher:
    """Stateful matcher; ``${NAME}`` bindings live for one requirement pass."""

    def __init__(self) -> None:
        self._bindings: dict[str, Any] = {}

    # -- record-set entry point ------------------------------------------------

    def validate(
        self,
        records: list[InsightRecord],
        assertions: Mapping[str, Any],
        query: RecordQuery | None = None,
    ) -> list[str]:
        del query  # EXECUTION_ARN is already substituted into the assertions.
        if not isinstance(assertions, Mapping):
            return ["InsightAssertions must be a mapping"]

        errors: list[str] = []
        dicts = [record.to_dict() for record in records]

        errors.extend(self._count_errors(assertions, len(records)))

        if "records" in assertions:
            expected_records = assertions["records"]
            if not _is_sequence(expected_records):
                errors.append("records must be a sequence")
            else:
                for index, spec in enumerate(expected_records):
                    path = f"records[{index}]"
                    if not isinstance(spec, Mapping):
                        errors.append(f"{path} must be a mapping")
                        continue
                    if index >= len(records):
                        errors.append(f"{path}: expected a record at this index, found only {len(records)}")
                        continue
                    errors.extend(self._match_record(spec, dicts[index], records[index], path))
        return errors

    # -- record cardinality ----------------------------------------------------

    def _count_errors(self, assertions: Mapping[str, Any], count: int) -> list[str]:
        errors: list[str] = []
        if "record_count" in assertions:
            expected = assertions["record_count"]
            if not _is_int(expected):
                errors.append("record_count must be an integer")
            elif count != expected:
                errors.append(f"record_count: expected {expected} record(s), found {count}")
        if "min_record_count" in assertions:
            minimum = assertions["min_record_count"]
            if not _is_int(minimum):
                errors.append("min_record_count must be an integer")
            elif count < minimum:
                errors.append(f"min_record_count: expected at least {minimum} record(s), found {count}")
        if "max_record_count" in assertions:
            maximum = assertions["max_record_count"]
            if not _is_int(maximum):
                errors.append("max_record_count must be an integer")
            elif count > maximum:
                errors.append(f"max_record_count: expected at most {maximum} record(s), found {count}")
        return errors

    # -- one record ------------------------------------------------------------

    def _match_record(
        self,
        spec: Mapping[str, Any],
        record_dict: Mapping[str, Any],
        record: InsightRecord,
        path: str,
    ) -> list[str]:
        errors: list[str] = []
        if "expect" in spec:
            expect = spec["expect"]
            if not isinstance(expect, Mapping):
                errors.append(f"{path}.expect must be a mapping")
            else:
                errors.extend(self._expect_errors(expect, record_dict, f"{path}.expect"))
        if "absent" in spec:
            errors.extend(self._absent_errors(spec["absent"], record_dict, path))
        if "operations" in spec:
            errors.extend(self._match_operations(spec["operations"], record, f"{path}.operations"))
        if "operations_by_name" in spec:
            errors.extend(self._match_by_name(spec["operations_by_name"], record, f"{path}.operations_by_name"))
        return errors

    def _match_operations(self, op_specs: Any, record: InsightRecord, path: str) -> list[str]:
        if record.operations is None:
            return [f"{path}: record has no 'operations' array (sink emitted operationsByName?)"]
        if not _is_sequence(op_specs):
            return [f"{path} must be a sequence"]

        ops = [operation.to_dict() for operation in record.operations]
        errors: list[str] = []
        for index, spec in enumerate(op_specs):
            item_path = f"{path}[{index}]"
            if not isinstance(spec, Mapping):
                errors.append(f"{item_path} must be a mapping")
                continue
            selector = spec.get("select", {})
            if not isinstance(selector, Mapping):
                errors.append(f"{item_path}.select must be a mapping")
                continue
            matched = [op for op in ops if self._selector_matches(selector, op)]

            count = spec.get("count")
            if count is None:
                if len(matched) != 1:
                    errors.append(
                        f"{item_path}.select matched {len(matched)} operation(s); "
                        f"expected exactly one (add 'count' to match several)"
                    )
                    continue
            elif not _is_int(count):
                errors.append(f"{item_path}.count must be an integer")
                continue
            elif len(matched) != count:
                errors.append(f"{item_path}.select matched {len(matched)} operation(s); expected {count}")
                continue

            expect = spec.get("expect")
            absent = spec.get("absent")
            for match_index, op in enumerate(matched):
                op_path = item_path if len(matched) == 1 else f"{item_path}[{match_index}]"
                if expect is not None:
                    if not isinstance(expect, Mapping):
                        errors.append(f"{op_path}.expect must be a mapping")
                    else:
                        errors.extend(self._expect_errors(expect, op, f"{op_path}.expect"))
                if absent is not None:
                    errors.extend(self._absent_errors(absent, op, op_path))
        return errors

    def _match_by_name(self, specs: Any, record: InsightRecord, path: str) -> list[str]:
        if record.operations_by_name is None:
            return [f"{path}: record has no 'operationsByName' map (sink emitted operations array?)"]
        if not isinstance(specs, Mapping):
            return [f"{path} must be a mapping"]

        summaries = {name: summary.to_dict() for name, summary in record.operations_by_name.items()}
        errors: list[str] = []
        for name, spec in specs.items():
            name_path = f"{path}.{name}"
            if name not in summaries:
                errors.append(f"{name_path}: operation name not present in operationsByName")
                continue
            if not isinstance(spec, Mapping):
                errors.append(f"{name_path} must be a mapping")
                continue
            expect = spec.get("expect")
            absent = spec.get("absent")
            if expect is not None:
                if not isinstance(expect, Mapping):
                    errors.append(f"{name_path}.expect must be a mapping")
                else:
                    errors.extend(self._expect_errors(expect, summaries[name], f"{name_path}.expect"))
            if absent is not None:
                errors.extend(self._absent_errors(absent, summaries[name], name_path))
        return errors

    # -- value matching --------------------------------------------------------

    def _absent_errors(self, absent: Any, actual: Mapping[str, Any], path: str) -> list[str]:
        if not _is_sequence(absent):
            return [f"{path}.absent must be a sequence"]
        errors: list[str] = []
        for key in absent:
            if key in actual:
                errors.append(f"{path}: key {key!r} must be absent but is present (value {actual[key]!r})")
        return errors

    def _expect_errors(self, expected: Any, actual: Any, path: str) -> list[str]:
        if expected == "*":
            return []
        regex = _as_regex(expected)
        if regex is not None:
            if regex.search(str(actual)):
                return []
            return [f"{path}: {actual!r} does not match regex {regex.pattern!r}"]
        name = _as_placeholder(expected)
        if name is not None:
            if name in self._bindings:
                if self._bindings[name] == actual:
                    return []
                return [f"{path}: placeholder ${{{name}}} bound to {self._bindings[name]!r}, found {actual!r}"]
            self._bindings[name] = actual
            return []
        if isinstance(expected, Mapping) and set(expected) == {"$any_of"}:
            alternatives = expected["$any_of"]
            if not _is_sequence(alternatives) or not alternatives:
                return [f"{path}.$any_of must be a non-empty sequence"]
            if any(self._value_matches(alternative, actual) for alternative in alternatives):
                return []
            return [f"{path}: {actual!r} does not match any $any_of alternative"]
        if isinstance(expected, Mapping):
            if not isinstance(actual, Mapping):
                return [f"{path}: expected a mapping"]
            errors: list[str] = []
            for key, value in expected.items():
                child_path = f"{path}.{key}"
                if key not in actual:
                    errors.append(f"{child_path}: property is missing")
                else:
                    errors.extend(self._expect_errors(value, actual[key], child_path))
            return errors
        if _is_sequence(expected):
            if not _is_sequence(actual):
                return [f"{path}: expected a sequence"]
            if len(expected) != len(actual):
                return [f"{path}: expected {len(expected)} item(s), found {len(actual)}"]
            errors = []
            for index, (expected_item, actual_item) in enumerate(zip(expected, actual, strict=True)):
                errors.extend(self._expect_errors(expected_item, actual_item, f"{path}[{index}]"))
            return errors
        if expected != actual:
            return [f"{path}: expected {expected!r}, found {actual!r}"]
        return []

    def _value_matches(self, expected: Any, actual: Any) -> bool:
        """Non-binding boolean match used for selectors and ``$any_of``."""

        if expected == "*":
            return True
        regex = _as_regex(expected)
        if regex is not None:
            return bool(regex.search(str(actual)))
        if _as_placeholder(expected) is not None:
            return True
        if isinstance(expected, Mapping) and set(expected) == {"$any_of"}:
            alternatives = expected["$any_of"]
            return _is_sequence(alternatives) and any(
                self._value_matches(alternative, actual) for alternative in alternatives
            )
        if isinstance(expected, Mapping):
            return isinstance(actual, Mapping) and all(
                key in actual and self._value_matches(value, actual[key]) for key, value in expected.items()
            )
        if _is_sequence(expected):
            return (
                _is_sequence(actual)
                and len(expected) == len(actual)
                and all(self._value_matches(e, a) for e, a in zip(expected, actual, strict=True))
            )
        return expected == actual

    def _selector_matches(self, selector: Mapping[str, Any], op: Mapping[str, Any]) -> bool:
        return all(key in op and self._value_matches(value, op[key]) for key, value in selector.items())


def validate_insight_records(
    records: list[InsightRecord],
    assertions: Mapping[str, Any],
    query: RecordQuery | None = None,
) -> list[str]:
    """Validate fetched records against ``InsightAssertions``.

    A fresh matcher (fresh ``${NAME}`` bindings) is used per call so the polling
    ``accept`` callback never leaks bindings from a rejected fetch.
    """

    return _InsightMatcher().validate(records, assertions, query)
