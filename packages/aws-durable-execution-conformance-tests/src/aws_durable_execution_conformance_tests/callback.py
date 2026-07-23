# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Durable execution callback operations.

Provides a CallbackSender class that sends success, failure, and heartbeat
callbacks to Lambda durable executions via the boto3 SDK.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from botocore.exceptions import BotoCoreError, ClientError

# region Exceptions


class CallbackError(Exception):
    """Raised when a callback operation fails."""

    def __init__(self, operation: str, callback_id: str, stderr: str):
        super().__init__(f"Callback {operation} failed for callback_id={callback_id}: {stderr}")
        self.operation = operation
        self.callback_id = callback_id
        self.stderr = stderr


# endregion


# region Models


@dataclass(frozen=True)
class CallbackAction:
    """A configured callback action from the test YAML.

    Attributes:
        callback_name: Name to match against CallbackStarted events.
            Use "*" to match any callback.
        operation: One of "success", "failure", or "heartbeat".
        payload: Data to send with the callback. For success, this is
            serialized as JSON. For failure, it should contain ErrorType,
            ErrorMessage, etc.
        delay_seconds: Optional delay in seconds before sending the
            callback. Defaults to 0 (no delay). Useful for controlling
            the order of callback responses in multi-callback tests.
    """

    callback_name: str
    operation: str
    payload: Any = None
    delay_seconds: float = 0

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> CallbackAction:
        """Create a CallbackAction from a YAML-parsed dictionary.

        Args:
            data: Dictionary with CallbackName, Operation, and optional Payload.

        Returns:
            A new CallbackAction instance.

        Raises:
            ValueError: If required fields are missing or operation is invalid.
        """
        callback_name: str | None = data.get("CallbackName")
        if not callback_name:
            msg = "CallbackAction requires a 'CallbackName' field"
            raise ValueError(msg)

        operation: str | None = data.get("Operation")
        if not operation:
            msg = "CallbackAction requires an 'Operation' field"
            raise ValueError(msg)

        valid_operations: set[str] = {"success", "failure", "heartbeat"}
        if operation.lower() not in valid_operations:
            msg = f"Invalid operation '{operation}'. Must be one of: {', '.join(sorted(valid_operations))}"
            raise ValueError(msg)

        payload: Any = data.get("Payload")
        delay_seconds: float = float(data.get("Delay", 0))
        return cls(
            callback_name=callback_name,
            operation=operation.lower(),
            payload=payload,
            delay_seconds=delay_seconds,
        )


@dataclass(frozen=True)
class CallbackResult:
    """Result of a callback operation.

    Attributes:
        success: Whether the callback was sent successfully.
        operation: The operation that was performed.
        callback_id: The callback ID that was targeted.
        output: stdout from the AWS CLI command.
    """

    success: bool
    operation: str
    callback_id: str
    output: str


# endregion


# region Callback sender


class CallbackSender:
    """Sends durable execution callbacks via the boto3 SDK.

    Args:
        client: Pre-created low-level Lambda client.
    """

    def __init__(self, client: Any):
        self._client = client

    def send(self, callback_id: str, action: CallbackAction) -> CallbackResult:
        """Dispatch a callback based on the action's operation type.

        Args:
            callback_id: The callback ID from the CallbackCreated event.
            action: The CallbackAction describing what to send.

        Returns:
            CallbackResult with success status and output.

        Raises:
            CallbackError: If the SDK call fails.
        """
        match action.operation:
            case "success":
                return self._send_success(callback_id, action.payload)
            case "failure":
                return self._send_failure(callback_id, action.payload)
            case "heartbeat":
                return self._send_heartbeat(callback_id)
            case _:
                msg = f"Unknown operation: {action.operation}"
                raise ValueError(msg)

    def _send_success(
        self,
        callback_id: str,
        payload: Any | None,
    ) -> CallbackResult:
        """Send a success callback.

        Args:
            callback_id: The callback ID to respond to.
            payload: JSON-serializable result payload. Can be a string,
                number, dict, list, or any JSON-serializable value.

        Returns:
            CallbackResult with success status.

        Raises:
            CallbackError: If the SDK call fails.
        """
        kwargs: dict[str, Any] = {"CallbackId": callback_id}
        if payload is not None:
            kwargs["Result"] = json.dumps(payload).encode("utf-8")

        try:
            response: dict[str, Any] = self._client.send_durable_execution_callback_success(**kwargs)
        except (ClientError, BotoCoreError) as e:
            raise CallbackError(
                operation="success",
                callback_id=callback_id,
                stderr=str(e),
            ) from e

        return CallbackResult(
            success=True,
            operation="success",
            callback_id=callback_id,
            output=str(response),
        )

    def _send_failure(
        self,
        callback_id: str,
        payload: Any | None,
    ) -> CallbackResult:
        """Send a failure callback.

        Args:
            callback_id: The callback ID to respond to.
            payload: Dictionary with ErrorType, ErrorMessage, ErrorData,
                and/or StackTrace fields.

        Returns:
            CallbackResult with success status.

        Raises:
            CallbackError: If the SDK call fails.
        """
        kwargs: dict[str, Any] = {"CallbackId": callback_id}
        if payload is not None:
            if not isinstance(payload, dict):
                msg = (
                    f"Failure callback payload must be a dict containing"
                    f" ErrorType/ErrorMessage/ErrorData/StackTrace fields,"
                    f" got {type(payload).__name__}: {payload!r}"
                )
                raise CallbackError(
                    operation="failure",
                    callback_id=callback_id,
                    stderr=msg,
                )
            error_info: dict[str, str] = {}
            for key in ("ErrorType", "ErrorMessage", "ErrorData", "StackTrace"):
                if key in payload:
                    error_info[key] = payload[key]
            if error_info:
                kwargs["Error"] = error_info

        try:
            response: dict[str, Any] = self._client.send_durable_execution_callback_failure(**kwargs)
        except (ClientError, BotoCoreError) as e:
            raise CallbackError(
                operation="failure",
                callback_id=callback_id,
                stderr=str(e),
            ) from e

        return CallbackResult(
            success=True,
            operation="failure",
            callback_id=callback_id,
            output=str(response),
        )

    def _send_heartbeat(self, callback_id: str) -> CallbackResult:
        """Send a heartbeat callback.

        Args:
            callback_id: The callback ID to send heartbeat for.

        Returns:
            CallbackResult with success status.

        Raises:
            CallbackError: If the SDK call fails.
        """
        try:
            response: dict[str, Any] = self._client.send_durable_execution_callback_heartbeat(
                CallbackId=callback_id,
            )
        except (ClientError, BotoCoreError) as e:
            raise CallbackError(
                operation="heartbeat",
                callback_id=callback_id,
                stderr=str(e),
            ) from e

        return CallbackResult(
            success=True,
            operation="heartbeat",
            callback_id=callback_id,
            output=str(response),
        )


# endregion
