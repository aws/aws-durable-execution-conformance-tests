# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Deterministic OTLP/HTTP collector used by backend-neutral conformance runs."""

from __future__ import annotations

import argparse
import json
import threading
import urllib.parse
from collections.abc import Iterable
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from google.protobuf.message import DecodeError
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
    ExportTraceServiceResponse,
)

from aws_durable_execution_conformance_tests_otel.model import Trace, trace_to_dict
from aws_durable_execution_conformance_tests_otel.normalizers import (
    normalize_otlp_json,
    normalize_otlp_protobuf,
)


class TraceStore:
    """Thread-safe in-memory canonical trace store."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._traces: dict[str, Trace] = {}

    def ingest(self, traces: Iterable[Trace]) -> None:
        with self._lock:
            for trace in traces:
                existing = self._traces.get(trace.trace_id)
                if existing is None:
                    self._traces[trace.trace_id] = trace
                    continue
                by_span = {span.span_id: span for span in existing.spans}
                by_span.update({span.span_id: span for span in trace.spans})
                self._traces[trace.trace_id] = Trace(
                    trace_id=trace.trace_id,
                    spans=tuple(by_span.values()),
                    log_trace_ids=tuple(dict.fromkeys((*existing.log_trace_ids, *trace.log_trace_ids))),
                )

    def find(
        self,
        *,
        execution_arn: str | None,
        service_name: str | None,
        trace_id: str | None,
    ) -> Trace | None:
        with self._lock:
            traces = list(self._traces.values())
        if trace_id:
            return next((trace for trace in traces if trace.trace_id == trace_id), None)
        for trace in traces:
            has_execution = not execution_arn or any(
                execution_arn in {str(value) for value in span.attributes.values()} for span in trace.spans
            )
            has_service = not service_name or any(span.service_name == service_name for span in trace.spans)
            if has_execution and has_service:
                return trace
        return None

    def clear(self) -> None:
        with self._lock:
            self._traces.clear()


def _handler(store: TraceStore) -> type[BaseHTTPRequestHandler]:
    class OtlpHandler(BaseHTTPRequestHandler):
        server_version = "DurableExecutionOtlpCollector/1"

        def log_message(self, _format: str, *args: Any) -> None:
            del args

        def _json(
            self,
            status: HTTPStatus,
            payload: dict[str, Any],
        ) -> None:
            body = json.dumps(payload).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:
            parsed = urllib.parse.urlparse(self.path)
            if parsed.path == "/health":
                self._json(HTTPStatus.OK, {"status": "ok"})
                return
            if parsed.path != "/api/traces":
                self._json(HTTPStatus.NOT_FOUND, {"error": "not found"})
                return
            query = urllib.parse.parse_qs(parsed.query)
            trace = store.find(
                execution_arn=_first(query, "execution_arn"),
                service_name=_first(query, "service_name"),
                trace_id=_first(query, "trace_id"),
            )
            self._json(
                HTTPStatus.OK,
                {"trace": trace_to_dict(trace) if trace is not None else None},
            )

        def do_DELETE(self) -> None:
            if self.path != "/api/traces":
                self._json(HTTPStatus.NOT_FOUND, {"error": "not found"})
                return
            store.clear()
            self._json(HTTPStatus.OK, {"status": "cleared"})

        def do_POST(self) -> None:
            if self.path != "/v1/traces":
                self._json(HTTPStatus.NOT_FOUND, {"error": "not found"})
                return
            try:
                size = int(self.headers.get("Content-Length", "0"))
                body = self.rfile.read(size)
                content_type = self.headers.get("Content-Type", "")
                if "json" in content_type:
                    payload = json.loads(body or b"{}")
                    traces = normalize_otlp_json(payload)
                else:
                    traces = normalize_otlp_protobuf(body)
                store.ingest(traces)
            except (DecodeError, ValueError, TypeError):
                self._json(
                    HTTPStatus.BAD_REQUEST,
                    {"error": "invalid OTLP trace payload"},
                )
                return

            if "json" in content_type:
                self._json(HTTPStatus.OK, {})
                return
            response = ExportTraceServiceResponse().SerializeToString()
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/x-protobuf")
            self.send_header("Content-Length", str(len(response)))
            self.end_headers()
            self.wfile.write(response)

    return OtlpHandler


def _first(query: dict[str, list[str]], key: str) -> str | None:
    values = query.get(key)
    return values[0] if values else None


class CollectorServer:
    """Embeddable OTLP collector with an HTTP query API."""

    def __init__(self, host: str = "127.0.0.1", port: int = 0) -> None:
        self.store = TraceStore()
        self._server = ThreadingHTTPServer((host, port), _handler(self.store))
        self._thread: threading.Thread | None = None

    @property
    def endpoint(self) -> str:
        host, port = self._server.server_address[:2]
        if isinstance(host, bytes):
            host = host.decode()
        return f"http://{host}:{port}"

    def start(self) -> CollectorServer:
        if self._thread is None:
            self._thread = threading.Thread(
                target=self._server.serve_forever,
                name="otel-test-collector",
                daemon=True,
            )
            self._thread.start()
        return self

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None

    def __enter__(self) -> CollectorServer:
        return self.start()

    def __exit__(self, *_args: object) -> None:
        self.close()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Run the conformance OTLP test collector")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=4318)
    args = parser.parse_args(argv)

    server = CollectorServer(args.host, args.port)
    print(f"OTLP collector listening at {server.endpoint}", flush=True)
    try:
        server._server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server._server.server_close()


if __name__ == "__main__":
    main()
