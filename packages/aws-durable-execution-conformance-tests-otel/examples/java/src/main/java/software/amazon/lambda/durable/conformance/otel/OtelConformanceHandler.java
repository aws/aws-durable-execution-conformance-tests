// SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
//
// SPDX-License-Identifier: Apache-2.0
package software.amazon.lambda.durable.conformance.otel;

import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import java.util.Map;
import software.amazon.lambda.durable.DurableConfig;
import software.amazon.lambda.durable.DurableHandler;
import software.amazon.lambda.durable.TypeToken;
import software.amazon.lambda.durable.otel.OtelPlugin;

abstract class OtelConformanceHandler<O> extends DurableHandler<Map<String, Object>, O> {

    protected OtelConformanceHandler() {
        super(new TypeToken<Map<String, Object>>() {});
    }

    @Override
    protected final DurableConfig createConfiguration() {
        var exporter = OtlpGrpcSpanExporter.getDefault();
        var plugin =
                new OtelPlugin(SdkTracerProvider.builder().addSpanProcessor(SimpleSpanProcessor.create(exporter)));
        return DurableConfig.builder().withPlugins(plugin).build();
    }

    protected final void requireScenario(Map<String, Object> event, String expected) {
        var actual = event.get("scenario");
        if (!expected.equals(actual)) {
            throw new IllegalArgumentException("Expected scenario " + expected + ", received " + actual);
        }
    }
}
