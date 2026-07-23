// SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
//
// SPDX-License-Identifier: Apache-2.0
package software.amazon.lambda.durable.conformance.otel;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import java.util.Map;
import software.amazon.distro.opentelemetry.exporter.xray.udp.trace.AwsXrayUdpSpanExporterBuilder;
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
        var exporter = createExporter();
        var resource =
                Resource.getDefault()
                        .merge(
                                Resource.create(
                                        Attributes.of(
                                                AttributeKey.stringKey("service.name"),
                                                System.getenv().getOrDefault("OTEL_SERVICE_NAME", "invocation"))));
        var plugin = new OtelPlugin(
                SdkTracerProvider.builder()
                        .setResource(resource)
                        .addSpanProcessor(SimpleSpanProcessor.create(exporter)));
        return DurableConfig.builder().withPlugins(plugin).build();
    }

    private SpanExporter createExporter() {
        var otlpEndpoint = System.getenv("OTEL_EXPORTER_OTLP_ENDPOINT");
        if (otlpEndpoint != null && !otlpEndpoint.isBlank()) {
            return OtlpGrpcSpanExporter.builder().setEndpoint(otlpEndpoint).build();
        }
        return new AwsXrayUdpSpanExporterBuilder()
                .setEndpoint(
                        System.getenv()
                                .getOrDefault("AWS_XRAY_DAEMON_ADDRESS", "127.0.0.1:2000"))
                .build();
    }

    protected final void requireScenario(Map<String, Object> event, String expected) {
        var actual = event.get("scenario");
        if (!expected.equals(actual)) {
            throw new IllegalArgumentException("Expected scenario " + expected + ", received " + actual);
        }
    }
}
