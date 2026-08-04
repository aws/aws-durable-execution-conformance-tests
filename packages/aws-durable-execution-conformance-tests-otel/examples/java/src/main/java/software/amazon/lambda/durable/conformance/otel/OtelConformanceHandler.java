// SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
//
// SPDX-License-Identifier: Apache-2.0
package software.amazon.lambda.durable.conformance.otel;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.SdkTracerProviderBuilder;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import software.amazon.distro.opentelemetry.exporter.xray.udp.trace.AwsXrayUdpSpanExporterBuilder;
import software.amazon.lambda.durable.DurableConfig;
import software.amazon.lambda.durable.DurableHandler;
import software.amazon.lambda.durable.TypeToken;
import software.amazon.lambda.durable.plugin.DurableExecutionPlugin;

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
                                                System.getenv()
                                                        .getOrDefault(
                                                                "OTEL_SERVICE_NAME",
                                                                "durable-execution-conformance"))));
        var plugin = createPlugin(
                SdkTracerProvider.builder()
                        .setResource(resource)
                        .addSpanProcessor(SimpleSpanProcessor.create(exporter)));
        return DurableConfig.builder().withPlugins(plugin).build();
    }

    private DurableExecutionPlugin createPlugin(SdkTracerProviderBuilder tracerProviderBuilder) {
        var executionView = "execution".equals(System.getenv("OTEL_PLUGIN_MODE"));
        var classNames = executionView
                ? new String[] {"software.amazon.lambda.durable.otel.ExecutionOtelPlugin"}
                : new String[] {
                    "software.amazon.lambda.durable.otel.InvocationOtelPlugin",
                    "software.amazon.lambda.durable.otel.OtelPlugin"
                };
        for (var className : classNames) {
            try {
                return (DurableExecutionPlugin)
                        Class.forName(className)
                                .getConstructor(SdkTracerProviderBuilder.class)
                                .newInstance(tracerProviderBuilder);
            } catch (ClassNotFoundException error) {
                // The preview plugin was renamed after the 2.1.0 release.
            } catch (ReflectiveOperationException error) {
                throw new IllegalStateException(
                        "Could not initialize OpenTelemetry plugin " + className, error);
            }
        }
        throw new IllegalStateException(
                "No supported Java OpenTelemetry plugin is available for "
                        + (executionView ? "execution" : "invocation")
                        + " view");
    }

    private SpanExporter createExporter() {
        var otlpEndpoint = System.getenv("OTEL_EXPORTER_OTLP_ENDPOINT");
        if (otlpEndpoint != null && !otlpEndpoint.isBlank()) {
            var builder = OtlpGrpcSpanExporter.builder().setEndpoint(otlpEndpoint);
            applyOtlpHeaders(System.getenv("OTEL_EXPORTER_OTLP_HEADERS"), builder::addHeader);
            return builder.build();
        }
        return new AwsXrayUdpSpanExporterBuilder()
                .setEndpoint(
                        System.getenv()
                                .getOrDefault("AWS_XRAY_DAEMON_ADDRESS", "127.0.0.1:2000"))
                .build();
    }

    static void applyOtlpHeaders(
            String rawHeaders, BiConsumer<String, String> addHeader) {
        parseOtlpHeaders(rawHeaders).forEach(addHeader);
    }

    private static Map<String, String> parseOtlpHeaders(String rawHeaders) {
        var headers = new LinkedHashMap<String, String>();
        if (rawHeaders == null || rawHeaders.isBlank()) {
            return headers;
        }

        var entries = rawHeaders.split(",", -1);
        for (int index = 0; index < entries.length; index++) {
            var entry = entries[index];
            var separator = entry.indexOf('=');
            if (separator <= 0) {
                throw invalidOtlpHeader(index, null);
            }
            String name;
            String value;
            try {
                name =
                        URLDecoder.decode(
                                entry.substring(0, separator).trim(), StandardCharsets.UTF_8);
                value =
                        URLDecoder.decode(
                                entry.substring(separator + 1).trim(), StandardCharsets.UTF_8);
            } catch (IllegalArgumentException error) {
                throw invalidOtlpHeader(index, error);
            }
            if (name.isBlank()) {
                throw invalidOtlpHeader(index, null);
            }
            headers.put(name, value);
        }
        return headers;
    }

    private static IllegalArgumentException invalidOtlpHeader(
            int index, IllegalArgumentException cause) {
        return new IllegalArgumentException(
                "Invalid OTEL_EXPORTER_OTLP_HEADERS entry at position " + (index + 1), cause);
    }

    protected final void requireScenario(Map<String, Object> event, String expected) {
        var actual = event.get("scenario");
        if (!expected.equals(actual)) {
            throw new IllegalArgumentException("Expected scenario " + expected + ", received " + actual);
        }
    }

    protected final long longDelaySeconds(Map<String, Object> event) {
        var rawDelay = event.get("delay_seconds");
        final long delay;
        try {
            delay = rawDelay instanceof Number
                    ? ((Number) rawDelay).longValue()
                    : Long.parseLong(String.valueOf(rawDelay));
        } catch (NumberFormatException error) {
            throw new IllegalArgumentException(
                    "delay_seconds must be an integer from 1 through 86400", error);
        }
        if (delay < 1 || delay > 86400) {
            throw new IllegalArgumentException(
                    "delay_seconds must be an integer from 1 through 86400");
        }
        return delay;
    }
}
