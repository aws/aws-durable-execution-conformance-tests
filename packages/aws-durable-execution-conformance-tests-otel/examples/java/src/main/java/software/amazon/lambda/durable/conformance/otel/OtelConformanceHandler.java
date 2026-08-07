// SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
//
// SPDX-License-Identifier: Apache-2.0
package software.amazon.lambda.durable.conformance.otel;

import java.util.Map;
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
        return DurableConfig.builder().withPlugins(createPlugin()).build();
    }

    private DurableExecutionPlugin createPlugin() {
        var executionView = "execution".equals(System.getenv("OTEL_PLUGIN_MODE"));
        var classNames = executionView
                ? new String[] {"software.amazon.lambda.durable.otel.ExecutionOtelPlugin"}
                : new String[] {
                    "software.amazon.lambda.durable.otel.InvocationOtelPlugin",
                    "software.amazon.lambda.durable.otel.OtelPlugin"
                };
        for (var className : classNames) {
            try {
                var pluginClass = Class.forName(className);
                return (DurableExecutionPlugin) pluginClass.getConstructor().newInstance();
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
