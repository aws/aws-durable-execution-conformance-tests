// SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
//
// SPDX-License-Identifier: Apache-2.0
package software.amazon.lambda.durable.conformance.otel;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.Test;

public final class OtelConformanceHandlerTest {

    @Test
    public void appliesAuthenticatedOtlpHeaders() {
        var headers = new LinkedHashMap<String, String>();

        OtelConformanceHandler.applyOtlpHeaders(
                "authorization=Bearer%20secret,x-scope=tenant%2Cone", headers::put);

        assertEquals(
                Map.of("authorization", "Bearer secret", "x-scope", "tenant,one"), headers);
    }

    @Test
    public void acceptsMissingOtlpHeaders() {
        var headers = new LinkedHashMap<String, String>();

        OtelConformanceHandler.applyOtlpHeaders(null, headers::put);
        OtelConformanceHandler.applyOtlpHeaders("  ", headers::put);

        assertTrue(headers.isEmpty());
    }

    @Test
    public void rejectsMalformedOtlpHeadersWithoutExposingTheirContents() {
        var error =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                OtelConformanceHandler.applyOtlpHeaders(
                                        "authorization", (name, value) -> {}));

        assertEquals(
                "Invalid OTEL_EXPORTER_OTLP_HEADERS entry at position 1", error.getMessage());
    }
}
