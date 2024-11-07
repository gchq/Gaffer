/*
 * Copyright 2024 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.gov.gchq.gaffer.commonutil.otel;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;

public final class OtelUtil {

    public static final String USER_ATTRIBUTE = "enduser.id";
    public static final String JOB_ID_ATTRIBUTE = "gaffer.jobId";
    public static final String GRAPH_ID_ATTRIBUTE = "gaffer.graphId";
    public static final String VIEW_ATTRIBUTE = "gaffer.view";
    public static final String GREMLIN_QUERY_ATTRIBUTE = "gaffer.gremlin.query";

    private static boolean openTelemetryActive = false;

    private OtelUtil() {
        // Utility class
    }

    /**
     * Creates a new span with the given tracer and span names, note will
     * return an 'invalid' span if OpenTelementry is turned off.
     *
     * @param tracerName Name of the Tracer to use
     * @param spanName Name of the {@link Span} to use.
     * @return new {@link Span}
     */
    public static Span startSpan(final String tracerName, final String spanName) {
        // If not using opentelementry use dummy spans
        if (!openTelemetryActive) {
            return Span.getInvalid();
        }
        // Create span and set attributes
        return GlobalOpenTelemetry
            .getTracer(tracerName)
            .spanBuilder(spanName)
            .startSpan();
    }

    /**
     * Get if OpenTelemetry is in use.
     *
     * @return Is active
     */
    public static boolean getOpenTelemetryActive() {
        return openTelemetryActive;
    }

    /**
     * Set if OpenTelemetry is in use.
     *
     * @param active Is active
     */
    public static void setOpenTelemetryActive(final boolean active) {
        openTelemetryActive = active;
    }
}
