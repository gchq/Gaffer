/*
 * Copyright 2017 Crown Copyright
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
package uk.gov.gchq.gaffer.rest.serialisation;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class JsonSerialisationUtilTest {

    @Test
    public void testIsIn() {
        // Given
        final String className = "uk.gov.gchq.koryphe.impl.predicate.IsIn";
        final Map<String, String> expectedValues = new HashMap<>();
        expectedValues.put("values", "java.lang.Object[]");

        // When
        final Map<String, String> result = JsonSerialisationUtil.getSerialisedFieldClasses(className);

        // Then
        assertEquals(expectedValues.entrySet(), result.entrySet());
    }

    @Test
    public void testInRange() {
        // Given
        final String className = "uk.gov.gchq.koryphe.impl.predicate.range.InRange";
        final Map<String, String> expectedValues = new HashMap<>();
        expectedValues.put("start", "T");
        expectedValues.put("end", "T");
        expectedValues.put("startInclusive", "java.lang.Boolean");
        expectedValues.put("endInclusive", "java.lang.Boolean");

        // When
        final Map<String, String> result = JsonSerialisationUtil.getSerialisedFieldClasses(className);

        // Then
        assertEquals(expectedValues.entrySet(), result.entrySet());
    }

    @Test
    public void testInDateRangeAndInTimeRange() {
        // Given
        final String classNameIDR = "uk.gov.gchq.koryphe.impl.predicate.range.InDateRange";
        final String classNameITR = "uk.gov.gchq.koryphe.impl.predicate.range.InTimeRange";
        final Map<String, String> expectedValues = new HashMap<>();
        expectedValues.put("timeUnit", "java.lang.String");
        expectedValues.put("offsetUnit", "java.lang.String");
        expectedValues.put("start", "java.lang.String");
        expectedValues.put("startOffset", "java.lang.Long");
        expectedValues.put("startInclusive", "java.lang.Boolean");
        expectedValues.put("end", "java.lang.String");
        expectedValues.put("endOffset", "java.lang.Long");
        expectedValues.put("endInclusive", "java.lang.Boolean");

        // When
        final Map<String, String> resultIDR = JsonSerialisationUtil.getSerialisedFieldClasses(classNameIDR);
        final Map<String, String> resultITR = JsonSerialisationUtil.getSerialisedFieldClasses(classNameITR);

        // Then
        assertEquals(expectedValues.entrySet(), resultIDR.entrySet());
        assertEquals(resultIDR.entrySet(), resultITR.entrySet());
    }

    @Test
    public void testGetWalks() {
        // Given
        final String className = "uk.gov.gchq.gaffer.operation.impl.GetWalks";
        final Map<String, String> expectedValues = new HashMap<>();
        expectedValues.put("resultsLimit", "java.lang.Integer");
        expectedValues.put("operations", "java.util.List<uk.gov.gchq.gaffer.operation.io.Output<java.lang.Iterable<uk.gov.gchq.gaffer.data.element.Element>>>");
        expectedValues.put("options", "java.util.Map<java.lang.String, java.lang.String>");
        expectedValues.put("input", "java.lang.Iterable<? extends uk.gov.gchq.gaffer.data.element.id.EntityId>");

        // When
        final Map<String, String> result = JsonSerialisationUtil.getSerialisedFieldClasses(className);

        // Then
        assertEquals(expectedValues.entrySet(), result.entrySet());
    }

    @Test
    public void testClassWithNoGettersOrSetters() {
        // Given
        final String className = "uk.gov.gchq.koryphe.impl.predicate.IsTrue";

        // When
        final Map<String, String> result = JsonSerialisationUtil.getSerialisedFieldClasses(className);

        // Then
        assertTrue(result.entrySet().isEmpty());
    }

    @Test
    public void testClassContainingJsonAnnotations() {
        // Given
        final String className = "uk.gov.gchq.gaffer.data.graph.Walk";
        final Map<String, String> expectedValues = new HashMap<>();
        expectedValues.put("edges", "java.util.List<java.util.Set<uk.gov.gchq.gaffer.data.element.Edge>>");
        expectedValues.put("entities", "java.util.List<java.util.Map.java.util.Map$Entry<java.lang.Object, java.util.Set<uk.gov.gchq.gaffer.data.element.Entity>>>");

        // When
        final Map<String, String> result = JsonSerialisationUtil.getSerialisedFieldClasses(className);

        // Then]
        assertEquals(expectedValues.entrySet(), result.entrySet());
    }

    @Test
    public void testEdge() {
        // Given
        final String className = "uk.gov.gchq.gaffer.data.element.Edge";
        final Map<String, String> expectedFields = new HashMap<>();
        expectedFields.put("class", "uk.gov.gchq.gaffer.data.element.Edge");
        expectedFields.put("source", "java.lang.Object");
        expectedFields.put("destination", "java.lang.Object");
        expectedFields.put("matchedVertex", "java.lang.String");
        expectedFields.put("group", "java.lang.String");
        expectedFields.put("properties", "uk.gov.gchq.gaffer.data.element.Properties");
        expectedFields.put("directed", "boolean");

        // When
        final Map<String, String> result = JsonSerialisationUtil.getSerialisedFieldClasses(className);

        // Then
        assertEquals(result.entrySet(), expectedFields.entrySet());
    }
}
