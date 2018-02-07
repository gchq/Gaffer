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
        final String classname = "uk.gov.gchq.koryphe.impl.predicate.range.InRange";
        final Map<String, String> expectedValues = new HashMap<>();
        expectedValues.put("start", "T");
        expectedValues.put("end", "T");
        expectedValues.put("startInclusive", "java.lang.Boolean");
        expectedValues.put("endInclusive", "java.lang.Boolean");

        // When
        final Map<String, String> result = JsonSerialisationUtil.getSerialisedFieldClasses(classname);

        // Then
        assertEquals(expectedValues.entrySet(), result.entrySet());
    }

    // TODO fails but shouldn't
    @Test
    public void testGetWalks() {
        // Given
        final String className = "uk.gov.gchq.gaffer.operation.impl.GetWalks";
        final Map<String, String> expectedValues = new HashMap<>();
        expectedValues.put("resultsLimit", "java.lang.Integer");
        expectedValues.put("operations", "java.util.List<uk.gov.gchq.gaffer.operation.io.Output<java.lang.Iterable<uk.gov.gchq.gaffer.data.element.Element>>>");
        expectedValues.put("options", "java.util.Map<java.lang.String, java.lang.String");
        expectedValues.put("input", "java.lang.Iterable<? extends uk.gov.gchq.gaffer.data.element.id.EntityId");

        // When
        final Map<String, String> result = JsonSerialisationUtil.getSerialisedFieldClasses(className);

        // Then
        assertEquals(expectedValues.entrySet(), result.entrySet());
    }


    /* TODO:
    class with annotations in self
    class with annotations in parent class
    class with multiple setters/getters
    class with no getters/setters?
    class with generic parameters
    */

}
