/*
 * Copyright 2017-2018 Crown Copyright
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

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.operation.impl.GetWalks;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.serialisation.util.JsonSerialisationUtil;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class JsonSerialisationUtilTest {
    @Test
    public void testGetWalks() {
        // Given
        final String className = GetWalks.class.getName();
        final Map<String, String> expectedValues = new HashMap<>();
        expectedValues.put("resultsLimit", Integer.class.getName());
        expectedValues.put("operations", "java.util.List<uk.gov.gchq.gaffer.operation.io.Output<java.lang.Iterable<uk.gov.gchq.gaffer.data.element.Element>>>");
        expectedValues.put("options", "java.util.Map<java.lang.String,java.lang.String>");
        expectedValues.put("input", "java.lang.Object[]");

        // When
        final Map<String, String> result = JsonSerialisationUtil.getSerialisedFieldClasses(className);

        // Then
        assertEquals(expectedValues.entrySet(), result.entrySet());
    }

    @Test
    public void testAddElementsFields() {
        // Given
        final String className = AddElements.class.getName();
        final Map<String, String> expectedValues = new HashMap<>();
        expectedValues.put("validate", Boolean.class.getName());
        expectedValues.put("skipInvalidElements", Boolean.class.getName());
        expectedValues.put("options", "java.util.Map<java.lang.String,java.lang.String>");
        expectedValues.put("input", "uk.gov.gchq.gaffer.data.element.Element[]");

        // When
        final Map<String, String> result = JsonSerialisationUtil.getSerialisedFieldClasses(className);

        // Then
        assertEquals(expectedValues.entrySet(), result.entrySet());
    }

    @Test
    public void testEdge() {
        // Given
        final String className = Edge.class.getName();
        final Map<String, String> expectedFields = new HashMap<>();
        expectedFields.put("class", Class.class.getName());
        expectedFields.put("source", Object.class.getName());
        expectedFields.put("destination", Object.class.getName());
        expectedFields.put("matchedVertex", String.class.getName());
        expectedFields.put("group", String.class.getName());
        expectedFields.put("properties", Properties.class.getName());
        expectedFields.put("directed", Boolean.class.getName());
        expectedFields.put("directedType", String.class.getName());

        // When
        final Map<String, String> result = JsonSerialisationUtil.getSerialisedFieldClasses(className);

        // Then
        assertEquals(8, result.size());
        assertEquals(expectedFields.entrySet(), result.entrySet());
    }
}
