/*
 * Copyright 2016 Crown Copyright
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

package uk.gov.gchq.gaffer.data.element;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
public class PropertiesTest {
    @Test
    public void shouldConstructEmptyProperties() {
        // Given

        // When
        final Properties properties = new Properties();

        // Then
        assertTrue(properties.isEmpty());
    }

    @Test
    public void shouldConstructPropertiesWithOtherProperties() {
        // Given
        final Map<String, Object> otherProperties = new HashMap<>();
        otherProperties.put("propertyName", "property value");

        // When
        final Properties properties = new Properties(otherProperties);

        // Then
        assertEquals(1, properties.size());
        assertEquals("property value", properties.get("propertyName"));
    }

    @Test
    public void shouldConstructPropertiesWithProperty() {
        // Given
        // When
        final Properties properties = new Properties("propertyName", "property value");

        // Then
        assertEquals(1, properties.size());
        assertEquals("property value", properties.get("propertyName"));
    }

    @Test
    public void shouldRemoveProperties() {
        // Given
        final String property1 = "property 1";
        final String property2 = "property 2";
        final String property3 = "property 3";
        final String property4 = "property 4";
        final String propertyValue1 = "property value 1";
        final String propertyValue2 = "property value 2";
        final String propertyValue3 = "property value 3";
        final String propertyValue4 = "property value 4";
        final Collection<String> propertiesToRemove = Arrays.asList(property2, property4);

        final Properties properties = new Properties();
        properties.put(property1, propertyValue1);
        properties.put(property2, propertyValue2);
        properties.put(property3, propertyValue3);
        properties.put(property4, propertyValue4);

        // When
        properties.remove(propertiesToRemove);

        // Then
        assertEquals(2, properties.size());
        assertEquals(propertyValue1, properties.get(property1));
        assertEquals(propertyValue3, properties.get(property3));
    }

    @Test
    public void shouldKeepOnlyGivenProperties() {
        // Given
        final String property1 = "property 1";
        final String property2 = "property 2";
        final String property3 = "property 3";
        final String property4 = "property 4";
        final String propertyValue1 = "property value 1";
        final String propertyValue2 = "property value 2";
        final String propertyValue3 = "property value 3";
        final String propertyValue4 = "property value 4";
        final Collection<String> propertiesToKeep = Arrays.asList(property1, property3);

        final Properties properties = new Properties();
        properties.put(property1, propertyValue1);
        properties.put(property2, propertyValue2);
        properties.put(property3, propertyValue3);
        properties.put(property4, propertyValue4);

        // When
        properties.keepOnly(propertiesToKeep);

        // Then
        assertEquals(2, properties.size());
        assertEquals(propertyValue1, properties.get(property1));
        assertEquals(propertyValue3, properties.get(property3));
    }

    @Test
    public void shouldCloneProperties() {
        // Given
        final String property1 = "property 1";
        final String property2 = "property 2";
        final String propertyValue1 = "property value 1";
        final String propertyValue2 = "property value 2";
        final Properties properties = new Properties();
        properties.put(property1, propertyValue1);
        properties.put(property2, propertyValue2);

        // When
        final Properties clone = properties.clone();

        // Then
        assertEquals(2, clone.size());
        assertNotSame(properties, clone);
        assertEquals(propertyValue1, clone.get(property1));
        assertEquals(propertyValue2, clone.get(property2));
    }

    @Test
    public void shouldReturnHumanReadableToString() {
        // Given
        final String property1 = "property 1";
        final String property2 = "property 2";
        final String propertyValue1 = "property value 1";
        final String propertyValue2 = "property value 2";
        final Properties properties = new Properties();
        properties.put(property1, propertyValue1);
        properties.put(property2, propertyValue2);

        // When
        final String toString = properties.toString();

        // Then
        assertTrue(toString.contains("property 2="
                + "<java.lang.String>property value 2"));
        assertTrue(toString.contains("property 1="
                + "<java.lang.String>property value 1"));
    }
}
