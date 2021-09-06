/*
 * Copyright 2016-2021 Crown Copyright
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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

public class PropertiesTest {

    @Test
    public void shouldConstructEmptyProperties() {
        final Properties properties = new Properties();

        assertThat(properties).isEmpty();
    }

    @Test
    public void shouldConstructPropertiesWithOtherProperties() {
        final Map<String, Object> otherProperties = new HashMap<>();
        otherProperties.put("propertyName", "property value");

        final Properties properties = new Properties(otherProperties);

        assertThat(properties).hasSize(1);
        assertEquals("property value", properties.get("propertyName"));
    }

    @Test
    public void shouldConstructPropertiesWithProperty() {
        final Properties properties = new Properties("propertyName", "property value");

        assertThat(properties).hasSize(1);
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
        assertThat(properties).hasSize(2);
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
        assertThat(properties).hasSize(2);
        assertEquals(propertyValue1, properties.get(property1));
        assertEquals(propertyValue3, properties.get(property3));
    }

    @Test
    public void shouldRemovePropertyIfAddedWithNullValue() {
        // Given
        final Properties properties = new Properties();
        properties.put("property1", "propertyValue1");
        properties.put("property2", "propertyValue2");

        // When
        properties.put("property1", null);

        // Then
        assertThat(properties).hasSize(1);
        assertThat(properties.get("property1")).isNull();
    }

    @Test
    public void shouldNotAddPropertyIfPropertyNameIsNull() {
        final Properties properties = new Properties();
        properties.put(null, "propertyValue1");

        assertThat(properties).isEmpty();
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
        assertThat(clone).hasSize(2);
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
        assertThat(toString)
                .contains("property 1=<java.lang.String>property value 1")
                .contains("property 2=<java.lang.String>property value 2");
    }
}
