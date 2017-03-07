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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

@RunWith(MockitoJUnitRunner.class)
public class PropertiesTupleTest {

    @Test
    public void shouldSetAndGetFields() {
        // Given
        final Properties properties1 = new Properties();
        final Properties properties2 = new Properties();
        final PropertiesTuple tuple = new PropertiesTuple();
        tuple.setProperties(properties1);

        // When / Then
        assertSame(properties1, tuple.getProperties());
        tuple.setProperties(properties2);
        assertSame(properties2, tuple.getProperties());
    }

    @Test
    public void shouldGetPropertyFromPropertiesWithinTuple() {
        // Given
        final String propertName = "property name";
        final String propertyValue = "property value";
        final Properties properties = new Properties(propertName, propertyValue);
        final PropertiesTuple tuple = new PropertiesTuple(properties);

        // When
        final Object property = tuple.get(propertName);

        // Then
        assertSame(propertyValue, property);
    }

    @Test
    public void shouldPutPropertyInTupleAndStoreInProperties() {
        // Given
        final Properties properties = new Properties();
        final PropertiesTuple tuple = new PropertiesTuple(properties);
        final String propertyName = "property name";
        final String propertyValue = "property value";

        // When
        tuple.put(propertyName, propertyValue);

        // Then
        assertEquals(propertyValue, properties.get(propertyName));
    }
}
