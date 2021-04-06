/*
 * Copyright 2021 Crown Copyright
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

package uk.gov.gchq.gaffer.rest;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static uk.gov.gchq.gaffer.rest.SystemProperty.EXPOSED_PROPERTIES;

public class PropertiesUtilTest {

    private static final String NON_CORE_PROPERTY = "non.core.property";

    @BeforeEach
    @AfterEach
    public void clearPropety() {
        System.clearProperty(NON_CORE_PROPERTY);
        System.clearProperty(EXPOSED_PROPERTIES);
    }

    @Test
    public void shouldNotExposeNonCorePropertiesByDefault() {
        // Given
        System.setProperty(NON_CORE_PROPERTY, "test");

        // When
        String property = PropertiesUtil.getProperty("non.core.property");

        // Then
        assertNull(property);
    }

    @Test
    public void shouldExposePropertyIfPropertyIsInExtraCoreProperties() {
        // Given
        System.setProperty(EXPOSED_PROPERTIES, NON_CORE_PROPERTY);
        System.setProperty(NON_CORE_PROPERTY, "test");

        // When
        String property = PropertiesUtil.getProperty("non.core.property");

        // Then
        assertEquals("test", property);
    }

    @Test
    public void shouldUseCommaDelimiterToSeparateProperties() {
        // Given
        System.setProperty(EXPOSED_PROPERTIES, "some.other.property," + NON_CORE_PROPERTY);
        System.setProperty(NON_CORE_PROPERTY, "test");

        // When
        String property = PropertiesUtil.getProperty("non.core.property");

        // Then
        assertEquals("test", property);
    }


}
