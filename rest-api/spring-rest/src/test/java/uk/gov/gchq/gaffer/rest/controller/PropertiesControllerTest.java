/*
 * Copyright 2020-2021 Crown Copyright
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

package uk.gov.gchq.gaffer.rest.controller;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.core.exception.GafferRuntimeException;
import uk.gov.gchq.gaffer.rest.PropertiesUtil;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static uk.gov.gchq.gaffer.core.exception.Status.NOT_FOUND;
import static uk.gov.gchq.gaffer.rest.SystemProperty.APP_BANNER_COLOUR;
import static uk.gov.gchq.gaffer.rest.SystemProperty.APP_BANNER_DESCRIPTION;
import static uk.gov.gchq.gaffer.rest.SystemProperty.APP_DESCRIPTION;
import static uk.gov.gchq.gaffer.rest.SystemProperty.APP_DESCRIPTION_DEFAULT;
import static uk.gov.gchq.gaffer.rest.SystemProperty.APP_DOCUMENTATION_URL;
import static uk.gov.gchq.gaffer.rest.SystemProperty.APP_DOCUMENTATION_URL_DEFAULT;
import static uk.gov.gchq.gaffer.rest.SystemProperty.APP_TITLE;
import static uk.gov.gchq.gaffer.rest.SystemProperty.APP_TITLE_DEFAULT;
import static uk.gov.gchq.gaffer.rest.SystemProperty.FAVICON_LARGE_URL;
import static uk.gov.gchq.gaffer.rest.SystemProperty.FAVICON_SMALL_URL;
import static uk.gov.gchq.gaffer.rest.SystemProperty.GAFFER_VERSION;
import static uk.gov.gchq.gaffer.rest.SystemProperty.GAFFER_VERSION_DEFAULT;
import static uk.gov.gchq.gaffer.rest.SystemProperty.KORYPHE_VERSION;
import static uk.gov.gchq.gaffer.rest.SystemProperty.KORYPHE_VERSION_DEFAULT;
import static uk.gov.gchq.gaffer.rest.SystemProperty.LOGO_IMAGE_URL;
import static uk.gov.gchq.gaffer.rest.SystemProperty.LOGO_IMAGE_URL_DEFAULT;
import static uk.gov.gchq.gaffer.rest.SystemProperty.LOGO_LINK;
import static uk.gov.gchq.gaffer.rest.SystemProperty.LOGO_LINK_DEFAULT;

public class PropertiesControllerTest {

    @BeforeEach
    public void clearAllProperties() {
        for (final String property : PropertiesUtil.getProperties().keySet()) {
            System.clearProperty(property);
        }
    }

    @Test
    public void shouldReturnDefaultPropertiesWhenNotConfigured() {
        // Given
        PropertiesController propertiesController = new PropertiesController();

        // When
        final Map<String, String> defaultProperties = new LinkedHashMap<>();
        defaultProperties.put(APP_TITLE, APP_TITLE_DEFAULT);
        defaultProperties.put(APP_DESCRIPTION, APP_DESCRIPTION_DEFAULT);
        defaultProperties.put(APP_BANNER_DESCRIPTION, "");
        defaultProperties.put(APP_BANNER_COLOUR, "");
        defaultProperties.put(APP_DOCUMENTATION_URL, APP_DOCUMENTATION_URL_DEFAULT);
        defaultProperties.put(LOGO_LINK, LOGO_LINK_DEFAULT);
        defaultProperties.put(LOGO_IMAGE_URL, LOGO_IMAGE_URL_DEFAULT);
        defaultProperties.put(FAVICON_SMALL_URL, LOGO_IMAGE_URL_DEFAULT);
        defaultProperties.put(FAVICON_LARGE_URL, LOGO_IMAGE_URL_DEFAULT);
        defaultProperties.put(GAFFER_VERSION, GAFFER_VERSION_DEFAULT);
        defaultProperties.put(KORYPHE_VERSION, KORYPHE_VERSION_DEFAULT);

        // Then
        assertEquals(defaultProperties, propertiesController.getProperties());
    }

    @Test
    public void shouldReturnDifferentPropertiesIfSet() {
        // Given
        PropertiesController propertiesController = new PropertiesController();
        System.setProperty(APP_TITLE, "test");
        System.setProperty(APP_DESCRIPTION, "A Test for properties");

        // When
        final Map<String, String> updatedProperties = new LinkedHashMap<>();
        updatedProperties.put(APP_TITLE, "test");
        updatedProperties.put(APP_DESCRIPTION, "A Test for properties");
        updatedProperties.put(APP_BANNER_DESCRIPTION, "");
        updatedProperties.put(APP_BANNER_COLOUR, "");
        updatedProperties.put(APP_DOCUMENTATION_URL, APP_DOCUMENTATION_URL_DEFAULT);
        updatedProperties.put(LOGO_LINK, LOGO_LINK_DEFAULT);
        updatedProperties.put(LOGO_IMAGE_URL, LOGO_IMAGE_URL_DEFAULT);
        updatedProperties.put(FAVICON_SMALL_URL, LOGO_IMAGE_URL_DEFAULT);
        updatedProperties.put(FAVICON_LARGE_URL, LOGO_IMAGE_URL_DEFAULT);
        updatedProperties.put(GAFFER_VERSION, GAFFER_VERSION_DEFAULT);
        updatedProperties.put(KORYPHE_VERSION, KORYPHE_VERSION_DEFAULT);

        // Then
        assertEquals(updatedProperties, propertiesController.getProperties());
    }

    @Test
    public void shouldThrowExceptionIfPropertyDoesNotExist() {
        // Given
        PropertiesController propertiesController = new PropertiesController();

        // When / Then
        assertThatExceptionOfType(GafferRuntimeException.class)
                .isThrownBy(() -> propertiesController.getProperty("made.up.property"))
                .withMessage("Property: made.up.property could not be found.")
                .extracting(GafferRuntimeException::getStatus)
                .isEqualTo(NOT_FOUND);
    }

    @Test
    public void shouldReturnDefaultIfPropertyExistsButNotUpdated() {
        // Given
        PropertiesController propertiesController = new PropertiesController();

        // When
        String property = propertiesController.getProperty(APP_TITLE);

        // Then
        assertEquals("Gaffer REST", property);
    }

    @Test
    public void shouldReturnUpdatedValueIfPropertyExistsAndHasBeenSet() {
        // Given
        PropertiesController propertiesController = new PropertiesController();
        System.setProperty(APP_TITLE, "myTitle");

        // When
        String property = propertiesController.getProperty(APP_TITLE);

        // Then
        assertEquals("myTitle", property);
    }
}
