/*
 * Copyright 2017-2020 Crown Copyright
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

package uk.gov.gchq.gaffer.rest.service.v2;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.rest.SystemProperty;

import javax.ws.rs.core.Response;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class PropertyServiceV2IT extends AbstractRestApiV2IT {

    @BeforeEach
    @AfterEach
    public void cleanUp() {
        System.clearProperty("gaffer.properties");
        System.clearProperty("gaffer.test1");
        System.clearProperty("gaffer.test2");
        System.clearProperty("gaffer.test3");
        System.clearProperty(SystemProperty.APP_TITLE);
    }

    @Test
    public void shouldThrowErrorOnUnknownPropertyWhenNoneSet() {
        // When
        final Response response = client.getProperty("UNKNOWN");

        //Then
        assertEquals(404, response.getStatus());
    }

    @Test
    public void shouldThrowErrorOnUnknownProperty() {
        //Given
        System.setProperty("gaffer.properties", "gaffer.test1,gaffer.test2");
        System.setProperty("gaffer.test1", "1");
        System.setProperty("gaffer.test2", "2");

        // When
        final Response response = client.getProperty("UNKNOWN");

        //Then
        assertEquals(404, response.getStatus());
    }

    @Test
    public void shouldThrowErrorOnPropertyThatIsNotExposed() {
        //Given
        System.setProperty("gaffer.properties", "gaffer.test1,gaffer.test2");
        System.setProperty("gaffer.test1", "1");
        System.setProperty("gaffer.test2", "2");
        System.setProperty("gaffer.test3", "3");

        // When
        final Response response = client.getProperty("gaffer.test3");

        //Then
        assertEquals(404, response.getStatus());
    }

    @Test
    public void shouldGetAllProperties() {
        //Given
        System.setProperty("gaffer.properties", "gaffer.test1,gaffer.test2");
        System.setProperty("gaffer.test1", "1");
        System.setProperty("gaffer.test2", "2");
        System.setProperty("gaffer.test3", "3");
        System.setProperty(SystemProperty.APP_TITLE, "newTitle");

        // When
        final Response response = client.getProperties();

        //Then
        assertEquals(200, response.getStatus());
        Map<String, Object> properties = response.readEntity(Map.class);

        final LinkedHashMap<String, String> expectedProperties = new LinkedHashMap<>(PropertiesServiceV2.CORE_EXPOSED_PROPERTIES);
        expectedProperties.put("gaffer.test1", "1");
        expectedProperties.put("gaffer.test2", "2");
        expectedProperties.put(SystemProperty.APP_TITLE, "newTitle");
        assertEquals(expectedProperties, properties);
    }

    @Test
    public void shouldGetAllPropertiesWhenNoCustomPropertiesCsvDefined() {
        //Given
        System.setProperty("gaffer.test1", "1");
        System.setProperty("gaffer.test2", "2");
        System.setProperty("gaffer.test3", "3");
        System.setProperty(SystemProperty.APP_TITLE, "newTitle");

        // When
        final Response response = client.getProperties();

        //Then
        assertEquals(200, response.getStatus());
        Map<String, Object> properties = response.readEntity(Map.class);

        final LinkedHashMap<String, String> expectedProperties = new LinkedHashMap<>(PropertiesServiceV2.CORE_EXPOSED_PROPERTIES);
        expectedProperties.put(SystemProperty.APP_TITLE, "newTitle");
        assertEquals(expectedProperties, properties);
    }

    @Test
    public void shouldGetKnownProperty() {
        //Given
        System.setProperty("gaffer.properties", "gaffer.test1,gaffer.test2");
        System.setProperty("gaffer.test1", "1");
        System.setProperty("gaffer.test2", "2");

        // When
        final Response response = client.getProperty("gaffer.test1");

        //Then
        assertEquals(200, response.getStatus());
        String property = response.readEntity(String.class);
        assertEquals("1", property);
    }

    @Test
    public void shouldGetKnownCoreProperty() {
        //Given
        System.setProperty("gaffer.properties", "gaffer.test1,gaffer.test2");
        System.setProperty("gaffer.test1", "1");
        System.setProperty("gaffer.test2", "2");

        // When
        final Response response = client.getProperty(SystemProperty.APP_TITLE);

        //Then
        assertEquals(200, response.getStatus());
        String property = response.readEntity(String.class);
        assertEquals(SystemProperty.APP_TITLE_DEFAULT, property);
    }

    @Test
    public void shouldGetOverriddenKnownCoreProperty() {
        //Given
        System.setProperty("gaffer.properties", "gaffer.test1,gaffer.test2");
        System.setProperty("gaffer.test1", "1");
        System.setProperty("gaffer.test2", "2");
        System.setProperty(SystemProperty.APP_TITLE, "newTitle");

        // When
        final Response response = client.getProperty(SystemProperty.APP_TITLE);

        //Then
        assertEquals(200, response.getStatus());
        String property = response.readEntity(String.class);
        assertEquals("newTitle", property);
    }

    @Test
    public void shouldGetKorypheVersion() {
        // When
        final Response response = client.getProperty(SystemProperty.KORYPHE_VERSION);

        final String propertyValue = response.readEntity(String.class);

        assertNotNull(propertyValue);
    }

    @Test
    public void shouldGetGafferVersion() {
        // When
        final Response response = client.getProperty(SystemProperty.GAFFER_VERSION);

        final String propertyValue = response.readEntity(String.class);

        assertNotNull(propertyValue);
    }
}
