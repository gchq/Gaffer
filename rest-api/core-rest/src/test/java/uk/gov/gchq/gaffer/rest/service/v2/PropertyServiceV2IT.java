package uk.gov.gchq.gaffer.rest.service.v2;
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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.rest.SystemProperty;

import javax.ws.rs.core.Response;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class PropertyServiceV2IT extends AbstractRestApiV2IT {

    @Before
    @After
    public void cleanUp() {
        System.clearProperty("gaffer.properties");
        System.clearProperty("gaffer.test1");
        System.clearProperty("gaffer.test2");
        System.clearProperty("gaffer.test3");
        System.clearProperty(SystemProperty.APP_TITLE);
    }

    @Test
    public void shouldThrowErrorOnUnknownPropertyWhenNoneSet() throws IOException {
        //Given

        // When
        final Response response = getClient().getProperty("UNKNOWN");

        //Then
        assertEquals(404, response.getStatus());
    }

    @Test
    public void shouldThrowErrorOnUnknownProperty() throws IOException {
        //Given
        System.setProperty("gaffer.properties", "gaffer.test1,gaffer.test2");
        System.setProperty("gaffer.test1", "1");
        System.setProperty("gaffer.test2", "2");

        // When
        final Response response = getClient().getProperty("UNKNOWN");

        //Then
        assertEquals(404, response.getStatus());
    }

    @Test
    public void shouldThrowErrorOnPropertyThatIsNotExposed() throws IOException {
        //Given
        System.setProperty("gaffer.properties", "gaffer.test1,gaffer.test2");
        System.setProperty("gaffer.test1", "1");
        System.setProperty("gaffer.test2", "2");
        System.setProperty("gaffer.test3", "3");

        // When
        final Response response = getClient().getProperty("gaffer.test3");

        //Then
        assertEquals(404, response.getStatus());
    }

    @Test
    public void shouldGetAllProperties() throws IOException {
        //Given
        System.setProperty("gaffer.properties", "gaffer.test1,gaffer.test2");
        System.setProperty("gaffer.test1", "1");
        System.setProperty("gaffer.test2", "2");
        System.setProperty("gaffer.test3", "3");
        System.setProperty(SystemProperty.APP_TITLE, "newTitle");

        // When
        final Response response = getClient().getProperties();

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
    public void shouldGetAllPropertiesWhenNoCustomPropertiesCsvDefined() throws IOException {
        //Given
        System.setProperty("gaffer.test1", "1");
        System.setProperty("gaffer.test2", "2");
        System.setProperty("gaffer.test3", "3");
        System.setProperty(SystemProperty.APP_TITLE, "newTitle");

        // When
        final Response response = getClient().getProperties();

        //Then
        assertEquals(200, response.getStatus());
        Map<String, Object> properties = response.readEntity(Map.class);

        final LinkedHashMap<String, String> expectedProperties = new LinkedHashMap<>(PropertiesServiceV2.CORE_EXPOSED_PROPERTIES);
        expectedProperties.put(SystemProperty.APP_TITLE, "newTitle");
        assertEquals(expectedProperties, properties);
    }

    @Test
    public void shouldGetKnownProperty() throws IOException {
        //Given
        System.setProperty("gaffer.properties", "gaffer.test1,gaffer.test2");
        System.setProperty("gaffer.test1", "1");
        System.setProperty("gaffer.test2", "2");

        // When
        final Response response = getClient().getProperty("gaffer.test1");

        //Then
        assertEquals(200, response.getStatus());
        String property = response.readEntity(String.class);
        assertEquals("1", property);
    }

    @Test
    public void shouldGetKnownCoreProperty() throws IOException {
        //Given
        System.setProperty("gaffer.properties", "gaffer.test1,gaffer.test2");
        System.setProperty("gaffer.test1", "1");
        System.setProperty("gaffer.test2", "2");

        // When
        final Response response = getClient().getProperty(SystemProperty.APP_TITLE);

        //Then
        assertEquals(200, response.getStatus());
        String property = response.readEntity(String.class);
        assertEquals(SystemProperty.APP_TITLE_DEFAULT, property);
    }

    @Test
    public void shouldGetOverriddenKnownCoreProperty() throws IOException {
        //Given
        System.setProperty("gaffer.properties", "gaffer.test1,gaffer.test2");
        System.setProperty("gaffer.test1", "1");
        System.setProperty("gaffer.test2", "2");
        System.setProperty(SystemProperty.APP_TITLE, "newTitle");

        // When
        final Response response = getClient().getProperty(SystemProperty.APP_TITLE);

        //Then
        assertEquals(200, response.getStatus());
        String property = response.readEntity(String.class);
        assertEquals("newTitle", property);
    }
}
