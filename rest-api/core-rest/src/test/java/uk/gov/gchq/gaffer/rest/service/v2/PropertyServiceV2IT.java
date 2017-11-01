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

import org.junit.Test;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class PropertyServiceV2IT extends AbstractRestApiV2IT {

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
        System.setProperty("gaffer.test2", "3");

        // When
        final Response response = getClient().getProperty("UNKNOWN");

        //Then
        assertEquals(404, response.getStatus());
    }

    @Test
    public void shouldGetAllProperties() throws IOException {
        //Given
        System.setProperty("gaffer.properties", "gaffer.test1,gaffer.test2");
        System.setProperty("gaffer.test1", "1");
        System.setProperty("gaffer.test2", "3");
        // When
        final Response response = getClient().getProperties();

        //Then
        assertEquals(200, response.getStatus());
        Map<String, Object> properties = response.readEntity(Map.class);
        assertEquals(2, properties.size());
        assertEquals("1", properties.get("gaffer.test1"));
        assertEquals("3", properties.get("gaffer.test2"));
    }

    @Test
    public void shouldGetKnownProperty() throws IOException {
        //Given
        System.setProperty("gaffer.properties", "gaffer.test1,gaffer.test2");
        System.setProperty("gaffer.test1", "1");
        System.setProperty("gaffer.test2", "3");
        // When
        final Response response = getClient().getProperty("gaffer.test1");

        //Then
        assertEquals(200, response.getStatus());
        String property = response.readEntity(String.class);
        assertEquals("1", property);
    }
}
