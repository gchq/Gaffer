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

package uk.gov.gchq.koryphe.bifunction;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import java.io.IOException;
import java.util.function.BiFunction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public abstract class BiFunctionTest {
    private static final ObjectMapper MAPPER = createObjectMapper();

    private static ObjectMapper createObjectMapper() {
        final ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);
        return mapper;
    }

    protected abstract BiFunction getInstance();

    protected abstract Class<? extends BiFunction> getFunctionClass();

    @Test
    public abstract void shouldJsonSerialiseAndDeserialise() throws IOException;

    protected String serialise(Object object) throws IOException {
        return MAPPER.writeValueAsString(object);
    }

    protected BiFunction deserialise(String json) throws IOException {
        return MAPPER.readValue(json, getFunctionClass());
    }

    @Test
    public void shouldEquals() {
        // Given
        final BiFunction instance = getInstance();

        // When
        final BiFunction other = getInstance();

        // Then
        assertEquals(instance, other);
        assertEquals(instance.hashCode(), other.hashCode());
    }

    @Test
    public void shouldEqualsWhenSameObject() {
        // Given
        final BiFunction instance = getInstance();

        // Then
        assertEquals(instance, instance);
        assertEquals(instance.hashCode(), instance.hashCode());
    }

    @Test
    public void shouldNotEqualsWhenDifferentClass() {
        // Given
        final BiFunction instance = getInstance();

        // When
        final Object other = new Object();

        // Then
        assertNotEquals(instance, other);
        assertNotEquals(instance.hashCode(), other.hashCode());
    }

    @Test
    public void shouldNotEqualsNull() {
        // Given
        final BiFunction instance = getInstance();

        // Then
        assertNotEquals(instance, null);
    }
}
