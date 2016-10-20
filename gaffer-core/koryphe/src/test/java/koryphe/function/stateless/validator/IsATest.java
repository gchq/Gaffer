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

package koryphe.function.stateless.validator;

import org.junit.Test;
import util.JsonSerialiser;

import java.io.IOException;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class IsATest {
    @Test
    public void shouldAcceptTheValueWhenSameClass() {
        // Given
        final IsA validator = new IsA(String.class);

        // When
        boolean valid = validator.execute("Test");

        // Then
        assertTrue(valid);
    }

    @Test
    public void shouldRejectTheValueWhenDifferentClasses() {
        // Given
        final IsA validator = new IsA(String.class);

        // When
        boolean valid = validator.execute(5);

        // Then
        assertFalse(valid);
    }

    @Test
    public void shouldAcceptTheValueWhenSuperClass() {
        // Given
        final IsA validator = new IsA(Number.class);

        // When
        boolean valid = validator.execute(5);

        // Then
        assertTrue(valid);
    }

    @Test
    public void shouldAcceptTheValueWhenNullValue() {
        // Given
        final IsA validator = new IsA(String.class);

        // When
        boolean valid = validator.execute(null);

        // Then
        assertTrue(valid);
    }

    @Test
    public void shouldJsonSerialiseAndDeserialise() throws IOException {
        // Given
        final Class<Integer> type = Integer.class;
        final IsA filter = new IsA(type);

        // When
        final String json = JsonSerialiser.serialise(filter);

        String isAName = IsA.class.getCanonicalName();
        String typeName = type.getCanonicalName();

        // Then
        assertEquals("{\"class\":\"" + isAName + "\",\"type\":\"" + typeName + "\"}", json);

        // When 2
        final IsA deserialisedFilter = JsonSerialiser.deserialise(json, IsA.class);

        // Then 2
        assertNotNull(deserialisedFilter);
        assertEquals(type.getName(), deserialisedFilter.getType());
    }
}
