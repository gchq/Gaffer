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

package uk.gov.gchq.koryphe.predicate;

import org.junit.Test;
import uk.gov.gchq.koryphe.util.JsonSerialiser;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class IsATest {
    @Test
    public void shouldAcceptTheValueWhenSameClass() {
        // Given
        final IsA predicate = new IsA(String.class);

        // When
        boolean valid = predicate.test("Test");

        // Then
        assertTrue(valid);
    }

    @Test
    public void shouldRejectTheValueWhenDifferentClasses() {
        // Given
        final IsA predicate = new IsA(String.class);

        // When
        boolean valid = predicate.test(5);

        // Then
        assertFalse(valid);
    }

    @Test
    public void shouldAcceptTheValueWhenSuperClass() {
        // Given
        final IsA predicate = new IsA(Number.class);

        // When
        boolean valid = predicate.test(5);

        // Then
        assertTrue(valid);
    }

    @Test
    public void shouldAcceptTheValueWhenNullValue() {
        // Given
        final IsA predicate = new IsA(String.class);

        // When
        boolean valid = predicate.test(null);

        // Then
        assertTrue(valid);
    }

    @Test
    public void shouldCreateIsAFromClassName() {
        // Given
        final String type = "java.lang.String";

        // When
        final IsA predicate = new IsA(type);

        // Then
        assertNotNull(predicate);
        assertEquals(predicate.getType(), type);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionIfInvalidClassNameProvided() {
        // Given
        final String type = "java.util.String";

        // When
        final IsA predicate = new IsA(type);
    }

    @Test
    public void shouldJsonSerialiseAndDeserialise() throws IOException {
        // Given
        final Class<Integer> type = Integer.class;
        final IsA filter = new IsA(type);

        // When
        final String json = JsonSerialiser.serialise(filter);

        // Then
        assertEquals("{\"class\":\"uk.gov.gchq.koryphe.predicate.IsA\",\"type\":\"java.lang.Integer\"}", json);

        // When 2
        final IsA deserialisedFilter = JsonSerialiser.deserialise(json, IsA.class);

        // Then 2
        assertNotNull(deserialisedFilter);
        assertEquals(type.getName(), deserialisedFilter.getType());
    }
}
