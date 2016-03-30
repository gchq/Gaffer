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

package gaffer.function2.global;

import gaffer.function2.FunctionTest;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class IsATest extends FunctionTest {
    @Test
    public void shouldAcceptTheValueWhenSameClass() {
        // Given
        final IsA validator = new IsA(String.class);

        // When
        boolean valid = validator.validate("Test");

        // Then
        assertTrue(valid);
    }

    @Test
    public void shouldRejectTheValueWhenDifferentClasses() {
        // Given
        final IsA validator = new IsA(String.class);

        // When
        boolean valid = validator.validate(5);

        // Then
        assertFalse(valid);
    }

    @Test
    public void shouldAcceptTheValueWhenSuperClass() {
        // Given
        final IsA validator = new IsA(Number.class);

        // When
        boolean valid = validator.validate(5);

        // Then
        assertTrue(valid);
    }

    @Test
    public void shouldAcceptTheValueWhenNullValue() {
        // Given
        final IsA validator = new IsA(String.class);

        // When
        boolean valid = validator.validate(null);

        // Then
        assertTrue(valid);
    }

    @Test
    public void shouldCopy() {
        // Given
        final IsA validator = new IsA(String.class);

        // When
        final IsA validatorCopy = validator.copy();

        // Then
        assertNotSame(validator, validatorCopy);
        assertEquals(String.class.getName(), validatorCopy.getType());
    }

    @Test
    public void shouldJsonSerialiseAndDeserialise() throws IOException {
        // Given
        final Class<Integer> type = Integer.class;
        final IsA filter = new IsA(type);

        // When
        final String json = serialise(filter);

        // Then
        assertEquals("{\"class\":\"gaffer.function2.global.IsA\",\"type\":\"java.lang.Integer\"}", json);

        // When 2
        final IsA deserialisedFilter = deserialise(json, IsA.class);

        // Then 2
        assertNotNull(deserialisedFilter);
        assertEquals(type.getName(), deserialisedFilter.getType());
    }
}
