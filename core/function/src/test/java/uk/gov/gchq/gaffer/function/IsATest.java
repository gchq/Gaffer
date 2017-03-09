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

package uk.gov.gchq.gaffer.function;

import org.junit.Test;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class IsATest extends PredicateTest {
    @Test
    public void shouldAcceptTheValueWhenSameClass() {
        // Given
        final IsA filter = new IsA(String.class);

        // When
        boolean accepted = filter.test(new String[]{"Test"});

        // Then
        assertTrue(accepted);
    }

    @Test
    public void shouldRejectTheValueWhenDifferentClasses() {
        // Given
        final IsA filter = new IsA(String.class);

        // When
        boolean accepted = filter.test(new Integer[]{5});

        // Then
        assertFalse(accepted);
    }

    @Test
    public void shouldAcceptTheValueWhenSuperClass() {
        // Given
        final IsA filter = new IsA(Number.class);

        // When
        boolean accepted = filter.test(new Integer[]{5});

        // Then
        assertTrue(accepted);
    }

    @Test
    public void shouldAcceptTheValueWhenNullValue() {
        // Given
        final IsA filter = new IsA(String.class);

        // When
        boolean accepted = filter.test(new Object[]{null});

        // Then
        assertTrue(accepted);
    }

    @Test
    public void shouldJsonSerialiseAndDeserialise() throws IOException {
        // Given
        final Class<Integer> type = Integer.class;
        final IsA filter = new IsA(type);

        // When
        final String json = serialise(filter);

        // Then
        assertEquals("{\"class\":\"uk.gov.gchq.gaffer.function.IsA\",\"type\":\"java.lang.Integer\"}", json);

        // When 2
        final IsA deserialisedFilter = (IsA) deserialise(json);

        // Then 2
        assertNotNull(deserialisedFilter);
        assertEquals(type.getName(), deserialisedFilter.getType());
    }


    @Override
    protected IsA getInstance() {
        return new IsA(String.class);
    }

    @Override
    protected Class<IsA> getPredicateClass() {
        return IsA.class;
    }
}
