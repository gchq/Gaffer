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

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Lists;
import org.junit.Test;
import uk.gov.gchq.koryphe.util.JsonSerialiser;
import java.io.IOException;
import java.util.function.Predicate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class OrTest {
    private final Predicate<Double> mockTrue = new MockPredicateTrue();
    private final Predicate<Double> mockFalse = new MockPredicateFalse();

    private final static double DOUBLE = 0.0d;
    
    @Test
    public void shouldReturnTrueForAllTruePredicates() {
        // Given
        final Or predicate = new Or<>(mockTrue, mockTrue);

        // When
        boolean valid = predicate.test(DOUBLE);

        // Then
        assertTrue(valid);
    }

    @Test
    public void shouldReturnTrueForTrueAndFalsePredicates() {
        // Given
        final Or predicate = new Or<>(mockTrue, mockFalse);

        // When
        boolean valid = predicate.test(DOUBLE);

        // Then
        assertTrue(valid);
    }

    @Test
    public void shouldReturnFalseForAllFalsePredicates() {
        // Given
        final Or predicate = new Or<>(mockFalse, mockFalse);

        // When
        boolean valid = predicate.test(DOUBLE);

        // Then
        assertFalse(valid);
    }

    @Test
    public void shouldReturnTrueForAllTruePredicates_asList() {
        // Given
        final Or predicate = new Or<>(Lists.newArrayList(mockTrue, mockTrue));

        // When
        boolean valid = predicate.test(DOUBLE);

        // Then
        assertTrue(valid);
    }

    @Test
    public void shouldReturnTrueForTrueAndFalsePredicates_asList() {
        // Given
        final Or predicate = new Or<>(Lists.newArrayList(mockTrue, mockFalse));

        // When
        boolean valid = predicate.test(DOUBLE);

        // Then
        assertTrue(valid);
    }

    @Test
    public void shouldReturnFalseForAllFalsePredicates_asList() {
        // Given
        final Or predicate = new Or<>(Lists.newArrayList(mockFalse, mockFalse));

        // When
        boolean valid = predicate.test(DOUBLE);

        // Then
        assertFalse(valid);
    }

    @Test
    public void shouldJsonSerialiseAndDeserialise() throws IOException {
        // Given
        final Or or = new Or<>(mockTrue, mockFalse);

        // When
        final String json = JsonSerialiser.serialise(or);

        // Then
        assertEquals("{\"class\":\"uk.gov.gchq.koryphe.predicate.Or\"," +
                "\"functions\":[{\"class\":\"uk.gov.gchq.koryphe.predicate.MockPredicateTrue\"}," +
                "{\"class\":\"uk.gov.gchq.koryphe.predicate.MockPredicateFalse\"}]}", json);

        // When 2
        final Or deserialisedFilter = JsonSerialiser.deserialise(json, new TypeReference<Or<Object>>() {});

        // Then 2
        assertNotNull(deserialisedFilter);
    }
}
