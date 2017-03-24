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
import org.junit.Test;
import uk.gov.gchq.koryphe.util.JsonSerialiser;
import java.io.IOException;
import java.util.function.Predicate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.anyDouble;

public class NotTest {
    private final Predicate truePredicate = new MockPredicateTrue();
    private final Predicate falsePredicate = new MockPredicateFalse();

    private final static double DOUBLE = 0.0d;

    @Test
    public void shouldNegate() {
        // Given
        final Not<Double> not = new Not(truePredicate);

        // When
        boolean valid = not.test(DOUBLE);

        // Then
        assertFalse(valid);
    }

    @Test
    public void shouldJsonSerialiseAndDeserialise() throws IOException {
        // Given
        final Not filter = new Not<>(truePredicate);

        // When
        final String json = JsonSerialiser.serialise(filter);

        // Then
        assertEquals("{\"class\":\"uk.gov.gchq.koryphe.predicate.Not\",\"predicate\":{\"class\":\"uk.gov.gchq.koryphe.predicate.MockPredicateTrue\"}}", json);

        // When 2
        final Not deserialisedFilter = JsonSerialiser.deserialise(json, new TypeReference<Not<Object>>() {});

        // Then 2
        assertNotNull(deserialisedFilter);
    }
}
