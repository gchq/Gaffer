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
package uk.gov.gchq.gaffer.function.filter;

import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.JsonUtil;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.function.IsA;
import uk.gov.gchq.gaffer.function.PredicateTest;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import java.util.function.Predicate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class NotTest extends PredicateTest {
    @Test
    public void shouldAcceptTheValueWhenTheWrappedFunctionReturnsFalse() {
        // Given
        final Predicate<String> function = mock(Predicate.class);
        final Not<String> filter = new Not<>(function);
        given(function.test("some value")).willReturn(false);

        // When
        boolean accepted = filter.test("some value");

        // Then
        assertTrue(accepted);
        verify(function).test("some value");
    }

    @Test
    public void shouldRejectTheValueWhenTheWrappedFunctionReturnsTrue() {
        // Given
        final Predicate<String> function = mock(Predicate.class);
        final Not<String> filter = new Not<>(function);
        given(function.test("some value")).willReturn(true);

        // When
        boolean accepted = filter.test("some value");

        // Then
        assertFalse(accepted);
        verify(function).test("some value");
    }

    @Test
    public void shouldRejectTheValueWhenNullFunction() {
        // Given
        final Not<String> filter = new Not<>();

        // When
        boolean accepted = filter.test("some value");

        // Then
        assertFalse(accepted);
    }

    @Test
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final IsA isA = new IsA(String.class);
        final Not<Object> filter = new Not<>(isA);

        // When
        final String json = new String(new JSONSerialiser().serialise(filter, true));

        // Then
        JsonUtil.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.function.filter.Not\",%n" +
                "  \"predicate\" : {%n" +
                "    \"class\" : \"uk.gov.gchq.gaffer.function.IsA\",%n" +
                "    \"type\" : \"java.lang.String\"%n" +
                "  }%n" +
                "}"), json);

        // When 2
        final Not deserialisedFilter = new JSONSerialiser().deserialise(json.getBytes(), Not.class);

        // Then 2
        assertNotNull(deserialisedFilter);
        assertEquals(String.class.getName(), ((IsA) deserialisedFilter.getPredicate()).getType());
    }

    @Override
    protected Class<Not> getPredicateClass() {
        return Not.class;
    }

    @Override
    protected Not<Object> getInstance() {
        return new Not<>(new IsA(String.class));
    }
}
