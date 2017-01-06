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
import uk.gov.gchq.gaffer.function.FilterFunction;
import uk.gov.gchq.gaffer.function.FilterFunctionTest;
import uk.gov.gchq.gaffer.function.IsA;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class NotTest extends FilterFunctionTest {
    @Test
    public void shouldAcceptTheValueWhenTheWrappedFunctionReturnsFalse() {
        // Given
        final FilterFunction function = mock(FilterFunction.class);
        final Not filter = new Not(function);
        final Object[] input = new Object[]{"some value"};
        given(function.isValid(input)).willReturn(false);

        // When
        boolean accepted = filter.isValid(input);

        // Then
        assertTrue(accepted);
        verify(function).isValid(input);
    }

    @Test
    public void shouldAcceptTheValueWhenTheWrappedFunctionReturnsTrue() {
        // Given
        final FilterFunction function = mock(FilterFunction.class);
        final Not filter = new Not(function);
        final Object[] input = new Object[]{"some value"};
        given(function.isValid(input)).willReturn(true);

        // When
        boolean accepted = filter.isValid(input);

        // Then
        assertFalse(accepted);
        verify(function).isValid(input);
    }

    @Test
    public void shouldRejectTheValueWhenNullFunction() {
        // Given
        final Not filter = new Not();
        final Object[] input = new Object[]{"some value"};

        // When
        boolean accepted = filter.isValid(input);

        // Then
        assertTrue(accepted);
    }

    @Test
    public void shouldClone() {
        // Given
        final FilterFunction function = mock(FilterFunction.class);
        final FilterFunction clonedFunction = mock(FilterFunction.class);
        final Not filter = new Not(function);
        given(function.statelessClone()).willReturn(clonedFunction);

        // When
        final Not clonedFilter = filter.statelessClone();

        // Then
        assertNotSame(filter, clonedFilter);
        assertSame(clonedFunction, clonedFilter.getFunction());
    }

    @Test
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final IsA isA = new IsA(String.class);
        final Not filter = new Not(isA);

        // When
        final String json = new String(new JSONSerialiser().serialise(filter, true));

        // Then
        JsonUtil.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.function.filter.Not\",%n" +
                "  \"function\" : {%n" +
                "    \"class\" : \"uk.gov.gchq.gaffer.function.IsA\",%n" +
                "    \"type\" : \"java.lang.String\"%n" +
                "  }%n" +
                "}"), json);

        // When 2
        final Not deserialisedFilter = new JSONSerialiser().deserialise(json.getBytes(), Not.class);

        // Then 2
        assertNotNull(deserialisedFilter);
        assertEquals(String.class.getName(), ((IsA) deserialisedFilter.getFunction()).getType());
    }

    @Override
    protected Class<Not> getFunctionClass() {
        return Not.class;
    }

    @Override
    protected Not getInstance() {
        return new Not(new IsA(String.class));
    }
}
