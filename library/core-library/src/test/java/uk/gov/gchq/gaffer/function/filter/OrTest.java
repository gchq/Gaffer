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
import org.mockito.Mockito;
import uk.gov.gchq.gaffer.commonutil.JsonUtil;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.function.ArrayTuple;
import uk.gov.gchq.gaffer.function.FilterFunction;
import uk.gov.gchq.gaffer.function.FilterFunctionTest;
import uk.gov.gchq.gaffer.function.context.ConsumerFunctionContext;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class OrTest extends FilterFunctionTest {

    @Test
    public void shouldAcceptWhenOneFunctionsAccepts() {
        // Given
        final String test = "test";
        final String test1a = "test1a";
        final String test1b = "test1b";
        final String test2a = "test2a";

        final ConsumerFunctionContext<Integer, FilterFunction> funcContext1 = mock(ConsumerFunctionContext.class);
        final ConsumerFunctionContext<Integer, FilterFunction> funcContext2 = mock(ConsumerFunctionContext.class);
        final FilterFunction func1 = mock(FilterFunction.class);
        final FilterFunction func2 = mock(FilterFunction.class);
        final Or or = new Or(Arrays.asList(funcContext1, funcContext2));

        given(funcContext1.getFunction()).willReturn(func1);
        given(funcContext2.getFunction()).willReturn(func2);

        given(funcContext1.select(Mockito.any(ArrayTuple.class))).willReturn(new Object[]{test, test1a, test1b});
        given(funcContext2.select(Mockito.any(ArrayTuple.class))).willReturn(new Object[]{test, test2a});

        given(func1.isValid(new String[]{test, test1a, test1b})).willReturn(true);
        given(func2.isValid(new String[]{test, test2a})).willReturn(false);

        // When
        boolean accepted = or.isValid(new String[]{test, test1a, test2a, test1b});

        // Then
        assertTrue(accepted);
        verify(func1).isValid(new String[]{test, test1a, test1b});
        verify(func2, never()).isValid(new String[]{test, test2a});
    }

    @Test
    public void shouldAcceptWhenNoFunctions() {
        // Given
        final Or or = new Or();

        // When
        boolean accepted = or.isValid(new String[]{"test"});

        // Then
        assertTrue(accepted);
    }

    @Test
    public void shouldAcceptWhenNoFunctionsOrNullInput() {
        // Given
        final Or or = new Or();

        // When
        boolean accepted = or.isValid(null);

        // Then
        assertTrue(accepted);
    }


    @Test
    public void shouldRejectWhenAllFunctionsReject() {
        // Given
        final String test = "test";
        final String test1a = "test1a";
        final String test1b = "test1b";
        final String test2a = "test2a";

        final ConsumerFunctionContext<Integer, FilterFunction> funcContext1 = mock(ConsumerFunctionContext.class);
        final ConsumerFunctionContext<Integer, FilterFunction> funcContext2 = mock(ConsumerFunctionContext.class);
        final ConsumerFunctionContext<Integer, FilterFunction> funcContext3 = mock(ConsumerFunctionContext.class);

        final FilterFunction func1 = mock(FilterFunction.class);
        final FilterFunction func2 = mock(FilterFunction.class);
        final FilterFunction func3 = mock(FilterFunction.class);
        final Or or = new Or(Arrays.asList(funcContext1, funcContext2, funcContext3));

        given(funcContext1.getFunction()).willReturn(func1);
        given(funcContext2.getFunction()).willReturn(func2);
        given(funcContext3.getFunction()).willReturn(func3);

        given(funcContext1.select(Mockito.any(ArrayTuple.class))).willReturn(new Object[]{test, test1a, test1b});
        given(funcContext2.select(Mockito.any(ArrayTuple.class))).willReturn(new Object[]{test, test2a});
        given(funcContext3.select(Mockito.any(ArrayTuple.class))).willReturn(new Object[]{test});

        given(func1.isValid(new String[]{test, test1a, test1b})).willReturn(false);
        given(func2.isValid(new String[]{test, test2a})).willReturn(false);
        given(func3.isValid(new String[]{test})).willReturn(false);

        // When
        boolean accepted = or.isValid(new String[]{test, test1a, test2a, test1b});

        // Then
        assertFalse(accepted);
        verify(func1).isValid(new String[]{test, test1a, test1b});
        verify(func2).isValid(new String[]{test, test2a});
        verify(func3, never()).isValid(new String[]{test, test2a});
    }

    @Test
    public void shouldClone() {
        // Given
        final ConsumerFunctionContext<Integer, FilterFunction> funcContext1 = mock(ConsumerFunctionContext.class);
        final ConsumerFunctionContext<Integer, FilterFunction> funcContext2 = mock(ConsumerFunctionContext.class);
        final FilterFunction func1 = mock(FilterFunction.class);
        final FilterFunction func2 = mock(FilterFunction.class);
        final FilterFunction func1Cloned = mock(FilterFunction.class);
        final FilterFunction func2Cloned = mock(FilterFunction.class);
        final Or or = new Or(Arrays.asList(funcContext1, funcContext2));

        given(funcContext1.getFunction()).willReturn(func1);
        given(funcContext2.getFunction()).willReturn(func2);

        given(funcContext1.getSelection()).willReturn(Arrays.asList(0, 1, 3));
        given(funcContext2.getSelection()).willReturn(Arrays.asList(0, 2));

        given(func1.statelessClone()).willReturn(func1Cloned);
        given(func2.statelessClone()).willReturn(func2Cloned);

        // When
        final Or clonedOr = or.statelessClone();

        // Then
        assertNotSame(or, clonedOr);
        assertNotNull(clonedOr);
        assertEquals(2, clonedOr.getFunctions().size());
        assertSame(func1Cloned, clonedOr.getFunctions().get(0).getFunction());
        assertArrayEquals(new Integer[]{0, 1, 3}, clonedOr.getFunctions().get(0).getSelection().toArray());
        assertSame(func2Cloned, clonedOr.getFunctions().get(1).getFunction());
        assertArrayEquals(new Integer[]{0, 2}, clonedOr.getFunctions().get(1).getSelection().toArray());
    }

    @Test
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final Or filter = new Or(Collections.singletonList(new ConsumerFunctionContext<Integer, FilterFunction>(new Or(), Arrays.asList(0, 1, 2))));

        // When
        final String json = new String(new JSONSerialiser().serialise(filter, true));

        // Then
        JsonUtil.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.function.filter.Or\",%n" +
                "  \"functions\" : [ {%n" +
                "    \"function\" : {%n" +
                "      \"class\" : \"uk.gov.gchq.gaffer.function.filter.Or\",%n" +
                "      \"functions\" : [ ]%n" +
                "    },%n" +
                "    \"selection\" : [ 0, 1, 2 ]%n" +
                "  } ]%n" +
                "}"), json);

        // When 2
        final Or deserialisedFilter = new JSONSerialiser().deserialise(json.getBytes(), Or.class);

        // Then 2
        assertNotNull(deserialisedFilter);
    }

    @Override
    protected Class<Or> getFunctionClass() {
        return Or.class;
    }

    @Override
    protected Or getInstance() {
        return new Or();
    }
}
