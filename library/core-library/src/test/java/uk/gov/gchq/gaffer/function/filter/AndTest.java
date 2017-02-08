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
import java.util.Date;
import java.util.InputMismatchException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class AndTest extends FilterFunctionTest {

    @Test
    public void shouldAcceptWhenAllFunctionsAccept() {
        // Given
        final String test = "test";
        final String test1a = "test1a";
        final String test1b = "test1b";
        final String test2a = "test2a";

        final ConsumerFunctionContext<Integer, FilterFunction> funcContext1 = mock(ConsumerFunctionContext.class);
        final ConsumerFunctionContext<Integer, FilterFunction> funcContext2 = mock(ConsumerFunctionContext.class);
        final FilterFunction func1 = mock(FilterFunction.class);
        final FilterFunction func2 = mock(FilterFunction.class);
        final And and = new And(Arrays.asList(funcContext1, funcContext2));

        given(funcContext1.getFunction()).willReturn(func1);
        given(funcContext2.getFunction()).willReturn(func2);

        given(funcContext1.select(Mockito.any(ArrayTuple.class))).willReturn(new Object[]{test, test1a, test1b});
        given(funcContext2.select(Mockito.any(ArrayTuple.class))).willReturn(new Object[]{test, test2a});


        given(func1.isValid(new String[]{test, test1a, test1b})).willReturn(true);
        given(func2.isValid(new String[]{test, test2a})).willReturn(true);

        // When
        boolean accepted = and.isValid(new String[]{test, test1a, test2a, test1b});

        // Then
        assertTrue(accepted);
    }

    @Test
    public void shouldAcceptWhenNoFunctions() {
        // Given
        final And and = new And();

        // When
        boolean accepted = and.isValid(new String[]{"test"});

        // Then
        assertTrue(accepted);
    }

    @Test
    public void shouldAcceptWhenNoFunctionsAndNullInput() {
        // Given
        final And and = new And();

        // When
        boolean accepted = and.isValid(null);

        // Then
        assertTrue(accepted);
    }


    @Test
    public void shouldRejectWhenOneFunctionRejects() {
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
        final And and = new And(Arrays.asList(funcContext1, funcContext2, funcContext3));

        given(funcContext1.getFunction()).willReturn(func1);
        given(funcContext2.getFunction()).willReturn(func2);
        given(funcContext3.getFunction()).willReturn(func3);

        given(funcContext1.select(Mockito.any(ArrayTuple.class))).willReturn(new Object[]{test, test1a, test1b});
        given(funcContext2.select(Mockito.any(ArrayTuple.class))).willReturn(new Object[]{test, test2a});
        given(funcContext3.select(Mockito.any(ArrayTuple.class))).willReturn(new Object[]{test});

        given(func1.isValid(new String[]{test, test1a, test1b})).willReturn(true);
        given(func2.isValid(new String[]{test, test2a})).willReturn(false);
        given(func3.isValid(new String[]{test})).willReturn(true);

        // When
        boolean accepted = and.isValid(new String[]{test, test1a, test2a, test1b});

        // Then
        assertFalse(accepted);
        verify(func1).isValid(new String[]{test, test1a, test1b});
        verify(func2).isValid(new String[]{test, test2a});
        verify(func3, never()).isValid(new String[]{test, test2a});
    }

    @Test
    public void shouldConstructInputTypesCorrectly() {
        // Given
        final ConsumerFunctionContext<Integer, FilterFunction> funcContext1 = mock(ConsumerFunctionContext.class);
        final ConsumerFunctionContext<Integer, FilterFunction> funcContext2 = mock(ConsumerFunctionContext.class);
        final FilterFunction func1 = mock(FilterFunction.class);
        final FilterFunction func2 = mock(FilterFunction.class);
        final And and = new And(Arrays.asList(funcContext1, funcContext2));

        final Class<?>[] expectedInputClasses = new Class<?>[]{Integer.class, String.class, Long.class, Double.class};
        given(func1.getInputClasses()).willReturn(new Class<?>[]{Integer.class, Object.class});
        given(func2.getInputClasses()).willReturn(new Class<?>[]{String.class, Long.class, Double.class});
        given(funcContext1.getFunction()).willReturn(func1);
        given(funcContext2.getFunction()).willReturn(func2);
        given(funcContext1.getSelection()).willReturn(Arrays.asList(0, 1));
        given(funcContext2.getSelection()).willReturn(Arrays.asList(1, 2, 3));

        // When
        final Class<?>[] inputClasses = and.getInputClasses();

        // Then
        assertArrayEquals(expectedInputClasses, inputClasses);
    }

    @Test
    public void shouldThrowExceptionWhenInputTypesAreNotCompatible() {
        // Given
        final ConsumerFunctionContext<Integer, FilterFunction> funcContext1 = mock(ConsumerFunctionContext.class);
        final ConsumerFunctionContext<Integer, FilterFunction> funcContext2 = mock(ConsumerFunctionContext.class);
        final FilterFunction func1 = mock(FilterFunction.class);
        final FilterFunction func2 = mock(FilterFunction.class);
        final And and = new And(Arrays.asList(funcContext1, funcContext2));

        given(func1.getInputClasses()).willReturn(new Class<?>[]{Integer.class, Date.class});
        given(func2.getInputClasses()).willReturn(new Class<?>[]{String.class, Long.class, Double.class});
        given(funcContext1.getFunction()).willReturn(func1);
        given(funcContext2.getFunction()).willReturn(func2);
        given(funcContext1.getSelection()).willReturn(Arrays.asList(0, 1));
        given(funcContext2.getSelection()).willReturn(Arrays.asList(1, 2, 3));

        // When / Then
        try {
            and.getInputClasses();
            fail("Exception expected");
        } catch (InputMismatchException e) {
            assertNotNull(e);
        }
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
        final And and = new And(Arrays.asList(funcContext1, funcContext2));

        given(funcContext1.getFunction()).willReturn(func1);
        given(funcContext2.getFunction()).willReturn(func2);

        given(funcContext1.getSelection()).willReturn(Arrays.asList(0, 1, 3));
        given(funcContext2.getSelection()).willReturn(Arrays.asList(0, 2));

        given(func1.statelessClone()).willReturn(func1Cloned);
        given(func2.statelessClone()).willReturn(func2Cloned);

        // When
        final And clonedAnd = and.statelessClone();

        // Then
        assertNotSame(and, clonedAnd);
        assertNotNull(clonedAnd);
        assertEquals(2, clonedAnd.getFunctions().size());
        assertSame(func1Cloned, clonedAnd.getFunctions().get(0).getFunction());
        assertArrayEquals(new Integer[]{0, 1, 3}, clonedAnd.getFunctions().get(0).getSelection().toArray());
        assertSame(func2Cloned, clonedAnd.getFunctions().get(1).getFunction());
        assertArrayEquals(new Integer[]{0, 2}, clonedAnd.getFunctions().get(1).getSelection().toArray());
    }

    @Test
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final And filter = new And(Collections.singletonList(new ConsumerFunctionContext<Integer, FilterFunction>(new And(), Arrays.asList(0, 1, 2))));

        // When
        final String json = new String(new JSONSerialiser().serialise(filter, true));

        // Then
        JsonUtil.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.function.filter.And\",%n" +
                "  \"functions\" : [ {%n" +
                "    \"function\" : {%n" +
                "      \"class\" : \"uk.gov.gchq.gaffer.function.filter.And\",%n" +
                "      \"functions\" : [ ]%n" +
                "    },%n" +
                "    \"selection\" : [ 0, 1, 2 ]%n" +
                "  } ]%n" +
                "}"), json);


        // When 2
        final And deserialisedFilter = new JSONSerialiser().deserialise(json.getBytes(), And.class);

        // Then 2
        assertNotNull(deserialisedFilter);
    }

    @Override
    protected Class<And> getFunctionClass() {
        return And.class;
    }

    @Override
    protected And getInstance() {
        return new And();
    }
}
