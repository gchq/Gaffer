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

package uk.gov.gchq.gaffer.function.context;

import org.junit.Test;
import uk.gov.gchq.gaffer.function.ConsumerFunction;
import uk.gov.gchq.gaffer.function.Tuple;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class ConsumerFunctionContextTest {
    @Test
    public void shouldBuildContext() {
        // Given
        final String reference1 = "reference 1";
        final String reference2 = "reference 2";

        final ConsumerFunction func1 = mock(ConsumerFunction.class);

        // When
        final ConsumerFunctionContext<String, ConsumerFunction> context =
                new ConsumerFunctionContext.Builder<String, ConsumerFunction>()
                        .select(reference1, reference2)
                        .execute(func1)
                        .build();

        // Then
        assertEquals(2, context.getSelection().size());
        assertEquals(reference1, context.getSelection().get(0));
        assertEquals(reference2, context.getSelection().get(1));
        assertSame(func1, context.getFunction());
    }

    @Test
    public void shouldThrowExceptionWhenBuildContextWhenSelectCalledTwice() {
        // Given
        final String reference1 = "reference 1";
        final String reference2 = "reference 2";
        final ConsumerFunction func1 = mock(ConsumerFunction.class);

        // When / Then
        try {
            new ConsumerFunctionContext.Builder<String, ConsumerFunction>()
                    .select(reference1)
                    .execute(func1)
                    .select(reference2)
                    .build();
            fail("Exception expected");
        } catch (final IllegalStateException e) {
            assertNotNull(e);
        }
    }

    @Test
    public void shouldThrowExceptionWhenBuildContextWhenExecuteCalledTwice() {
        // Given
        final String reference1 = "reference 1";
        final ConsumerFunction func1 = mock(ConsumerFunction.class);
        final ConsumerFunction func2 = mock(ConsumerFunction.class);

        // When / Then
        try {
            new ConsumerFunctionContext.Builder<String, ConsumerFunction>()
                    .select(reference1)
                    .execute(func1)
                    .execute(func2)
                    .build();
            fail("Exception expected");
        } catch (final IllegalStateException e) {
            assertNotNull(e);
        }
    }

    @Test
    public void shouldSelectValuesFromTuple() {
        // Given
        final String reference1 = "reference 1";
        final String reference2 = "reference 2";
        final String reference3 = "reference 3";
        final String value1 = "value 1";
        final String value2 = null;
        final String value3 = "value 2";
        final Object[] values = {value1, value2, value3};
        final List<Object> selections = Arrays.asList((Object) reference1, reference2, reference3);
        final Tuple<Object> tuple = mock(Tuple.class);
        given(tuple.get(reference1)).willReturn(value1);
        given(tuple.get(reference2)).willReturn(value2);
        given(tuple.get(reference3)).willReturn(value3);

        final ConsumerFunctionContext<Object, ConsumerFunction> context = new ConsumerFunctionContext<>();
        context.setSelection(selections);

        // When
        final Object[] selectedValues = context.select(tuple);

        // Then
        assertArrayEquals(values, selectedValues);
    }
}
