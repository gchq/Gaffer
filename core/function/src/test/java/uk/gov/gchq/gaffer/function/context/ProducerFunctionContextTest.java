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
import uk.gov.gchq.gaffer.function.ProducerFunction;
import uk.gov.gchq.gaffer.function.Tuple;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class ProducerFunctionContextTest {
    @Test
    public void shouldBuildContext() {
        // Given
        final String reference1 = "reference 1";
        final String reference2 = "reference 2";

        final ProducerFunction func1 = mock(ProducerFunction.class);

        // When
        final ProducerFunctionContext<String, ProducerFunction> context =
                new ProducerFunctionContext.Builder<String, ProducerFunction>()
                        .execute(func1)
                        .project(reference1, reference2)
                        .build();

        // Then
        assertEquals(2, context.getProjection().size());
        assertEquals(reference1, context.getProjection().get(0));
        assertEquals(reference2, context.getProjection().get(1));
        assertSame(func1, context.getFunction());
    }

    @Test
    public void shouldThrowExceptionWhenBuildContextWhenProjectCalledTwice() {
        // Given
        final String reference1 = "reference 1";
        final String reference2 = "reference 2";
        final ProducerFunction func1 = mock(ProducerFunction.class);

        // When / Then
        try {
            new ProducerFunctionContext.Builder<String, ProducerFunction>()
                    .execute(func1)
                    .project(reference1)
                    .project(reference2)
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
        final ProducerFunction func1 = mock(ProducerFunction.class);
        final ProducerFunction func2 = mock(ProducerFunction.class);

        // When / Then
        try {
            new ProducerFunctionContext.Builder<String, ProducerFunction>()
                    .execute(func1)
                    .project(reference1)
                    .execute(func2)
                    .build();
            fail("Exception expected");
        } catch (final IllegalStateException e) {
            assertNotNull(e);
        }
    }

    @Test
    public void shouldProjectValuesOntoTuple() {
        // Given
        final String reference1 = "reference 1";
        final String reference2 = "reference 2";
        final String value1 = "value 1";
        final String value2 = "value 2";
        final Object[] values = {value1, value2};
        final List<Object> projection = Arrays.asList((Object) reference1, reference2);
        final Tuple<Object> tuple = mock(Tuple.class);

        final ProducerFunctionContext<Object, ProducerFunction> context = new ProducerFunctionContext<>();
        context.setProjection(projection);

        // When
        context.project(tuple, values);

        // Then
        verify(tuple).put(reference1, value1);
        verify(tuple).put(reference2, value2);
    }

    @Test
    public void shouldProjectNullValuesOntoTuple() {
        // Given
        final String reference1 = "reference 1";
        final String reference2 = "reference 2";
        final String value1 = "value 1";
        final Object[] values = {value1};
        final List<Object> projection = Arrays.asList((Object) reference1, reference2);
        final Tuple<Object> tuple = mock(Tuple.class);

        final ProducerFunctionContext<Object, ProducerFunction> context = new ProducerFunctionContext<>();
        context.setProjection(projection);

        // When
        context.project(tuple, values);

        // Then
        verify(tuple).put(reference1, value1);
        verify(tuple).put(reference2, null);
    }
}
