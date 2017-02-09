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
import uk.gov.gchq.gaffer.function.annotation.Inputs;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

public class SimpleFilterFunctionTest extends FilterFunctionTest {
    @Test
    public void shouldThrowExceptionIfMoreThanOneGiven() {
        // Given
        final ExampleSimpleFilterFunction filter = new ExampleSimpleFilterFunction();

        // When / then
        try {
            filter.isValid(new String[]{"Test", "Test2"});
        } catch (final IllegalArgumentException e) {
            assertNotNull(e);
        }
    }

    @Test
    public void shouldThrowExceptionWhenNullArray() {
        // Given
        final ExampleSimpleFilterFunction filter = new ExampleSimpleFilterFunction();

        // When / then
        try {
            filter.isValid((Object[]) null);
        } catch (final IllegalArgumentException e) {
            assertNotNull(e);
        }
    }

    @Test
    public void shouldThrowExceptionWhenInvalidType() {
        // Given
        final ExampleSimpleFilterFunction filter = new ExampleSimpleFilterFunction();

        // When / then
        try {
            filter.isValid(new Object[]{1});
        } catch (final IllegalArgumentException e) {
            assertNotNull(e);
        }
    }

    @Test
    public void shouldAcceptTheValueWhenValidValue() {
        // Given
        final ExampleSimpleFilterFunction filter = new ExampleSimpleFilterFunction();

        // When
        boolean accepted = filter.isValid(new Object[]{"test"});

        // Then
        assertTrue(accepted);
    }

    @Test
    public void shouldClone() {
        // Given
        final ExampleSimpleFilterFunction filter = new ExampleSimpleFilterFunction();

        // When
        final ExampleSimpleFilterFunction clonedFilter = filter.statelessClone();

        // Then
        assertNotSame(filter, clonedFilter);
        assertNotNull(clonedFilter);
    }

    @Test
    public void shouldJsonSerialiseAndDeserialise() throws IOException {
        // Given
        final ExampleSimpleFilterFunction filter = new ExampleSimpleFilterFunction();

        // When
        final String json = serialise(filter);

        // Then
        assertEquals("{\"class\":\"uk.gov.gchq.gaffer.function.SimpleFilterFunctionTest$ExampleSimpleFilterFunction\"}", json);

        // When 2
        final ExampleSimpleFilterFunction deserialisedFilter = (ExampleSimpleFilterFunction) deserialise(json);

        // Then 2
        assertNotNull(deserialisedFilter);
    }

    @Override
    protected Class<? extends ConsumerFunction> getFunctionClass() {
        return ExampleSimpleFilterFunction.class;
    }

    @Override
    protected ExampleSimpleFilterFunction getInstance() {
        return new ExampleSimpleFilterFunction();
    }

    @Inputs(String.class)
    public static class ExampleSimpleFilterFunction extends SimpleFilterFunction<String> {

        ExampleSimpleFilterFunction() {
        }

        @Override
        public boolean isValid(final String input) {
            return true;
        }

        @Override
        public ExampleSimpleFilterFunction statelessClone() {
            return new ExampleSimpleFilterFunction();
        }
    }
}
