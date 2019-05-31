/*
 * Copyright 2018-2019 Crown Copyright
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

package uk.gov.gchq.gaffer.store.operation.handler.output;

import org.junit.Test;

import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.output.ToSingletonList;
import uk.gov.gchq.gaffer.store.Context;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ToSingletonListHandlerTest {
    @Test
    public void shouldConvertIntToSingletonList() throws OperationException {
        // Given
        final int singleInt = 4;
        final List<Integer> expectedResult = Arrays.asList(4);

        final ToSingletonListHandler handler = new ToSingletonListHandler();
        final ToSingletonList<Integer> operation = new ToSingletonList.Builder<Integer>().input(singleInt).build();

        // When
        final Iterable<Integer> result = handler.doOperation(operation, new Context(), null);

        // Then
        assertEquals(expectedResult, result);
    }

    @Test
    public void shouldHandleNullInput() throws OperationException {
        // Given
        final ToSingletonListHandler handler = new ToSingletonListHandler();
        final ToSingletonList<Integer> operation = new ToSingletonList.Builder<Integer>().input(null).build();

        // When
        try {
            handler.doOperation(operation, new Context(), null);
        } catch (final OperationException e) {
            assertTrue(e.getMessage().equals("Input cannot be null"));
        }
    }
}
