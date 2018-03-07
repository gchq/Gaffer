/*
 * Copyright 2017 Crown Copyright
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
package uk.gov.gchq.gaffer.store.operation.handler;

import org.junit.Test;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.Repeat;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class RepeatHandlerTest {

    @Test
    public void shouldRepeatDelegateOperationBySetNumberOfTimes() throws OperationException {
        // Given
        final EntitySeed input = mock(EntitySeed.class);
        final int times = 5;
        final Operation delegate = mock(GetAdjacentIds.class);
        final Context context = mock(Context.class);
        final Store store = mock(Store.class);

        final Repeat repeat = new Repeat.Builder()
                .input(input)
                .operation(delegate)
                .times(times)
                .build();

        final RepeatHandler handler = new RepeatHandler();

        // When
        final Object result = handler.doOperation(repeat, context, store);

        // Then
        verify(store, times(times)).execute((Output) delegate, context);
    }

    @Test
    public void shouldThrowExceptionForNullInput() {
        // Given
        final Object input = null;
        final int times = 2;
        final Operation delegate = mock(GetAdjacentIds.class);
        final Context context = mock(Context.class);
        final Store store = mock(Store.class);

        final Repeat repeat = new Repeat.Builder()
                .input(input)
                .operation(delegate)
                .times(times)
                .build();

        final RepeatHandler handler = new RepeatHandler();

        // When / Then
        try {
            handler.doOperation(repeat, context, store);
        } catch (final OperationException e) {
            assertTrue(e.getMessage().contains("Input cannot be null"));
        }
    }

    @Test
    public void shouldThrowExceptionIfTimesExceedsMaximum() {
        // Given
        final EntitySeed input = new EntitySeed("A");
        final int times = 25;
        final Operation delegate = mock(GetAdjacentIds.class);
        final Context context = mock(Context.class);
        final Store store = mock(Store.class);

        final Repeat repeat =  new Repeat.Builder()
                .input(input)
                .operation(delegate)
                .times(times)
                .build();

        final RepeatHandler handler = new RepeatHandler();

        // When / Then
        try {
            handler.doOperation(repeat, context, store);
        } catch (final OperationException e) {
            assertTrue(e.getMessage().contains("Maximum number of allowed repeats: 20 exceeded: " + times));
        }
    }
}
