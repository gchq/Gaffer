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
import uk.gov.gchq.gaffer.operation.impl.While;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.operation.util.Conditional;
import uk.gov.gchq.gaffer.operation.util.OperationConstants;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.koryphe.impl.predicate.IsFalse;

import java.util.function.Predicate;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class WhileHandlerTest {

    @Test
    public void shouldRepeatDelegateOperationUntilMaxRepeatsReached() throws OperationException {
        // Given
        final EntitySeed input = mock(EntitySeed.class);
        final int repeats = 5;
        final Operation delegate = mock(GetAdjacentIds.class);
        final Context context = mock(Context.class);
        final Store store = mock(Store.class);

        final While operation = new While.Builder()
                .input(input)
                .repeats(repeats)
                .operation(delegate)
                .build();

        final WhileHandler handler = new WhileHandler();

        // When
        handler.doOperation(operation, context, store);

        // Then
        verify(store, times(repeats)).execute((Output) delegate, context);
    }

    @Test
    public void shouldRepeatWhileConditionIsTrue() throws OperationException {
        // Given
        final EntitySeed input = mock(EntitySeed.class);
        final boolean condition = true;
        final int repeats = 10;
        final Operation delegate = mock(GetElements.class);
        final Context context = mock(Context.class);
        final Store store = mock(Store.class);

        final While operation = new While.Builder()
                .input(input)
                .condition(condition)
                .repeats(repeats)
                .operation(delegate)
                .build();

        final WhileHandler handler = new WhileHandler();

        // When
        handler.doOperation(operation, context, store);

        // Then
        verify(store, times(repeats)).execute((Output) delegate, context);
    }

    @Test
    public void shouldRepeatOnceIfRepeatsNotSet() throws OperationException {
        // Given
        final EntitySeed input = mock(EntitySeed.class);
        final boolean condition = true;
        final Operation delegate = mock(GetAdjacentIds.class);
        final Context context = mock(Context.class);
        final Store store = mock(Store.class);

        final While operation = new While.Builder()
                .input(input)
                .condition(condition)
                .operation(delegate)
                .build();

        final WhileHandler handler = new WhileHandler();

        // When
        handler.doOperation(operation, context, store);

        // Then
        verify(store, times(OperationConstants.TIMES_DEFAULT)).execute((Output) delegate, context);
    }

    @Test
    public void shouldNotRepeatWhileConditionIsFalse() throws OperationException {
        // Given
        final EntitySeed input = mock(EntitySeed.class);
        final int repeats = 3;
        final boolean condition = false;
        final Operation delegate = mock(GetElements.class);
        final Context context = mock(Context.class);
        final Store store = mock(Store.class);

        final While operation = new While.Builder()
                .input(input)
                .repeats(repeats)
                .condition(condition)
                .operation(delegate)
                .build();

        final WhileHandler handler = new WhileHandler();

        // When
        handler.doOperation(operation, context, store);

        // Then
        verify(store, never()).execute((Output) delegate, context);
    }

    @Test
    public void shouldNotExceedMaxConfiguredNumberOfRepeats() throws OperationException {
        // Given
        final EntitySeed input = mock(EntitySeed.class);
        final int repeats = 250;
        final Operation delegate = mock(GetElements.class);
        final Context context = mock(Context.class);
        final Store store = mock(Store.class);

        final While operation = new While.Builder()
                .input(input)
                .repeats(repeats)
                .operation(delegate)
                .build();

        final WhileHandler handler = new WhileHandler();

        // When
        handler.doOperation(operation, context, store);

        // When
        verify(store, times(OperationConstants.MAX_REPEATS_DEFAULT)).execute((Output) delegate, context);
    }

    @Test
    public void shouldThrowExceptionWhenPredicateCannotAcceptInputType() throws OperationException {
        // Given
        final Predicate predicate = new IsFalse();
        final Object input = new EntitySeed();
        final Conditional conditional = new Conditional(predicate);
        final Context context = mock(Context.class);
        final Store store = mock(Store.class);

        final While operation = new While.Builder()
                .input(input)
                .conditional(conditional)
                .operation(new GetElements())
                .build();

        final WhileHandler handler = new WhileHandler();

        // When / Then
        try {
            handler.doOperation(operation, context, store);
            fail("Exception expected");
        } catch (final OperationException e) {
            assertTrue(e.getMessage().contains("The predicate '" + predicate.getClass().getSimpleName() +
            "' cannot accept an input of type '" + input.getClass().getSimpleName() + "'"));
        }

    }
}
