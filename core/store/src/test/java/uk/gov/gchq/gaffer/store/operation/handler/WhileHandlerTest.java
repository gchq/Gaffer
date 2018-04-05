/*
 * Copyright 2017-2018 Crown Copyright
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

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.function.ExtractProperty;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.Map;
import uk.gov.gchq.gaffer.operation.impl.While;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.operation.util.Conditional;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.koryphe.impl.predicate.IsFalse;
import uk.gov.gchq.koryphe.impl.predicate.IsMoreThan;

import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class WhileHandlerTest {

    @Test
    public void shouldSetAndGetMaxRepeats() {
        // Given
        final WhileHandler handler = new WhileHandler();

        // When
        handler.setMaxRepeats(10);

        // Then
        assertEquals(10, handler.getMaxRepeats());

        // When 2
        handler.setMaxRepeats(25);

        // Then 2
        assertEquals(25, handler.getMaxRepeats());
    }

    @Test
    public void shouldRepeatDelegateOperationUntilMaxRepeatsReached() throws OperationException {
        // Given
        final List<EntitySeed> input = Collections.singletonList(mock(EntitySeed.class));
        final int maxRepeats = 3;
        final GetAdjacentIds delegate = mock(GetAdjacentIds.class);
        final GetAdjacentIds delegateClone1 = mock(GetAdjacentIds.class);
        final GetAdjacentIds delegateClone2 = mock(GetAdjacentIds.class);
        final GetAdjacentIds delegateClone3 = mock(GetAdjacentIds.class);
        final Context context = mock(Context.class);
        final Store store = mock(Store.class);

        given(delegate.shallowClone()).willReturn(delegateClone1, delegateClone2, delegateClone3);

        final CloseableIterable result1 = mock(CloseableIterable.class);
        final CloseableIterable result2 = mock(CloseableIterable.class);
        final CloseableIterable result3 = mock(CloseableIterable.class);
        given(store.execute(delegateClone1, context)).willReturn(result1);
        given(store.execute(delegateClone2, context)).willReturn(result2);
        given(store.execute(delegateClone3, context)).willReturn(result3);

        final While operation = new While.Builder<>()
                .input(input)
                .maxRepeats(maxRepeats)
                .operation(delegate)
                .build();

        final WhileHandler handler = new WhileHandler();

        // When
        final Object result = handler.doOperation(operation, context, store);

        // Then
        verify(delegateClone1).setInput(input);
        verify(delegateClone2).setInput(result1);
        verify(delegateClone3).setInput(result2);
        verify(store).execute((Output) delegateClone1, context);
        verify(store).execute((Output) delegateClone2, context);
        verify(store).execute((Output) delegateClone3, context);
        assertSame(result3, result);
    }

    @Test
    public void shouldRepeatWhileConditionIsTrue() throws OperationException {
        // Given
        final List<EntitySeed> input = Collections.singletonList(mock(EntitySeed.class));
        final boolean condition = true;
        final int maxRepeats = 3;
        final GetElements delegate = mock(GetElements.class);
        final GetElements delegateClone1 = mock(GetElements.class);
        final GetElements delegateClone2 = mock(GetElements.class);
        final GetElements delegateClone3 = mock(GetElements.class);
        final Context context = mock(Context.class);
        final Store store = mock(Store.class);

        given(delegate.shallowClone()).willReturn(delegateClone1, delegateClone2, delegateClone3);

        final CloseableIterable result1 = mock(CloseableIterable.class);
        final CloseableIterable result2 = mock(CloseableIterable.class);
        final CloseableIterable result3 = mock(CloseableIterable.class);
        given(store.execute(delegateClone1, context)).willReturn(result1);
        given(store.execute(delegateClone2, context)).willReturn(result2);
        given(store.execute(delegateClone3, context)).willReturn(result3);

        final While operation = new While.Builder<>()
                .input(input)
                .condition(condition)
                .maxRepeats(maxRepeats)
                .operation(delegate)
                .build();

        final WhileHandler handler = new WhileHandler();

        // When
        final Object result = handler.doOperation(operation, context, store);

        // Then
        verify(delegateClone1).setInput(input);
        verify(delegateClone2).setInput(result1);
        verify(delegateClone3).setInput(result2);
        verify(store).execute((Output) delegateClone1, context);
        verify(store).execute((Output) delegateClone2, context);
        verify(store).execute((Output) delegateClone3, context);
        assertSame(result3, result);
    }

    @Test
    public void shouldNotRepeatWhileConditionIsFalse() throws OperationException {
        // Given
        final EntitySeed input = mock(EntitySeed.class);
        final int maxRepeats = 3;
        final boolean condition = false;
        final Operation delegate = mock(GetElements.class);
        final Context context = mock(Context.class);
        final Store store = mock(Store.class);

        final While operation = new While.Builder<>()
                .input(input)
                .maxRepeats(maxRepeats)
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
    public void shouldThrowExceptionWhenMaxConfiguredNumberOfRepeatsExceeded() throws OperationException {
        // Given
        final EntitySeed input = mock(EntitySeed.class);
        final int maxRepeats = 2500;
        final Operation delegate = mock(GetElements.class);
        final Context context = mock(Context.class);
        final Store store = mock(Store.class);

        final While operation = new While.Builder<>()
                .input(input)
                .maxRepeats(maxRepeats)
                .operation(delegate)
                .build();

        final WhileHandler handler = new WhileHandler();

        // When / Then
        try {
            handler.doOperation(operation, context, store);
            fail("Exception expected");
        } catch (final OperationException e) {
            assertTrue(e.getMessage().contains("Max repeats of the While operation is too large: "
                    + maxRepeats + " > " + While.MAX_REPEATS));
        }
    }

    @Test
    public void shouldThrowExceptionWhenPredicateCannotAcceptInputType() throws OperationException {
        // Given
        final Predicate predicate = new IsFalse();
        final Object input = new EntitySeed();
        final Conditional conditional = new Conditional(predicate);
        final Context context = mock(Context.class);
        final Store store = mock(Store.class);

        final While operation = new While.Builder<>()
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

    @Test
    public void shouldUpdateTransformInputAndTestAgainstPredicate() throws OperationException {
        final Edge input = new Edge.Builder()
                .group("testEdge")
                .source("src")
                .dest("dest")
                .directed(true)
                .property("count", 3)
                .build();

        final Map<Element, Object> transform = mock(Map.class);
        final Map<Element, Object> transformClone = mock(Map.class);
        given(transform.shallowClone()).willReturn(transformClone);

        final Predicate predicate = new IsMoreThan(2);
        final Conditional conditional = new Conditional(predicate, transform);
        final Context context = mock(Context.class);
        final Store store = mock(Store.class);

        final GetElements getElements = mock(GetElements.class);

        final While operation = new While.Builder<>()
                .input(input)
                .maxRepeats(1)
                .conditional(conditional)
                .operation(getElements)
                .build();

        final WhileHandler handler = new WhileHandler();

        // When
        handler.doOperation(operation, context, store);

        // Then
        verify(transformClone).setInput(input);
        verify(store).execute(transformClone, context);
    }

    @Test
    public void shouldFailPredicateTestAndNotExecuteDelegateOperation() throws OperationException {
        // Given
        final Edge input = new Edge.Builder()
                .group("testEdge")
                .source("src")
                .dest("dest")
                .directed(true)
                .property("count", 3)
                .build();

        final Map<Element, Object> transform = new Map.Builder<Element>()
                .first(new ExtractProperty("count"))
                .build();

        final Predicate predicate = new IsMoreThan(5);
        final Conditional conditional = new Conditional(predicate, transform);
        final Context context = mock(Context.class);
        final Store store = mock(Store.class);

        final GetElements getElements = mock(GetElements.class);

        final While operation = new While.Builder<>()
                .input(input)
                .maxRepeats(1)
                .conditional(conditional)
                .operation(getElements)
                .build();

        final WhileHandler handler = new WhileHandler();

        // When
        handler.doOperation(operation, context, store);

        // Then
        verify(store, never()).execute((Output) getElements, context);
    }

    @Test
    public void shouldExecuteNonOutputOperation() throws OperationException {
        // Given
        final AddElements addElements = new AddElements.Builder()
                .input(new Edge.Builder().build())
                .build();
        final Context context = mock(Context.class);
        final Store store = mock(Store.class);

        final While operation = new While.Builder<>()
                .operation(addElements)
                .condition(true)
                .maxRepeats(3)
                .build();

        final WhileHandler handler = new WhileHandler();

        // When
        handler.doOperation(operation, context, store);

        // Then
        verify(store, times(3)).execute(addElements, context);
    }

    @Test
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final WhileHandler handler = new WhileHandler();
        handler.setMaxRepeats(5);

        // When
        final byte[] json = JSONSerialiser.serialise(handler);
        final WhileHandler deserialisedHandler = JSONSerialiser.deserialise(json, WhileHandler.class);

        // Then
        assertEquals(5, deserialisedHandler.getMaxRepeats());
    }
}
