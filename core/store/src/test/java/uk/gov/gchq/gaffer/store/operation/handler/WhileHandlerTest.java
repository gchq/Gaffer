/*
 * Copyright 2017-2020 Crown Copyright
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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.function.ExtractProperty;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class WhileHandlerTest {

    @Test
    public void shouldSetAndGetMaxRepeats() {
        // Given
        final WhileHandler handler = new WhileHandler();

        // When
        handler.setMaxRepeats(10);

        // Then
        assertThat(handler.getMaxRepeats()).isEqualTo(10);

        // When 2
        handler.setMaxRepeats(25);

        // Then 2
        assertThat(handler.getMaxRepeats()).isEqualTo(25);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void shouldRepeatDelegateOperationUntilMaxRepeatsReached(@Mock final GetAdjacentIds delegate,
                                                                    @Mock final GetAdjacentIds delegateClone1,
                                                                    @Mock final GetAdjacentIds delegateClone2,
                                                                    @Mock final GetAdjacentIds delegateClone3,
                                                                    @Mock final Iterable result1,
                                                                    @Mock final Iterable result2,
                                                                    @Mock final Iterable result3,
                                                                    @Mock final Context context,
                                                                    @Mock final Store store)
            throws OperationException {
        // Given
        final List<EntitySeed> input = Collections.singletonList(mock(EntitySeed.class));
        final int maxRepeats = 3;

        given(delegate.shallowClone()).willReturn(delegateClone1, delegateClone2, delegateClone3);

        given(store.execute(delegateClone1, context)).willReturn(result1);
        given(store.execute(delegateClone2, context)).willReturn(result2);
        given(store.execute(delegateClone3, context)).willReturn(result3);

        final While<?, ?> operation = new While.Builder<>()
                .input(input)
                .maxRepeats(maxRepeats)
                .operation(delegate)
                .build();

        final WhileHandler handler = new WhileHandler();

        // When
        final Object result = handler.doOperation(operation, context, store);

        // Then
        verify(delegateClone1, times(1)).getInput();
        verify(delegateClone2, times(1)).getInput();
        verify(delegateClone3, times(1)).getInput();
        verify(store).execute((Output) delegateClone1, context);
        verify(store).execute((Output) delegateClone2, context);
        verify(store).execute((Output) delegateClone3, context);

        assertThat(result).isSameAs(result3);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void shouldRepeatWhileConditionIsTrue(@Mock final GetAdjacentIds delegate,
                                                 @Mock final GetAdjacentIds delegateClone1,
                                                 @Mock final GetAdjacentIds delegateClone2,
                                                 @Mock final GetAdjacentIds delegateClone3,
                                                 @Mock final Iterable result1,
                                                 @Mock final Iterable result2,
                                                 @Mock final Iterable result3,
                                                 @Mock final Context context,
                                                 @Mock final Store store)
            throws OperationException {
        // Given
        final List<EntitySeed> input = Collections.singletonList(mock(EntitySeed.class));
        final boolean condition = true;
        final int maxRepeats = 3;

        given(delegate.shallowClone()).willReturn(delegateClone1, delegateClone2, delegateClone3);
        given(store.execute(delegateClone1, context)).willReturn(result1);
        given(store.execute(delegateClone2, context)).willReturn(result2);
        given(store.execute(delegateClone3, context)).willReturn(result3);

        final While<?, ?> operation = new While.Builder<>()
                .input(input)
                .condition(condition)
                .maxRepeats(maxRepeats)
                .operation(delegate)
                .build();

        final WhileHandler handler = new WhileHandler();

        // When
        final Object result = handler.doOperation(operation, context, store);

        // Then
        verify(delegateClone1, times(1)).getInput();
        verify(delegateClone2, times(1)).getInput();
        verify(delegateClone3, times(1)).getInput();
        verify(store).execute((Output) delegateClone1, context);
        verify(store).execute((Output) delegateClone2, context);
        verify(store).execute((Output) delegateClone3, context);
        assertThat(result).isSameAs(result3);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void shouldNotRepeatWhileConditionIsFalse(@Mock final GetAdjacentIds delegate,
                                                     @Mock final Context context,
                                                     @Mock final Store store,
                                                     @Mock final EntitySeed input)
            throws OperationException {
        // Given
        final int maxRepeats = 3;
        final boolean condition = false;

        final While<?, ?> operation = new While.Builder<>()
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
    public void shouldThrowExceptionWhenMaxConfiguredNumberOfRepeatsExceeded(@Mock final GetAdjacentIds delegate,
                                                                             @Mock final Context context,
                                                                             @Mock final Store store,
                                                                             @Mock final EntitySeed input)
            throws OperationException {
        // Given
        final int maxRepeats = 2500;

        final While<?, ?> operation = new While.Builder<>()
                .input(input)
                .maxRepeats(maxRepeats)
                .operation(delegate)
                .build();

        final WhileHandler handler = new WhileHandler();

        // When / Then
        assertThatExceptionOfType(OperationException.class).isThrownBy(() -> handler.doOperation(operation, context, store))
                .withMessageContaining(String.format("Max repeats of the While operation is too large: %s > %s", maxRepeats, While.MAX_REPEATS));
    }

    @Test
    public void shouldThrowExceptionWhenPredicateCannotAcceptInputType(@Mock final Context context,
                                                                       @Mock final Store store)
            throws OperationException {
        // Given
        final Predicate<?> predicate = new IsFalse();
        final Object input = new EntitySeed();
        final Conditional conditional = new Conditional(predicate);

        final While<?, ?> operation = new While.Builder<>()
                .input(input)
                .conditional(conditional)
                .operation(new GetElements())
                .build();

        final WhileHandler handler = new WhileHandler();

        // When / Then
        assertThatExceptionOfType(OperationException.class).isThrownBy(() -> handler.doOperation(operation, context, store))
                .withMessageContaining(
                        String.format("The predicate '%s' cannot accept an input of type '%s'", predicate.getClass().getSimpleName(),
                                input.getClass().getSimpleName()));
    }

    @Test
    public void shouldUpdateTransformInputAndTestAgainstPredicate(@Mock final Context context,
                                                                  @Mock final Store store,
                                                                  @Mock final Map<Element, Object> transform,
                                                                  @Mock final Map<Element, Object> transformClone,
                                                                  @Mock final GetElements getElements)
            throws OperationException {
        final Edge input = new Edge.Builder()
                .group("testEdge")
                .source("src")
                .dest("dest")
                .directed(true)
                .property("count", 3)
                .build();

        given(transform.shallowClone()).willReturn(transformClone);

        final Predicate<?> predicate = new IsMoreThan(2);
        final Conditional conditional = new Conditional(predicate, transform);

        final While<?, ?> operation = new While.Builder<>()
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

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void shouldFailPredicateTestAndNotExecuteDelegateOperation(@Mock final Context context,
                                                                      @Mock final Store store,
                                                                      @Mock final GetElements getElements)
            throws OperationException {
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

        final Predicate<?> predicate = new IsMoreThan(5);
        final Conditional conditional = new Conditional(predicate, transform);

        final While<?, ?> operation = new While.Builder<>()
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
    public void shouldExecuteNonOutputOperation(@Mock final Context context,
                                                @Mock final Store store)
            throws OperationException {
        // Given
        final AddElements addElements = new AddElements.Builder()
                .input(new Edge.Builder().build())
                .build();

        final While<?, ?> operation = new While.Builder<>()
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
        assertThat(deserialisedHandler.getMaxRepeats()).isEqualTo(5);
    }
}
