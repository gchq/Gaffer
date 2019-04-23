/*
 * Copyright 2017-2019 Crown Copyright
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

import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.GetWalks;
import uk.gov.gchq.gaffer.operation.impl.If;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.util.Conditional;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.user.User;

import java.util.Arrays;
import java.util.function.Predicate;

import static org.junit.Assert.assertEquals;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class IfHandlerTest {

    private final Store store = mock(Store.class);
    private final Context context = new Context(new User());

    @Test
    public void shouldExecuteThenOperationWhenConditionMet() throws OperationException {
        // Given
        final Object input = Arrays.asList(new EntitySeed("1"), new EntitySeed("2"));
        final Conditional conditional = mock(Conditional.class);
        final Predicate<Object> predicate = mock(Predicate.class);
        final GetWalks then = mock(GetWalks.class);
        final GetElements otherwise = mock(GetElements.class);

        final If filter = new If.Builder<>()
                .input(input)
                .conditional(conditional)
                .then(then)
                .otherwise(otherwise)
                .build();

        final IfHandler handler = new IfHandler();

        given(conditional.getPredicate()).willReturn(predicate);
        given(predicate.test(input)).willReturn(true);

        // When
        final Object result = handler.doOperation(filter, context, store);

        // Then
        verify(predicate).test(input);
        verify(store).execute(then, context);
        verify(store, never()).execute(otherwise, context);
    }

    @Test
    public void shouldExecuteOtherwiseOperationWhenConditionNotMet() throws OperationException {
        // Given
        final Object input = Arrays.asList(new EntitySeed("1"), new EntitySeed("2"));
        final Conditional conditional = mock(Conditional.class);
        final Predicate<Object> predicate = mock(Predicate.class);
        final GetWalks then = mock(GetWalks.class);
        final GetElements otherwise = mock(GetElements.class);

        final If filter = new If.Builder<>()
                .input(input)
                .conditional(conditional)
                .then(then)
                .otherwise(otherwise)
                .build();

        final IfHandler handler = new IfHandler();

        given(conditional.getPredicate()).willReturn(predicate);
        given(predicate.test(input)).willReturn(false);

        // When
        final Object result = handler.doOperation(filter, context, store);

        // Then
        verify(predicate).test(input);
        verify(store, never()).execute(then, context);
        verify(store).execute(otherwise, context);
    }

    @Test
    public void shouldReturnInitialInputForNullOperations() throws OperationException {
        // Given
        final Object input = Arrays.asList(new EntitySeed("1"), new EntitySeed("2"));
        final Conditional conditional = mock(Conditional.class);
        final Predicate<Object> predicate = mock(Predicate.class);
        final GetWalks then = null;
        final GetElements otherwise = null;

        final If filter = new If.Builder<>()
                .input(input)
                .conditional(conditional)
                .then(then)
                .otherwise(otherwise)
                .build();

        given(conditional.getPredicate()).willReturn(predicate);
        given(predicate.test(input)).willReturn(true);

        final IfHandler handler = new IfHandler();

        // When
        final Object result = handler.doOperation(filter, context, store);

        // Then
        assertEquals(result, input);
        verify(predicate).test(input);
        verify(store, never()).execute(then, context);
        verify(store, never()).execute(otherwise, context);
    }

    @Test
    public void shouldExecuteThenWithBooleanCondition() throws OperationException {
        // Given
        final Object input = Arrays.asList(new EntitySeed("1"), new EntitySeed("2"));
        final GetElements then = mock(GetElements.class);
        final GetAllElements otherwise = mock(GetAllElements.class);

        final If filter = new If.Builder<>()
                .input(input)
                .condition(true)
                .then(then)
                .otherwise(otherwise)
                .build();

        final IfHandler handler = new IfHandler();

        // When
        final Object result = handler.doOperation(filter, context, store);

        // Then
        verify(store).execute(then, context);
        verify(store, never()).execute(otherwise, context);
    }

    @Test
    public void shouldExecuteCorrectlyWithOperationChainAsThen() throws OperationException {
        // Given
        final Object input = Arrays.asList(new EntitySeed("1"), new EntitySeed("2"));
        final Conditional conditional = mock(Conditional.class);
        final Predicate<Object> predicate = mock(Predicate.class);
        final OperationChain<Object> then = mock(OperationChain.class);
        final GetAllElements otherwise = mock(GetAllElements.class);

        final If filter = new If.Builder<>()
                .input(input)
                .conditional(conditional)
                .then(then)
                .otherwise(otherwise)
                .build();

        final IfHandler handler = new IfHandler();

        given(conditional.getPredicate()).willReturn(predicate);
        given(predicate.test(input)).willReturn(true);

        // When
        final Object result = handler.doOperation(filter, context, store);

        // Then
        verify(store).execute(then, context);
        verify(store, never()).execute(otherwise, context);
    }

    @Test
    public void shouldCorrectlyExecutePrePredicateTransformUsingConditional() throws OperationException {
        // Given
        final Object input = Arrays.asList(new EntitySeed("A"), new EntitySeed("B"));
        final Object intermediate = Arrays.asList(new EntitySeed("1"), new EntitySeed("2"));
        final Conditional conditional = mock(Conditional.class);
        final Predicate<Object> predicate = mock(Predicate.class);
        final OperationChain<Object> transform = mock(OperationChain.class);
        final GetElements then = mock(GetElements.class);
        final GetAllElements otherwise = mock(GetAllElements.class);

        final If filter = new If.Builder<>()
                .input(input)
                .conditional(conditional)
                .then(then)
                .build();

        final IfHandler handler = new IfHandler();

        given(conditional.getPredicate()).willReturn(predicate);
        given(conditional.getTransform()).willReturn(transform);
        given(store.execute(transform, context)).willReturn(intermediate);
        given(predicate.test(intermediate)).willReturn(true);

        // When
        final Object result = handler.doOperation(filter, context, store);

        // Then
        verify(predicate).test(intermediate);
        verify(predicate, never()).test(input);
        verify(store).execute(transform, context);
        verify(store).execute(then, context);
        verify(store, never()).execute(otherwise, context);
    }
}
