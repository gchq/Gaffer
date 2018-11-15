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

import com.google.common.collect.Lists;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.iterable.WrappedCloseableIterable;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.Limit;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.operation.OperationChainValidator;
import uk.gov.gchq.gaffer.store.optimiser.OperationChainOptimiser;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.ValidationResult;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertSame;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;

public class OperationChainHandlerTest {

    @Test
    public void shouldHandleOperationChain() throws OperationException {
        // Given
        final OperationChainValidator opChainValidator = mock(OperationChainValidator.class);
        final List<OperationChainOptimiser> opChainOptimisers = Collections.emptyList();

        final OperationChainHandler opChainHandler = new OperationChainHandler(opChainValidator, opChainOptimisers);

        final Context context = mock(Context.class);
        final Store store = mock(Store.class);
        final User user = mock(User.class);

        final StoreProperties storeProperties = new StoreProperties();

        final GetAdjacentIds op1 = mock(GetAdjacentIds.class);
        final GetElements op2 = mock(GetElements.class);
        final OperationChain opChain = new OperationChain(Arrays.asList(op1, op2));
        final Entity expectedResult = new Entity(TestGroups.ENTITY);

        given(context.getUser()).willReturn(user);
        given(store.getProperties()).willReturn(storeProperties);
        given(opChainValidator.validate(any(), any(), any())).willReturn(new ValidationResult());

        given(store.handleOperation(op1, context)).willReturn(new WrappedCloseableIterable<>(Collections
                .singletonList(new EntitySeed())));
        given(store.handleOperation(op2, context)).willReturn(expectedResult);

        // When
        final Object result = opChainHandler.doOperation(opChain, context, store);

        // Then
        assertSame(expectedResult, result);
    }

    @Test
    public void shouldHandleNonInputOperation() throws OperationException {
        // Given
        final OperationChainValidator opChainValidator = mock(OperationChainValidator.class);
        final List<OperationChainOptimiser> opChainOptimisers = Collections.emptyList();

        final OperationChainHandler opChainHandler = new OperationChainHandler(opChainValidator, opChainOptimisers);

        final Context context = mock(Context.class);
        final Store store = mock(Store.class);
        final User user = mock(User.class);

        final StoreProperties storeProperties = new StoreProperties();

        final GetAllElements op = mock(GetAllElements.class);
        final OperationChain opChain = new OperationChain(Collections.singletonList(op));
        final Entity expectedResult = new Entity(TestGroups.ENTITY);

        given(context.getUser()).willReturn(user);
        given(store.getProperties()).willReturn(storeProperties);
        given(opChainValidator.validate(any(), any(), any())).willReturn(new ValidationResult());

        given(store.handleOperation(op, context)).willReturn(expectedResult);

        // When
        final Object result = opChainHandler.doOperation(opChain, context, store);

        // Then
        assertSame(expectedResult, result);
    }

    @Test
    public void shouldHandleNestedOperationChain() throws OperationException {
        // Given
        final OperationChainValidator opChainValidator = mock(OperationChainValidator.class);
        final List<OperationChainOptimiser> opChainOptimisers = Collections.emptyList();

        final OperationChainHandler opChainHandler = new OperationChainHandler(opChainValidator, opChainOptimisers);

        final Context context = mock(Context.class);
        final Store store = mock(Store.class);
        final User user = mock(User.class);

        final StoreProperties storeProperties = new StoreProperties();

        final GetAdjacentIds op1 = mock(GetAdjacentIds.class);
        final GetElements op2 = mock(GetElements.class);
        final Limit op3 = mock(Limit.class);

        final OperationChain opChain1 = new OperationChain(Arrays.asList(op1, op2));
        final OperationChain opChain2 = new OperationChain(Arrays.asList(opChain1, op3));
        final Entity entityA = new Entity.Builder().group(TestGroups.ENTITY).vertex("A").build();
        final Entity entityB = new Entity.Builder().group(TestGroups.ENTITY).vertex("B").build();

        given(context.getUser()).willReturn(user);
        given(store.getProperties()).willReturn(storeProperties);
        given(opChainValidator.validate(any(), any(), any())).willReturn(new ValidationResult());

        given(store.handleOperation(op1, context)).willReturn(new WrappedCloseableIterable<>(Lists.newArrayList(new EntitySeed("A"), new EntitySeed("B"))));
        given(store.handleOperation(op2, context)).willReturn(new WrappedCloseableIterable<>(Lists.newArrayList(entityA, entityB)));
        given(store.handleOperation(op3, context)).willReturn(entityA);

        // When
        final Object result = opChainHandler.doOperation(opChain2, context, store);

        // Then
        assertSame(entityA, result);
    }
}
