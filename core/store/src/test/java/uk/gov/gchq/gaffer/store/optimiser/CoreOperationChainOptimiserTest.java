/*
 * Copyright 2016 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.gov.gchq.gaffer.store.optimiser;

import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.GetIterableOperation;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.Validatable;
import uk.gov.gchq.gaffer.operation.impl.Deduplicate;
import uk.gov.gchq.gaffer.operation.impl.Limit;
import uk.gov.gchq.gaffer.operation.impl.Validate;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.schema.Schema;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class CoreOperationChainOptimiserTest {
    @Test
    public void shouldAddValidateOperationForValidatableOperation() throws Exception {
        // Given
        final Store store = mock(Store.class);
        final CoreOperationChainOptimiser optimiser = new CoreOperationChainOptimiser(store);
        final Validatable<Integer> validatable1 = mock(Validatable.class);
        final boolean skipInvalidElements = true;
        final CloseableIterable<Element> elements = mock(CloseableIterable.class);
        final OperationChain<Integer> opChain = new OperationChain<>(validatable1);
        final Map<String, String> options = mock(HashMap.class);
        given(validatable1.getOptions()).willReturn(options);
        given(validatable1.isSkipInvalidElements()).willReturn(skipInvalidElements);
        given(validatable1.isValidate()).willReturn(true);
        given(validatable1.getElements()).willReturn(elements);


        // When
        final OperationChain<Integer> optimisedOpChain = optimiser.optimise(opChain);

        // Then
        assertEquals(2, optimisedOpChain.getOperations().size());
        assertSame(elements, ((Validate) optimisedOpChain.getOperations().get(0)).getElements());
        assertSame(options, optimisedOpChain.getOperations().get(0).getOptions());
        assertSame(validatable1, optimisedOpChain.getOperations().get(1));
        verify(validatable1).setElements(null);
    }

    @Test
    public void shouldNotAddValidateOperationWhenValidatableHasValidateSetToFalse() throws Exception {
        // Given
        final Store store = mock(Store.class);
        final CoreOperationChainOptimiser optimiser = new CoreOperationChainOptimiser(store);
        final Validatable<Integer> validatable1 = mock(Validatable.class);
        final boolean skipInvalidElements = true;
        final CloseableIterable<Element> elements = mock(CloseableIterable.class);
        final OperationChain<Integer> opChain = new OperationChain<>(validatable1);

        given(validatable1.isSkipInvalidElements()).willReturn(skipInvalidElements);
        given(validatable1.isValidate()).willReturn(false);
        given(validatable1.getElements()).willReturn(elements);


        // When
        final OperationChain<Integer> optimisedOpChain = optimiser.optimise(opChain);

        // Then
        assertEquals(1, optimisedOpChain.getOperations().size());
        assertSame(validatable1, optimisedOpChain.getOperations().get(0));
        verify(validatable1, never()).setElements(null);
    }

    @Test
    public void shouldThrowExceptionIfValidatableHasValidateSetToFalseAndStoreRequiresValidation() throws Exception {
        // Given
        final Store store = mock(Store.class);
        final CoreOperationChainOptimiser optimiser = new CoreOperationChainOptimiser(store);
        final Schema schema = mock(Schema.class);
        final Validatable<Integer> validatable1 = mock(Validatable.class);
        final OperationChain<Integer> opChain = new OperationChain<>(validatable1);

        given(schema.validate()).willReturn(true);
        given(store.isValidationRequired()).willReturn(true);
        given(validatable1.isValidate()).willReturn(false);

        // When / then
        try {
            optimiser.optimise(opChain);
        } catch (UnsupportedOperationException e) {
            assertNotNull(e);
        }
    }

    @Test
    public void shouldAddValidateOperationsForAllValidatableOperations() throws Exception {
        // Given
        final Store store = mock(Store.class);
        final CoreOperationChainOptimiser optimiser = new CoreOperationChainOptimiser(store);
        final CloseableIterable<Element> elements = mock(CloseableIterable.class);
        final Validatable<Integer> validatable1 = mock(Validatable.class);
        final Operation<Iterable<Element>, Iterable<Element>> nonValidatable1 = mock(Operation.class);
        final Validatable<Iterable<Element>> validatable2 = mock(Validatable.class);
        final Validatable<Iterable<Element>> validatable3 = mock(Validatable.class);
        final Operation<Iterable<Element>, Iterable<Element>> nonValidatable2 = mock(Operation.class);
        final boolean skipInvalidElements = true;
        final OperationChain<Integer> opChain = new OperationChain.Builder()
                .first(nonValidatable2)
                .then(validatable3)
                .then(validatable2)
                .then(nonValidatable1)
                .then(validatable1)
                .build();


        given(validatable1.getElements()).willReturn(elements);
        given(validatable1.isSkipInvalidElements()).willReturn(skipInvalidElements);
        given(validatable2.isSkipInvalidElements()).willReturn(skipInvalidElements);

        given(validatable1.isValidate()).willReturn(true);
        given(validatable2.isValidate()).willReturn(true);
        given(validatable3.isValidate()).willReturn(false);

        // When
        final OperationChain<Integer> optimisedOpChain = optimiser.optimise(opChain);

        // Then
        assertEquals(7, optimisedOpChain.getOperations().size());
        assertSame(nonValidatable2, optimisedOpChain.getOperations().get(0));
        assertSame(validatable3, optimisedOpChain.getOperations().get(1));
        assertTrue(optimisedOpChain.getOperations().get(2) instanceof Validate);
        assertSame(validatable2, optimisedOpChain.getOperations().get(3));
        assertSame(nonValidatable1, optimisedOpChain.getOperations().get(4));
        assertTrue(optimisedOpChain.getOperations().get(5) instanceof Validate);
        assertSame(elements, ((Validate) optimisedOpChain.getOperations().get(5)).getElements());
        assertSame(validatable1, optimisedOpChain.getOperations().get(6));
        verify(validatable2).setElements(null);
        verify(validatable1).setElements(null);
        verify(validatable3, never()).setElements(null);
    }

    @Test
    public void shouldAddDeduplicateOperationForGetOperationsWithDuplicateFlag() throws Exception {
        // Given
        final Store store = mock(Store.class);
        final CoreOperationChainOptimiser optimiser = new CoreOperationChainOptimiser(store);
        final GetIterableOperation getOperation = mock(GetIterableOperation.class);
        final OperationChain<Integer> opChain = new OperationChain<>(getOperation);
        given(getOperation.getResultLimit()).willReturn(null);
        given(getOperation.isDeduplicate()).willReturn(true);

        // When
        final OperationChain<Integer> optimisedOpChain = optimiser.optimise(opChain);

        // Then
        assertEquals(2, optimisedOpChain.getOperations().size());
        assertSame(getOperation, optimisedOpChain.getOperations().get(0));
        assertTrue(optimisedOpChain.getOperations().get(1) instanceof Deduplicate);
    }

    @Test
    public void shouldNotAddDeduplicateOperationForGetOperationsWithoutFlag() throws Exception {
        // Given
        final Store store = mock(Store.class);
        final CoreOperationChainOptimiser optimiser = new CoreOperationChainOptimiser(store);
        final GetIterableOperation getOperation = mock(GetIterableOperation.class);
        final OperationChain<Integer> opChain = new OperationChain<>(getOperation);
        given(getOperation.getResultLimit()).willReturn(null);
        given(getOperation.isDeduplicate()).willReturn(false);

        // When
        final OperationChain<Integer> optimisedOpChain = optimiser.optimise(opChain);

        // Then
        assertEquals(1, optimisedOpChain.getOperations().size());
        assertSame(getOperation, optimisedOpChain.getOperations().get(0));
    }

    @Test
    public void shouldAddLimitOperationForGetOperationsWithLimit() throws Exception {
        // Given
        final Store store = mock(Store.class);
        final CoreOperationChainOptimiser optimiser = new CoreOperationChainOptimiser(store);
        final GetIterableOperation getOperation = mock(GetIterableOperation.class);
        final OperationChain<Integer> opChain = new OperationChain<>(getOperation);
        final int resultLimit = 5;
        given(getOperation.getResultLimit()).willReturn(resultLimit);

        // When
        final OperationChain<Integer> optimisedOpChain = optimiser.optimise(opChain);

        // Then
        assertEquals(2, optimisedOpChain.getOperations().size());
        assertSame(getOperation, optimisedOpChain.getOperations().get(0));
        assertTrue(optimisedOpChain.getOperations().get(1) instanceof Limit);
        assertEquals(resultLimit, (int) ((Limit) optimisedOpChain.getOperations().get(1)).getResultLimit());
    }

    @Test
    public void shouldNotAddLimitOperationForGetOperationsWithoutLimit() throws Exception {
        // Given
        final Store store = mock(Store.class);
        final CoreOperationChainOptimiser optimiser = new CoreOperationChainOptimiser(store);
        final GetIterableOperation getOperation = mock(GetIterableOperation.class);
        final OperationChain<Integer> opChain = new OperationChain<>(getOperation);
        final Integer resultLimit = null;
        given(getOperation.getResultLimit()).willReturn(resultLimit);

        // When
        final OperationChain<Integer> optimisedOpChain = optimiser.optimise(opChain);


        // Then
        assertEquals(1, optimisedOpChain.getOperations().size());
        assertSame(getOperation, optimisedOpChain.getOperations().get(0));
    }

    @Test
    public void shouldAddLimitOperationBeforeDeduplicateOperationForPerformance() throws Exception {
        // Given
        final Store store = mock(Store.class);
        final CoreOperationChainOptimiser optimiser = new CoreOperationChainOptimiser(store);
        final GetIterableOperation getOperation = mock(GetIterableOperation.class);
        final OperationChain<Integer> opChain = new OperationChain<>(getOperation);
        final int resultLimit = 5;
        given(getOperation.getResultLimit()).willReturn(resultLimit);
        given(getOperation.isDeduplicate()).willReturn(true);

        // When
        final OperationChain<Integer> optimisedOpChain = optimiser.optimise(opChain);

        // Then
        assertEquals(3, optimisedOpChain.getOperations().size());
        assertSame(getOperation, optimisedOpChain.getOperations().get(0));
        assertTrue(optimisedOpChain.getOperations().get(1) instanceof Limit);
        assertEquals(resultLimit, (int) ((Limit) optimisedOpChain.getOperations().get(1)).getResultLimit());
        assertTrue(optimisedOpChain.getOperations().get(2) instanceof Deduplicate);
    }
}