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
package uk.gov.gchq.gaffer.operation.impl;

import com.google.common.collect.Lists;
import org.junit.Test;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationTest;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;

import java.util.Collection;
import java.util.Collections;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class IfTest extends OperationTest {
    @Override
    public void builderShouldCreatePopulatedOperation() {
        // Given
        final If<Object, Object> filter = getTestObject();

        // Then
        assertThat(filter.getInput(), is(notNullValue()));
        assertTrue(filter.getCondition());
        assertTrue(filter.getThen() instanceof GetElements);
        assertTrue(filter.getOtherwise() instanceof GetAllElements);
    }

    @Override
    public void shouldShallowCloneOperation() {
        // Given
        final Object input = "testInput";
        final If filter = new If.Builder<>()
                .input(input)
                .condition(true)
                .then(new GetElements.Builder()
                        .input(new EntitySeed("A"))
                        .build())
                .otherwise(new GetAllElements())
                .build();

        // When
        final If clone = filter.shallowClone();

        // Then
        assertNotSame(filter, clone);
        assertEquals(input, clone.getInput());
    }

    @Override
    protected If<Object, Object> getTestObject() {
        return new If.Builder<>()
                .input("testInput")
                .condition(true)
                .then(new GetElements.Builder()
                        .input(new EntitySeed("A"))
                        .build())
                .otherwise(new GetAllElements())
                .build();
    }

    @Test
    public void shouldGetOperations() {
        // Given
        final GetElements getElements = new GetElements.Builder()
                .input(new EntitySeed("A"))
                .build();

        final OperationChain opChain = new OperationChain.Builder()
                .first(new GetAllElements())
                .then(new Limit<>(3))
                .build();

        final If<Object, Object> filter = new If.Builder<>()
                .condition(true)
                .then(getElements)
                .otherwise(opChain)
                .build();

        final Collection<Operation> expectedOps = Lists.newArrayList(OperationChain.wrap(getElements), opChain);

        // When
        final Collection<Operation> result = filter.getOperations();

        // Then
        assertEquals(expectedOps, result);
    }

    @Test
    public void shouldUpdateOperations() {
        // Given
        final GetElements getElements = new GetElements.Builder()
                .input(new EntitySeed("A"))
                .build();

        final OperationChain opChain = new OperationChain.Builder()
                .first(new GetAllElements())
                .then(new Limit<>(3))
                .build();

        final If<Object, Object> filter = new If.Builder<>()
                .condition(false)
                .build();

        final Collection<Operation> opList = Lists.newArrayList(getElements, opChain);

        // When
        filter.updateOperations(opList);

        // Then
        assertNotNull(filter.getThen());
        assertNotNull(filter.getOtherwise());
        assertEquals(getElements, filter.getThen());
        assertEquals(opChain, filter.getOtherwise());
    }

    @Test
    public void shouldThrowErrorForTryingToUpdateOperationsWithEmptyList() {
        // Given
        final If<Object, Object> filter = new If.Builder<>()
                .condition(true)
                .build();

        final Collection<Operation> opList = Collections.emptyList();

        // When / Then
        try {
            filter.updateOperations(opList);
            fail("Exception expected");
        } catch (final IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Unable to update operations - there are not enough operations to set \"then\""));
        }
    }

    @Test
    public void shouldThrowErrorForTryingToUpdateOperationsWithTooFewOps() {
        // Given
        final GetElements getElements = new GetElements.Builder()
                .input(new EntitySeed("1"))
                .build();

        final If<Object, Object> filter = new If.Builder<>()
                .condition(false)
                .build();

        final Collection<Operation> opList = Lists.newArrayList(getElements);

        // When / Then
        try {
            filter.updateOperations(opList);
            fail("Exception expected");
        } catch (final IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Unable to update operations - there are not enough operations to set \"otherwise\""));
        }
    }

    @Test
    public void shouldThrowErrorForTryingToUpdateOperationsWithTooManyOps() {
        // Given
        final GetElements getElements = new GetElements.Builder()
                .input(new EntitySeed("2"))
                .build();

        final GetAllElements getAllElements = new GetAllElements();

        final Limit limit = new Limit(5);

        final If<Object, Object> filter = new If.Builder<>()
                .build();

        final Collection<Operation> opList = Lists.newArrayList(getElements, getAllElements, limit);

        // When / Then
        try {
            filter.updateOperations(opList);
            fail("Exception expected");
        } catch (final IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Unable to update operations - there are too many operations: 3"));
        }
    }
}
