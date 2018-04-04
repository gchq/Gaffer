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
package uk.gov.gchq.gaffer.operation.impl;

import org.junit.Test;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationTest;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.util.Conditional;
import uk.gov.gchq.koryphe.impl.predicate.Exists;

import java.util.function.Predicate;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class WhileTest extends OperationTest<While> {
    @Override
    public void builderShouldCreatePopulatedOperation() {
        // Given
        final While operation = new While.Builder<>()
                .input(new EntitySeed(1))
                .maxRepeats(10)
                .condition(true)
                .operation(new GetAdjacentIds())
                .build();

        // When / Then
        assertThat(operation.getInput(), is(notNullValue()));
        assertTrue(operation.getOperation() instanceof GetAdjacentIds);
        assertTrue(operation.isCondition());
        assertEquals(10, operation.getMaxRepeats());
    }

    @Test
    public void shouldThrowExceptionIfBothConditionalAndConditionUsedInBuilder() {
        // When / Then
        try {
            new While.Builder<>()
                    .condition(true)
                    .conditional(new Conditional())
                    .build();
        } catch (final IllegalArgumentException e) {
            assertEquals("Tried to set conditional when condition has already been configured.", e.getMessage());
        }
    }

    @Override
    public void shouldShallowCloneOperation() {
        // Given
        final EntitySeed input = new EntitySeed("E");
        final Predicate predicate = new Exists();
        final Operation delegate = new GetAdjacentIds();
        final int maxRepeats = 5;

        final While operation = new While.Builder<>()
                .input(input)
                .maxRepeats(maxRepeats)
                .conditional(predicate)
                .operation(delegate)
                .build();

        // When
        final While clone = operation.shallowClone();

        // Then
        assertNotSame(operation, clone);
        assertEquals(input, clone.getInput());
        assertEquals(maxRepeats, clone.getMaxRepeats());
    }

    @Override
    protected While getTestObject() {
        return new While.Builder<>()
                .input(new EntitySeed(2))
                .conditional(new Exists())
                .operation(new GetAdjacentIds())
                .build();
    }
}
