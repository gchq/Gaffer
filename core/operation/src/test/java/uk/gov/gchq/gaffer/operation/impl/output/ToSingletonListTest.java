/*
 * Copyright 2018 Crown Copyright
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

package uk.gov.gchq.gaffer.operation.impl.output;

import uk.gov.gchq.gaffer.operation.OperationTest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

public class ToSingletonListTest extends OperationTest<ToSingletonList> {

    @Override
    public void builderShouldCreatePopulatedOperation() {
        // Given
        final ToSingletonList<Integer> operation = new ToSingletonList.Builder<Integer>()
                .input(1)
                .build();

        // Then
        assertTrue(operation.getInput().equals(1));
    }

    @Override
    public void shouldShallowCloneOperation() {
        // Given
        final int input = 1;
        final ToSingletonList operation = new ToSingletonList.Builder<>()
                .input(input)
                .build();

        // When
        final ToSingletonList clone = operation.shallowClone();

        // Then
        assertNotSame(operation, clone);
        assertEquals(input, clone.getInput());
    }

    @Override
    protected ToSingletonList getTestObject() {
        return new ToSingletonList();
    }
}
