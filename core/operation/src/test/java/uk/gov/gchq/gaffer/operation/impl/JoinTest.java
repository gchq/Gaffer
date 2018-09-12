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

package uk.gov.gchq.gaffer.operation.impl;

import uk.gov.gchq.gaffer.operation.OperationTest;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.util.join.JoinType;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class JoinTest extends OperationTest<Join> {
    @Override
    public void builderShouldCreatePopulatedOperation() {
        // Given
        final Join op = getTestObject();

        // Then
        assertEquals(Arrays.asList(1, 2, 3), op.getInput());
        assertTrue(op.getOperation() instanceof GetAllElements);
        assertEquals(JoinType.INNER, op.getJoinType());
    }

    @Override
    public void shouldShallowCloneOperation() {
        // Given
        final Join op = getTestObject();

        // When
        final Join clone = op.shallowClone();

        assertNotEquals(op, clone);
        assertEquals(clone.getInput(), op.getInput());
        assertEquals(clone.getOperation(), op.getOperation());
        assertEquals(clone.getJoinType(), op.getJoinType());
        assertEquals(clone.getMatcher(), op.getMatcher());
    }

    @Override
    protected Join getTestObject() {
        return new Join.Builder<>()
                .input(Arrays.asList(1, 2, 3))
                .operation(new GetAllElements.Builder().build())
                .joinType(JoinType.INNER)
                .build();
    }
}
