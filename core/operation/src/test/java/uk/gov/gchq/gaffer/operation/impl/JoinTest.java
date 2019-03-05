/*
 * Copyright 2018-2019 Crown Copyright
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
import uk.gov.gchq.gaffer.operation.OperationTest;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.join.Join;
import uk.gov.gchq.gaffer.operation.impl.join.match.Match;
import uk.gov.gchq.gaffer.operation.impl.join.match.MatchKey;
import uk.gov.gchq.gaffer.operation.impl.join.methods.JoinType;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class JoinTest extends OperationTest<Join> {
    @Override
    public void builderShouldCreatePopulatedOperation() {
        // Given
        final Join op = new Join.Builder<>()
                .input(Arrays.asList(1, 2, 3))
                .operation(new GetAllElements.Builder().build())
                .matchMethod(new TestMatchImpl())
                .matchKey(MatchKey.LEFT)
                .joinType(JoinType.INNER)
                .flatten(false)
                .collectionLimit(10)
                .build();;

        // Then
        assertEquals(Arrays.asList(1, 2, 3), op.getInput());
        assertTrue(op.getOperation() instanceof GetAllElements);
        assertEquals(JoinType.INNER, op.getJoinType());
        assertTrue(op.getMatchMethod() instanceof Match);
        assertEquals(MatchKey.LEFT, op.getMatchKey());
        assertTrue(op.isFlatten() instanceof Boolean);
        assertTrue(op.getCollectionLimit().equals(10));
    }

    @Override
    public void shouldShallowCloneOperation() {
        // Given
        final Join op = new Join.Builder<>()
                .input(Arrays.asList(1, 2, 3))
                .operation(new GetAllElements.Builder().build())
                .matchMethod(new TestMatchImpl())
                .matchKey(MatchKey.LEFT)
                .joinType(JoinType.INNER)
                .flatten(false)
                .collectionLimit(10)
                .build();

        // When
        final Join clone = op.shallowClone();

        assertNotEquals(op, clone);
        assertEquals(clone.getInput(), op.getInput());
        assertEquals(clone.getOperation(), op.getOperation());
        assertEquals(clone.getJoinType(), op.getJoinType());
        assertEquals(clone.getMatchMethod(), op.getMatchMethod());
    }

    @Override
    protected Join getTestObject() {
        return new Join();
    }

    /**
     * Copy of the ElementMatch class using the count property to match by.
     */
    public static class TestMatchImpl implements Match {
        private List matches;

        @Override
        public void init(final Iterable matchCandidates) {
            matches = Lists.newArrayList(matchCandidates);
        }

        @Override
        public List matching(final Object unused) {
            return matches;
        }
    }

}
