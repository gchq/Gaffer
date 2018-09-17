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

package uk.gov.gchq.gaffer.integration.impl;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;
import uk.gov.gchq.gaffer.integration.AbstractStoreIT;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.Join;
import uk.gov.gchq.gaffer.operation.util.join.JoinType;
import uk.gov.gchq.gaffer.operation.util.matcher.MatchExact;
import uk.gov.gchq.gaffer.operation.util.matcher.MatchOnField;
import uk.gov.gchq.gaffer.operation.util.matcher.MatchingOnIterable;
import uk.gov.gchq.gaffer.operation.util.reducer.ReduceOn;
import uk.gov.gchq.gaffer.user.User;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

public class JoinIT extends AbstractStoreIT {

    final User user = new User();
    final List<TestPojo> inputTestList = Arrays.asList(new TestPojo(1, 3), new TestPojo(2, 4), new TestPojo(2, 4), new TestPojo(3, 5), new TestPojo(4, 6));
    final List<TestPojo> operationTestList = Arrays.asList(new TestPojo(2, 4), new TestPojo(4, 6), new TestPojo(4, 11), new TestPojo(6, 8), new TestPojo(8, 10));

    @Test
    public void shouldFullInnerJoinFromLeftToRight() throws OperationException, NoSuchFieldException {
        List<Map<TestPojo, List<TestPojo>>> expectedResults = new ArrayList<>();
        expectedResults.add(ImmutableMap.of(new TestPojo(2, 4), Arrays.asList(new TestPojo(2, 4))));
        expectedResults.add(ImmutableMap.of(new TestPojo(2, 4), Arrays.asList(new TestPojo(2, 4))));
        expectedResults.add(ImmutableMap.of(new TestPojo(4, 6), Arrays.asList(new TestPojo(4, 6), new TestPojo(4, 11))));

        Join<TestPojo, TestPojo> joinOp = new Join.Builder<TestPojo, TestPojo>()
                .input(inputTestList)
                .operation(new uk.gov.gchq.gaffer.operation.impl.Map.Builder<>().input(operationTestList).build())
                .joinType(JoinType.FULL_INNER)
                .matchingOnIterable(MatchingOnIterable.LEFT)
                .matcher(new MatchOnField(TestPojo.class.getField("field1")))
                .reducer(new ReduceOn())
                .build();

        final Iterable<? extends TestPojo> results = graph.execute(joinOp, user);

        assertThat(results, containsInAnyOrder(expectedResults.toArray()));
    }

    @Test
    public void shouldFullInnerJoinFromRightToLeft() throws OperationException, NoSuchFieldException {
        List<Map<TestPojo, List<TestPojo>>> expectedResults = new ArrayList<>();
        expectedResults.add(ImmutableMap.of(new TestPojo(2, 4), Arrays.asList(new TestPojo(2, 4), new TestPojo(2, 4))));
        expectedResults.add(ImmutableMap.of(new TestPojo(4, 6), Arrays.asList(new TestPojo(4, 6))));
        expectedResults.add(ImmutableMap.of(new TestPojo(4, 11), Arrays.asList(new TestPojo(4, 6))));

        Join<TestPojo, TestPojo> joinOp = new Join.Builder<TestPojo, TestPojo>()
                .input(inputTestList)
                .operation(new uk.gov.gchq.gaffer.operation.impl.Map.Builder<>().input(operationTestList).build())
                .joinType(JoinType.FULL_INNER)
                .matchingOnIterable(MatchingOnIterable.RIGHT)
                .matcher(new MatchOnField(TestPojo.class.getField("field1")))
                .reducer(new ReduceOn())
                .build();

        final Iterable<? extends TestPojo> results = graph.execute(joinOp, user);

        assertThat(results, containsInAnyOrder(expectedResults.toArray()));
    }

    @Test
    public void shouldFullJoinFromLeftToRight() throws OperationException {
        List<Map<TestPojo, List<TestPojo>>> expectedResults = new ArrayList<>();
        // Left with no relation to right
        expectedResults.add(ImmutableMap.of(new TestPojo(1, 3), Arrays.asList()));
        expectedResults.add(ImmutableMap.of(new TestPojo(3, 5), Arrays.asList()));
        // Left with relation to right
        expectedResults.add(ImmutableMap.of(new TestPojo(2, 4), Arrays.asList(new TestPojo(2, 4))));
        expectedResults.add(ImmutableMap.of(new TestPojo(2, 4), Arrays.asList(new TestPojo(2, 4))));
        expectedResults.add(ImmutableMap.of(new TestPojo(4, 6), Arrays.asList(new TestPojo(4, 6))));
        // Right with no relation to left
        expectedResults.add(ImmutableMap.of(new TestPojo(4, 11), Arrays.asList()));
        expectedResults.add(ImmutableMap.of(new TestPojo(6, 8), Arrays.asList()));
        expectedResults.add(ImmutableMap.of(new TestPojo(8, 10), Arrays.asList()));

        Join<TestPojo, TestPojo> joinOp = new Join.Builder<TestPojo, TestPojo>()
                .input(inputTestList)
                .operation(new uk.gov.gchq.gaffer.operation.impl.Map.Builder<>().input(operationTestList).build())
                .joinType(JoinType.FULL)
                .matchingOnIterable(MatchingOnIterable.LEFT)
                .matcher(new MatchExact())
                .reducer(new ReduceOn())
                .build();

        final Iterable<? extends TestPojo> results = graph.execute(joinOp, user);

        assertThat(results, containsInAnyOrder(expectedResults.toArray()));
    }

    @Test
    public void shouldFullJoinFromRightToLeft() throws OperationException {
        List<Map<TestPojo, List<TestPojo>>> expectedResults = new ArrayList<>();
        // Right with no relation to left
        expectedResults.add(ImmutableMap.of(new TestPojo(6, 8), Arrays.asList()));
        expectedResults.add(ImmutableMap.of(new TestPojo(8, 10), Arrays.asList()));
        expectedResults.add(ImmutableMap.of(new TestPojo(4, 11), Arrays.asList()));
        // Right with relation to left
        expectedResults.add(ImmutableMap.of(new TestPojo(2, 4), Arrays.asList(new TestPojo(2, 4), new TestPojo(2, 4))));
        expectedResults.add(ImmutableMap.of(new TestPojo(4, 6), Arrays.asList(new TestPojo(4, 6))));
        // Left with no relation to right
        expectedResults.add(ImmutableMap.of(new TestPojo(1, 3), Arrays.asList()));
        expectedResults.add(ImmutableMap.of(new TestPojo(3, 5), Arrays.asList()));

        Join<TestPojo, TestPojo> joinOp = new Join.Builder<TestPojo, TestPojo>()
                .input(inputTestList)
                .operation(new uk.gov.gchq.gaffer.operation.impl.Map.Builder<>().input(operationTestList).build())
                .joinType(JoinType.FULL)
                .matchingOnIterable(MatchingOnIterable.RIGHT)
                .matcher(new MatchExact())
                .reducer(new ReduceOn())
                .build();

        final Iterable<? extends TestPojo> results = graph.execute(joinOp, user);

        assertThat(results, containsInAnyOrder(expectedResults.toArray()));
    }

    @Test
    public void shouldFullOuterJoin() throws OperationException {
        List<Map<TestPojo, List<TestPojo>>> expectedResults = new ArrayList<>();
        expectedResults.add(ImmutableMap.of(new TestPojo(1, 3), Arrays.asList()));
        expectedResults.add(ImmutableMap.of(new TestPojo(3, 5), Arrays.asList()));
        expectedResults.add(ImmutableMap.of(new TestPojo(6, 8), Arrays.asList()));
        expectedResults.add(ImmutableMap.of(new TestPojo(8, 10), Arrays.asList()));
        expectedResults.add(ImmutableMap.of(new TestPojo(4, 11), Arrays.asList()));

        Join<TestPojo, TestPojo> joinOp = new Join.Builder<TestPojo, TestPojo>()
                .input(inputTestList)
                .operation(new uk.gov.gchq.gaffer.operation.impl.Map.Builder<>().input(operationTestList).build())
                .joinType(JoinType.FULL_OUTER)
                .matcher(new MatchExact())
                .reducer(new ReduceOn())
                .build();

        final Iterable<? extends TestPojo> results = graph.execute(joinOp, user);

        assertThat(results, containsInAnyOrder(expectedResults.toArray()));
    }

    @Test
    public void shouldOuterJoinFromLeftToRight() throws OperationException {
        List<Map<TestPojo, List<TestPojo>>> expectedResults = new ArrayList<>();
        expectedResults.add(ImmutableMap.of(new TestPojo(1, 3), Arrays.asList()));
        expectedResults.add(ImmutableMap.of(new TestPojo(3, 5), Arrays.asList()));

        Join<TestPojo, TestPojo> joinOp = new Join.Builder<TestPojo, TestPojo>()
                .input(inputTestList)
                .operation(new uk.gov.gchq.gaffer.operation.impl.Map.Builder<>().input(operationTestList).build())
                .joinType(JoinType.OUTER)
                .matchingOnIterable(MatchingOnIterable.LEFT)
                .matcher(new MatchExact())
                .reducer(new ReduceOn())
                .build();

        final Iterable<? extends TestPojo> results = graph.execute(joinOp, user);

        assertThat(results, containsInAnyOrder(expectedResults.toArray()));
    }

    @Test
    public void shouldOuterJoinFromRightToLeft() throws OperationException {
        List<Map<TestPojo, List<TestPojo>>> expectedResults = new ArrayList<>();
        expectedResults.add(ImmutableMap.of(new TestPojo(4, 11), Arrays.asList()));
        expectedResults.add(ImmutableMap.of(new TestPojo(6, 8), Arrays.asList()));
        expectedResults.add(ImmutableMap.of(new TestPojo(8, 10), Arrays.asList()));

        Join<TestPojo, TestPojo> joinOp = new Join.Builder<TestPojo, TestPojo>()
                .input(inputTestList)
                .operation(new uk.gov.gchq.gaffer.operation.impl.Map.Builder<>().input(operationTestList).build())
                .joinType(JoinType.OUTER)
                .matchingOnIterable(MatchingOnIterable.RIGHT)
                .matcher(new MatchExact())
                .reducer(new ReduceOn())
                .build();

        final Iterable<? extends TestPojo> results = graph.execute(joinOp, user);

        assertThat(results, containsInAnyOrder(expectedResults.toArray()));
    }

    @Test
    public void shouldInnerJoinFromLeftToRight() throws OperationException {
        List<Map<TestPojo, List<TestPojo>>> expectedResults = new ArrayList<>();
        expectedResults.add(ImmutableMap.of(new TestPojo(1, 3), Arrays.asList()));
        expectedResults.add(ImmutableMap.of(new TestPojo(2, 4), Arrays.asList(new TestPojo(2, 4))));
        expectedResults.add(ImmutableMap.of(new TestPojo(2, 4), Arrays.asList(new TestPojo(2, 4))));
        expectedResults.add(ImmutableMap.of(new TestPojo(3, 5), Arrays.asList()));
        expectedResults.add(ImmutableMap.of(new TestPojo(4, 6), Arrays.asList(new TestPojo(4, 6))));

        Join<TestPojo, TestPojo> joinOp = new Join.Builder<TestPojo, TestPojo>()
                .input(inputTestList)
                .operation(new uk.gov.gchq.gaffer.operation.impl.Map.Builder<>().input(operationTestList).build())
                .joinType(JoinType.INNER)
                .matchingOnIterable(MatchingOnIterable.LEFT)
                .matcher(new MatchExact())
                .reducer(new ReduceOn())
                .build();

        final Iterable<? extends TestPojo> results = graph.execute(joinOp, user);

        assertThat(results, containsInAnyOrder(expectedResults.toArray()));
    }

    @Test
    public void shouldInnerJoinFromRightToLeft() throws OperationException {
        List<Map<TestPojo, List<TestPojo>>> expectedResults = new ArrayList<>();
        expectedResults.add(ImmutableMap.of(new TestPojo(2, 4), Arrays.asList(new TestPojo(2, 4), new TestPojo(2, 4))));
        expectedResults.add(ImmutableMap.of(new TestPojo(4, 6), Arrays.asList(new TestPojo(4, 6))));
        expectedResults.add(ImmutableMap.of(new TestPojo(4, 11), Arrays.asList()));
        expectedResults.add(ImmutableMap.of(new TestPojo(6, 8), Arrays.asList()));
        expectedResults.add(ImmutableMap.of(new TestPojo(8, 10), Arrays.asList()));

        Join<TestPojo, TestPojo> joinOp = new Join.Builder<TestPojo, TestPojo>()
                .input(inputTestList)
                .operation(new uk.gov.gchq.gaffer.operation.impl.Map.Builder<>().input(operationTestList).build())
                .joinType(JoinType.INNER)
                .matchingOnIterable(MatchingOnIterable.RIGHT)
                .matcher(new MatchExact())
                .reducer(new ReduceOn())
                .build();

        final Iterable<? extends TestPojo> results = graph.execute(joinOp, user);

        assertThat(results, containsInAnyOrder(expectedResults.toArray()));
    }

    public class TestPojo {
        public Integer field1;
        public Integer field2;

        public TestPojo() {

        }

        public TestPojo(final Integer field1, final Integer field2) {
            this.field1 = field1;
            this.field2 = field2;
        }

        public Integer getField1() {
            return field1;
        }

        public void setField1(final Integer field1) {
            this.field1 = field1;
        }

        public Integer getField2() {
            return field2;
        }

        public void setField2(final Integer field2) {
            this.field2 = field2;
        }

        @Override
        public boolean equals(final Object obj) {
            if (this == obj) {
                return true;
            }

            if (null == obj || getClass() != obj.getClass()) {
                return false;
            }

            final TestPojo that = (TestPojo) obj;

            return new EqualsBuilder()
                    .append(field1, that.field1)
                    .append(field2, that.field2)
                    .isEquals();
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this)
                    .append("field1: " + field1)
                    .append("field2: " + field2)
                    .toString();
        }
    }
}
