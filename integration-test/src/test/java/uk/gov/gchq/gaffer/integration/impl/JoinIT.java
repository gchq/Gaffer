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

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;
import uk.gov.gchq.gaffer.integration.AbstractStoreIT;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.Join;
import uk.gov.gchq.gaffer.operation.util.join.JoinType;
import uk.gov.gchq.gaffer.operation.util.matcher.MatchExact;
import uk.gov.gchq.gaffer.operation.util.matcher.MatchOn;
import uk.gov.gchq.gaffer.operation.util.reducer.ReduceOn;
import uk.gov.gchq.gaffer.user.User;

import java.util.Arrays;
import java.util.List;

public class JoinIT extends AbstractStoreIT {

    final User user = new User();
    final List<TestPojo> inputTestList = Arrays.asList(new TestPojo(1, 3), new TestPojo(2, 4), new TestPojo(2, 4), new TestPojo(3, 5), new TestPojo(4, 6));
    final List<TestPojo> operationTestList = Arrays.asList(new TestPojo(2, 4), new TestPojo(4, 6), new TestPojo(4, 11), new TestPojo(6, 8), new TestPojo(8, 10));

    @Test
    public void shouldInnerJoinTwoSimpleLists() throws OperationException, NoSuchFieldException {
        Join<TestPojo, TestPojo> joinOp = new Join.Builder<TestPojo, TestPojo>()
                .input(inputTestList)
                .operation(new uk.gov.gchq.gaffer.operation.impl.Map.Builder<>().input(operationTestList).build())
                .joinType(JoinType.INNER)
                .matcher(new MatchOn(TestPojo.class.getField("field1")))
                .reducer(new ReduceOn())
                .build();

        final Iterable<? extends TestPojo> results = graph.execute(joinOp, user);
    }

    @Test
    public void shouldFullOuterJoinTwoSimpleLists() throws OperationException {
        Join<TestPojo, TestPojo> joinOp = new Join.Builder<TestPojo, TestPojo>()
                .input(inputTestList)
                .operation(new uk.gov.gchq.gaffer.operation.impl.Map.Builder<>().input(operationTestList).build())
                .joinType(JoinType.FULL_OUTER)
                .matcher(new MatchExact())
                .reducer(new ReduceOn())
                .build();

        final Iterable<? extends TestPojo> results = graph.execute(joinOp, user);
    }

    /*
    //@Test
    public void shouldLeftOuterJoinTwoSimpleLists() throws OperationException {
        Join<Integer, Integer> joinOp = new Join.Builder<Integer, Integer>()
                .input(Arrays.asList(1, 2, 3, 4))
                .operation(new uk.gov.gchq.gaffer.operation.impl.Map.Builder<>().input(Arrays.asList(2, 4, 6, 8)).build())
                .joinType(JoinType.LEFT_OUTER)
                .matcher(new MatchExact())
                .reducer(new ReduceOn())
                .build();

        final Iterable<? extends Integer> results = graph.execute(joinOp, user);

        for (Integer i : results) {
            System.out.println("Left outer : " + i);
        }
        System.out.println();
        System.out.println();
        System.out.println();
    }

    //@Test
    public void shouldRightOuterJoinTwoSimpleLists() throws OperationException {
        Join<Integer, Integer> joinOp = new Join.Builder<Integer, Integer>()
                .input(Arrays.asList(1, 2, 3, 4))
                .operation(new uk.gov.gchq.gaffer.operation.impl.Map.Builder<>().input(Arrays.asList(2, 4, 6, 8)).build())
                .joinType(JoinType.RIGHT_OUTER)
                .matcher(new MatchExact())
                .reducer(new ReduceOn())
                .build();

        final Iterable<? extends Integer> results = graph.execute(joinOp, user);

        for (Integer i : results) {
            System.out.println("Right outer : " + i);
        }
        System.out.println();
        System.out.println();
        System.out.println();
    }

    //@Test
    public void shouldLeftInnerJoinTwoSimpleLists() throws OperationException {
        Join<Integer, Integer> joinOp = new Join.Builder<Integer, Integer>()
                .input(Arrays.asList(1, 2, 3, 4))
                .operation(new uk.gov.gchq.gaffer.operation.impl.Map.Builder<>().input(Arrays.asList(2, 4, 6, 8)).build())
                .joinType(JoinType.LEFT_INNER)
                .matcher(new MatchExact())
                .reducer(new ReduceOn())
                .build();

        final Iterable<? extends Integer> results = graph.execute(joinOp, user);

        for (Integer i : results) {
            System.out.println("Left inner : " + i);
        }
        System.out.println();
        System.out.println();
        System.out.println();
    }

    //@Test
    public void shouldRightInnerJoinTwoSimpleLists() throws OperationException {
        Join<Integer, Integer> joinOp = new Join.Builder<Integer, Integer>()
                .input(Arrays.asList(1, 2, 3, 4))
                .operation(new uk.gov.gchq.gaffer.operation.impl.Map.Builder<>().input(Arrays.asList(2, 4, 6, 8)).build())
                .joinType(JoinType.RIGHT_INNER)
                .matcher(new MatchExact())
                .reducer(new ReduceOn())
                .build();

        final Iterable<? extends Integer> results = graph.execute(joinOp, user);

        for (Integer i : results) {
            System.out.println("Right inner : " + i);
        }
        System.out.println();
        System.out.println();
        System.out.println();
    }

    //@Test
    public void shouldFullJoinTwoSimpleLists() throws OperationException {
        Join<Integer, Integer> joinOp = new Join.Builder<Integer, Integer>()
                .input(Arrays.asList(1, 2, 3, 4))
                .operation(new uk.gov.gchq.gaffer.operation.impl.Map.Builder<>().input(Arrays.asList(2, 4, 6, 8)).build())
                .joinType(JoinType.FULL)
                .matcher(new MatchExact())
                .reducer(new ReduceOn())
                .build();

        final Iterable<? extends Integer> results = graph.execute(joinOp, user);

        for (Integer i : results) {
            System.out.println("full : " + i);
        }
        System.out.println();
        System.out.println();
        System.out.println();
    }
**/

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
