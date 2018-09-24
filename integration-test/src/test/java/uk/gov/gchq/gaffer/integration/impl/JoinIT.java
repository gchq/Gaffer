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
import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.function.ElementAggregator;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.util.ElementUtil;
import uk.gov.gchq.gaffer.integration.AbstractStoreIT;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.Join;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.util.join.JoinType;
import uk.gov.gchq.gaffer.operation.util.matcher.ExactMatch;
import uk.gov.gchq.gaffer.operation.util.matcher.MatchKey;
import uk.gov.gchq.gaffer.operation.util.matcher.MatchOnFields;
import uk.gov.gchq.gaffer.operation.util.merge.ElementMerge;
import uk.gov.gchq.gaffer.operation.util.merge.NoMerge;
import uk.gov.gchq.gaffer.operation.util.merge.ReduceType;
import uk.gov.gchq.gaffer.operation.util.merge.ResultsWanted;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.impl.binaryoperator.Sum;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

public class JoinIT extends AbstractStoreIT {

    final User user = new User();
    final String testField = "field1";
    final List<TestPojo> inputTestList = Arrays.asList(new TestPojo(1, 3), new TestPojo(2, 4), new TestPojo(2, 4), new TestPojo(3, 5), new TestPojo(4, 6));
    final List<TestPojo> operationTestList = Arrays.asList(new TestPojo(2, 4), new TestPojo(4, 6), new TestPojo(4, 11), new TestPojo(6, 8), new TestPojo(8, 10));

    @Override
    @Before
    public void setup() throws Exception {
        super.setup();
        addDefaultElements();
    }

    @Test
    public void shouldFullInnerJoinElementsFromLeftToRightWithFlatten() throws OperationException, NoSuchFieldException {
        final Entity entity1 = getEntity(VERTEX_PREFIXES[0] + 0);
        final Entity entity2 = getEntity(VERTEX_PREFIXES[0] + 1);
        final Entity entity3 = getEntity(VERTEX_PREFIXES[0] + 2);

        List<Element> expectedResults = Arrays.asList(entity1, entity2, entity1, entity2);

        final List<Element> inputElementList = Arrays.asList(entity1, entity2, entity3);

        final GetElements rhsGetElementsOperation = new GetElements.Builder()
                .input(new EntitySeed(VERTEX_PREFIXES[0] + 0),
                        new EntitySeed(VERTEX_PREFIXES[0] + 1))
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY)
                        .build())
                .build();

        Join<Element, Element> joinOp = new Join.Builder<Element, Element>()
                .input(inputElementList)
                .operation(rhsGetElementsOperation)
                .joinType(JoinType.FULL_INNER)
                .matchKey(MatchKey.LEFT)
                .matchMethod(new ExactMatch())
                .mergeMethod(new ElementMerge(ResultsWanted.BOTH, ReduceType.NONE, null))
                .build();

        final Iterable<? extends Element> results = graph.execute(joinOp, user);

        ElementUtil.assertElementEquals(expectedResults, results);
    }

    @Test
    public void shouldFullInnerJoinElementsFromLeftToRightWithReduceAndBothResults() throws OperationException, NoSuchFieldException {
        final Entity entity1 = getEntity(VERTEX_PREFIXES[0] + 0);
        final Entity entity2 = getEntity(VERTEX_PREFIXES[0] + 1);
        final Entity entity3 = getEntity(VERTEX_PREFIXES[0] + 2);

        List<Element> expectedResults = Arrays.asList(entity1, entity2);

        final List<Element> inputElementList = Arrays.asList(entity1, entity2, entity3);

        final GetElements rhsGetElementsOperation = new GetElements.Builder()
                .input(new EntitySeed(VERTEX_PREFIXES[0] + 0),
                        new EntitySeed(VERTEX_PREFIXES[0] + 1))
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY)
                        .build())
                .build();

        Join<Element, Element> joinOp = new Join.Builder<Element, Element>()
                .input(inputElementList)
                .operation(rhsGetElementsOperation)
                .joinType(JoinType.FULL_INNER)
                .matchKey(MatchKey.LEFT)
                .matchMethod(new ExactMatch())
                .mergeMethod(new ElementMerge(ResultsWanted.RELATED_ONLY, ReduceType.AGAINST_KEY, new ElementAggregator.Builder().select("count").execute(new Sum()).build()))
                .build();

        final Iterable<? extends Element> results = graph.execute(joinOp, user);

        ElementUtil.assertElementEquals(expectedResults, results);
    }

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
                .matchKey(MatchKey.LEFT)
                .matchMethod(new MatchOnFields(TestPojo.class.getField(testField)))
                .mergeMethod(new NoMerge())
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
                .matchKey(MatchKey.RIGHT)
                .matchMethod(new MatchOnFields(TestPojo.class.getField(testField)))
                .mergeMethod(new NoMerge())
                .build();

        final Iterable<? extends TestPojo> results = graph.execute(joinOp, user);

        assertThat(results, containsInAnyOrder(expectedResults.toArray()));
    }

    @Test
    public void shouldFullJoinFromLeftToRight() throws OperationException {
        List<Map<TestPojo, List<TestPojo>>> expectedResults = new ArrayList<>();
        expectedResults.add(ImmutableMap.of(new TestPojo(1, 3), Arrays.asList()));
        expectedResults.add(ImmutableMap.of(new TestPojo(3, 5), Arrays.asList()));
        expectedResults.add(ImmutableMap.of(new TestPojo(2, 4), Arrays.asList(new TestPojo(2, 4))));
        expectedResults.add(ImmutableMap.of(new TestPojo(2, 4), Arrays.asList(new TestPojo(2, 4))));
        expectedResults.add(ImmutableMap.of(new TestPojo(4, 6), Arrays.asList(new TestPojo(4, 6))));
        expectedResults.add(ImmutableMap.of(new TestPojo(4, 11), Arrays.asList()));
        expectedResults.add(ImmutableMap.of(new TestPojo(6, 8), Arrays.asList()));
        expectedResults.add(ImmutableMap.of(new TestPojo(8, 10), Arrays.asList()));

        Join<TestPojo, TestPojo> joinOp = new Join.Builder<TestPojo, TestPojo>()
                .input(inputTestList)
                .operation(new uk.gov.gchq.gaffer.operation.impl.Map.Builder<>().input(operationTestList).build())
                .joinType(JoinType.FULL)
                .matchKey(MatchKey.LEFT)
                .matchMethod(new ExactMatch())
                .mergeMethod(new NoMerge())
                .build();

        final Iterable<? extends TestPojo> results = graph.execute(joinOp, user);

        assertThat(results, containsInAnyOrder(expectedResults.toArray()));
    }

    @Test
    public void shouldFullJoinFromRightToLeft() throws OperationException {
        List<Map<TestPojo, List<TestPojo>>> expectedResults = new ArrayList<>();
        expectedResults.add(ImmutableMap.of(new TestPojo(6, 8), Arrays.asList()));
        expectedResults.add(ImmutableMap.of(new TestPojo(8, 10), Arrays.asList()));
        expectedResults.add(ImmutableMap.of(new TestPojo(4, 11), Arrays.asList()));
        expectedResults.add(ImmutableMap.of(new TestPojo(2, 4), Arrays.asList(new TestPojo(2, 4), new TestPojo(2, 4))));
        expectedResults.add(ImmutableMap.of(new TestPojo(4, 6), Arrays.asList(new TestPojo(4, 6))));
        expectedResults.add(ImmutableMap.of(new TestPojo(1, 3), Arrays.asList()));
        expectedResults.add(ImmutableMap.of(new TestPojo(3, 5), Arrays.asList()));

        Join<TestPojo, TestPojo> joinOp = new Join.Builder<TestPojo, TestPojo>()
                .input(inputTestList)
                .operation(new uk.gov.gchq.gaffer.operation.impl.Map.Builder<>().input(operationTestList).build())
                .joinType(JoinType.FULL)
                .matchKey(MatchKey.RIGHT)
                .matchMethod(new ExactMatch())
                .mergeMethod(new NoMerge())
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
                .matchMethod(new ExactMatch())
                .mergeMethod(new NoMerge())
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
                .matchKey(MatchKey.LEFT)
                .matchMethod(new ExactMatch())
                .mergeMethod(new NoMerge())
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
                .matchKey(MatchKey.RIGHT)
                .matchMethod(new ExactMatch())
                .mergeMethod(new NoMerge())
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
                .matchKey(MatchKey.LEFT)
                .matchMethod(new ExactMatch())
                .mergeMethod(new NoMerge())
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
                .matchKey(MatchKey.RIGHT)
                .matchMethod(new ExactMatch())
                .mergeMethod(new NoMerge())
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
