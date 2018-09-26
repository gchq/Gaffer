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

import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.CollectionUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.function.ElementAggregator;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.util.ElementUtil;
import uk.gov.gchq.gaffer.integration.AbstractStoreIT;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.Join;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.util.join.JoinType;
import uk.gov.gchq.gaffer.operation.util.match.ElementMatch;
import uk.gov.gchq.gaffer.operation.util.match.MatchKey;
import uk.gov.gchq.gaffer.operation.util.merge.ElementMerge;
import uk.gov.gchq.gaffer.operation.util.merge.ReduceType;
import uk.gov.gchq.gaffer.operation.util.merge.ResultsWanted;
import uk.gov.gchq.koryphe.impl.binaryoperator.Sum;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class JoinIT extends AbstractStoreIT {
    private List<Element> inputElements = new ArrayList<>(Arrays.asList(getJoinEntity(1), getJoinEntity(2), getJoinEntity(3), getJoinEntity(4), getJoinEntity(6)));

    private final GetElements rhsGetElementsOperation = new GetElements.Builder()
            .input(new EntitySeed(VERTEX_PREFIXES[0] + 0))
            .view(new View.Builder()
                    .entity(TestGroups.ENTITY_3)
                    .build())
            .build();

    @Override
    @Before
    public void setup() throws Exception {
        super.setup();
        addDefaultElements();
        addJoinEntityElements();
    }

    @Test
    public void shouldLeftKeyFullInnerJoinWithFlattenGettingKeys() throws OperationException {
        // Given
        final List<Element> expectedElements = Arrays.asList(getJoinEntity(1), getJoinEntity(2), getJoinEntity(3), getJoinEntity(4));

        Join<Element, Element> joinOp = new Join.Builder<Element, Element>()
                .input(inputElements)
                .operation(rhsGetElementsOperation)
                .joinType(JoinType.FULL_INNER)
                .matchKey(MatchKey.LEFT)
                .matchMethod(new ElementMatch("count"))
                .mergeMethod(new ElementMerge(ResultsWanted.KEY_ONLY, ReduceType.NONE, null))
                .build();

        // When
        final Iterable<? extends Element> results = graph.execute(joinOp, getUser());

        // Then
        ElementUtil.assertElementEquals(expectedElements, results);
    }

    @Test
    public void shouldRightKeyFullInnerJoinWithFlattenGettingKeys() throws OperationException, NoSuchFieldException {
        // Given
        final List<Element> expectedResults = Arrays.asList(getJoinEntity(1), getJoinEntity(2), getJoinEntity(3), getJoinEntity(4));

        Join<Element, Element> joinOp = new Join.Builder<Element, Element>()
                .input(inputElements)
                .operation(rhsGetElementsOperation)
                .joinType(JoinType.FULL_INNER)
                .matchKey(MatchKey.RIGHT)
                .matchMethod(new ElementMatch("count"))
                .mergeMethod(new ElementMerge(ResultsWanted.KEY_ONLY, ReduceType.NONE, null))
                .build();

        // When
        final Iterable<? extends Element> results = graph.execute(joinOp, getUser());

        // Then
        ElementUtil.assertElementEquals(expectedResults, results);
    }

    @Test
    public void shouldLeftKeyFullInnerJoinWithReduceGettingRelated() throws OperationException {
        // Get Full inner, grouped by count, then merge, get related only, reducing against the key, aggregated based on the count
        // Given
        final Entity expectedEntity = getJoinEntity(18);
        final List<Element> expectedElements = new ArrayList<>(Arrays.asList(expectedEntity, expectedEntity, expectedEntity, expectedEntity, expectedEntity));

        Join<Element, Element> joinOp = new Join.Builder<Element, Element>()
                .input(inputElements)
                .operation(rhsGetElementsOperation)
                .joinType(JoinType.FULL_INNER)
                .matchKey(MatchKey.LEFT)
                .matchMethod(new ElementMatch())
                .mergeMethod(new ElementMerge(ResultsWanted.RELATED_ONLY, ReduceType.AGAINST_KEY, new ElementAggregator.Builder().select("count").execute(new Sum()).build()))
                .build();

        // When
        final Iterable<? extends Element> results = graph.execute(joinOp, getUser());

        // Then
        ElementUtil.assertElementEquals(expectedElements, results);
    }

    @Test
    public void shouldLeftKeyFullJoin() throws OperationException {
        // Given
        final List<Element> expectedResults = inputElements;

        Join<Element, Element> joinOp = new Join.Builder<Element, Element>()
                .input(inputElements)
                .operation(rhsGetElementsOperation)
                .joinType(JoinType.FULL)
                .matchKey(MatchKey.LEFT)
                .matchMethod(new ElementMatch())
                .mergeMethod(new ElementMerge(ResultsWanted.KEY_ONLY, ReduceType.NONE, null))
                .build();

        // When
        final Iterable<? extends Element> results = graph.execute(joinOp, getUser());

        // Then
        ElementUtil.assertElementEquals(expectedResults, results);
    }

    @Test
    public void shouldRightKeyFullJoin() throws OperationException {
        // Given
        final List<Element> expectedResults = new ArrayList<>();

        Join<Element, Element> joinOp = new Join.Builder<Element, Element>()
                .input(inputElements)
                .operation(rhsGetElementsOperation)
                .joinType(JoinType.FULL_INNER)
                .matchKey(MatchKey.RIGHT)
                .matchMethod(new ElementMatch())
                .mergeMethod(new ElementMerge(ResultsWanted.KEY_ONLY, ReduceType.NONE, null))
                .build();

        // When
        final Iterable<? extends Element> results = graph.execute(joinOp, getUser());

        // Then
        ElementUtil.assertElementEquals(expectedResults, results);
    }

    @Test
    public void shouldFullOuterJoin() throws OperationException {
        // Given
        final List<Element> expectedResults = new ArrayList<>();

        Join<Element, Element> joinOp = new Join.Builder<Element, Element>()
                .input(inputElements)
                .operation(rhsGetElementsOperation)
                .joinType(JoinType.FULL_OUTER)
                .matchMethod(new ElementMatch())
                .mergeMethod(new ElementMerge(ResultsWanted.KEY_ONLY, ReduceType.NONE, null))
                .build();

        // When
        final Iterable<? extends Element> results = graph.execute(joinOp, getUser());

        // Then
        ElementUtil.assertElementEquals(expectedResults, results);
    }

    @Test
    public void shouldLeftKeyOuterJoin() throws OperationException {
        // Given
        final List<Element> expectedResults = new ArrayList<>();

        Join<Element, Element> joinOp = new Join.Builder<Element, Element>()
                .input(inputElements)
                .operation(rhsGetElementsOperation)
                .joinType(JoinType.OUTER)
                .matchKey(MatchKey.LEFT)
                .matchMethod(new ElementMatch())
                .mergeMethod(new ElementMerge(ResultsWanted.KEY_ONLY, ReduceType.NONE, null))
                .build();

        // When
        final Iterable<? extends Element> results = graph.execute(joinOp, getUser());

        // Then
        ElementUtil.assertElementEquals(expectedResults, results);
    }

    @Test
    public void shouldRightKeyOuterJoin() throws OperationException {
        // Given
        final List<Element> expectedResults = new ArrayList<>();

        Join<Element, Element> joinOp = new Join.Builder<Element, Element>()
                .input(inputElements)
                .operation(rhsGetElementsOperation)
                .joinType(JoinType.OUTER)
                .matchKey(MatchKey.RIGHT)
                .matchMethod(new ElementMatch())
                .mergeMethod(new ElementMerge(ResultsWanted.KEY_ONLY, ReduceType.NONE, null))
                .build();

        // When
        final Iterable<? extends Element> results = graph.execute(joinOp, getUser());

        // Then
        ElementUtil.assertElementEquals(expectedResults, results);
    }

    @Test
    public void shouldLeftKeyInnerJoin() throws OperationException {
        // Given
        final List<Element> expectedResults = new ArrayList<>();

        Join<Element, Element> joinOp = new Join.Builder<Element, Element>()
                .input(inputElements)
                .operation(rhsGetElementsOperation)
                .joinType(JoinType.INNER)
                .matchKey(MatchKey.LEFT)
                .matchMethod(new ElementMatch())
                .mergeMethod(new ElementMerge(ResultsWanted.KEY_ONLY, ReduceType.NONE, null))
                .build();

        // When
        final Iterable<? extends Element> results = graph.execute(joinOp, getUser());

        // Then
        ElementUtil.assertElementEquals(expectedResults, results);
    }

    @Test
    public void shouldRightKeyInnerJoin() throws OperationException {
        // Given
        final List<Element> expectedResults = new ArrayList<>();

        Join<Element, Element> joinOp = new Join.Builder<Element, Element>()
                .input(inputElements)
                .operation(rhsGetElementsOperation)
                .joinType(JoinType.INNER)
                .matchKey(MatchKey.RIGHT)
                .matchMethod(new ElementMatch())
                .mergeMethod(new ElementMerge(ResultsWanted.KEY_ONLY, ReduceType.NONE, null))
                .build();

        // When
        final Iterable<? extends Element> results = graph.execute(joinOp, getUser());

        // Then
        ElementUtil.assertElementEquals(expectedResults, results);
    }

    private void addJoinEntityElements() {
        for (int i = 1; i <= 4; i++) {
            final Entity entity = getJoinEntity(i);
            try {
                graph.execute(new AddElements.Builder().input(entity).build(), getUser());
            } catch (final OperationException e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }
        try {
            graph.execute(new AddElements.Builder().input(getJoinEntity(8)).build(), getUser());
        } catch (final OperationException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private Entity getJoinEntity(final Integer countProperty) {
        return new Entity.Builder()
                .group(TestGroups.ENTITY_3)
                .vertex(VERTEX_PREFIXES[0] + 0)
                .property(TestPropertyNames.SET, CollectionUtil.treeSet("3"))
                .property(TestPropertyNames.COUNT, Long.parseLong(countProperty.toString()))
                .build();
    }
}
