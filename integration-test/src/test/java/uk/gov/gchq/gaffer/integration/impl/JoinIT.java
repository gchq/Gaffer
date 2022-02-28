/*
 * Copyright 2018-2021 Crown Copyright
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

import com.google.common.collect.Lists;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.CollectionUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.function.ExtractProperty;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.util.ElementUtil;
import uk.gov.gchq.gaffer.integration.AbstractStoreIT;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.Map;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.impl.join.Join;
import uk.gov.gchq.gaffer.operation.impl.join.match.MatchKey;
import uk.gov.gchq.gaffer.operation.impl.join.methods.JoinType;
import uk.gov.gchq.gaffer.store.TestTypes;
import uk.gov.gchq.gaffer.store.operation.handler.join.match.ElementMatch;
import uk.gov.gchq.gaffer.store.operation.handler.join.match.KeyFunctionMatch;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.koryphe.impl.function.Identity;
import uk.gov.gchq.koryphe.impl.function.ToLong;
import uk.gov.gchq.koryphe.tuple.MapTuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class JoinIT extends AbstractStoreIT {
    private List<Element> inputElements = new ArrayList<>(Arrays.asList(getJoinEntity(TestGroups.ENTITY_3, 1), getJoinEntity(TestGroups.ENTITY_3, 2), getJoinEntity(TestGroups.ENTITY_3, 3), getJoinEntity(TestGroups.ENTITY_3, 4), getJoinEntity(TestGroups.ENTITY_3, 6)));
    private List<Element> innerJoinElements = new ArrayList<>(Arrays.asList(getJoinEntity(TestGroups.ENTITY_3, 1), getJoinEntity(TestGroups.ENTITY_3, 2), getJoinEntity(TestGroups.ENTITY_3, 3), getJoinEntity(TestGroups.ENTITY_3, 4)));

    private final GetElements rhsGetElementsOperation = new GetElements.Builder()
            .input(new EntitySeed(VERTEX_PREFIXES[0] + 0))
            .view(new View.Builder()
                    .entity(TestGroups.ENTITY_3)
                    .build())
            .build();

    @Override
    public void _setup() throws Exception {
        addJoinEntityElements(TestGroups.ENTITY_3);
    }

    @Test
    public void shouldRightSideInnerJoinUsingKeyFunctionMatch() throws OperationException {

        // Given
        final Map map = new Map.Builder<>().input(Lists.newArrayList(4L)).first(new Identity()).build();

        final ArrayList<Entity> input = Lists.newArrayList(
                getJoinEntity(TestGroups.ENTITY, 4),
                getJoinEntity(TestGroups.ENTITY_2, 4),
                getJoinEntity(TestGroups.ENTITY_3, 4)
        );

        final Join<Object> rightJoin = new Join.Builder<>()
                .flatten(true)
                .matchKey(MatchKey.RIGHT)
                .joinType(JoinType.INNER)
                .operation(map)
                .matchMethod(new KeyFunctionMatch(new Identity(), new ExtractProperty("count")))
                .input(input)
                .build();
        // When
        final Iterable<? extends MapTuple> rightResults = graph.execute(rightJoin, user);

        final List<java.util.Map> loadedResults = new ArrayList<>();
        rightResults.forEach(e -> loadedResults.add(e.getValues()));

        // Then

        assertThat(loadedResults).hasSize(3);
        assertThat(loadedResults.get(0)).containsEntry(MatchKey.LEFT.name(), getJoinEntity(TestGroups.ENTITY, 4))
                .containsEntry(MatchKey.RIGHT.name(), 4L);
        assertThat(loadedResults.get(1)).containsEntry(MatchKey.LEFT.name(), getJoinEntity(TestGroups.ENTITY_2, 4))
                .containsEntry(MatchKey.RIGHT.name(), 4L);
        assertThat(loadedResults.get(2)).containsEntry(MatchKey.LEFT.name(), getJoinEntity(TestGroups.ENTITY_3, 4))
                .containsEntry(MatchKey.RIGHT.name(), 4L);
    }

    @Test
    public void shouldLeftSideOuterJoinUsingKeyFunctionMatch() throws OperationException {

        // Given
        final Map map = new Map.Builder<>().input(Lists.newArrayList(4L, 1L, 2L)).first(new Identity()).build();

        final ArrayList<Entity> input = Lists.newArrayList(
                getJoinEntity(TestGroups.ENTITY, 4),
                getJoinEntity(TestGroups.ENTITY_2, 4),
                getJoinEntity(TestGroups.ENTITY_3, 2),
                getJoinEntity(TestGroups.ENTITY_4, 5),
                getJoinEntity(TestGroups.ENTITY_4, 6),
                getJoinEntity(TestGroups.ENTITY_5, 5)
        );

        final Join<Object> leftJoin = new Join.Builder<>()
                .flatten(true)
                .matchKey(MatchKey.LEFT)
                .joinType(JoinType.OUTER)
                .operation(map)
                .matchMethod(new KeyFunctionMatch(new ExtractProperty("count"), new ToLong()))
                .input(input)
                .build();
        // When
        final Iterable<? extends MapTuple> results = graph.execute(leftJoin, user);
        final List<java.util.Map> loadedResults = new ArrayList<>();
        results.forEach(e -> loadedResults.add(e.getValues()));

        // Then
        assertThat(loadedResults).hasSize(3);
        assertThat(loadedResults.get(0))
                .containsEntry(MatchKey.LEFT.name(), getJoinEntity(TestGroups.ENTITY_4, 5))
                .containsEntry(MatchKey.RIGHT.name(), null);
        assertThat(loadedResults.get(1))
                .containsEntry(MatchKey.LEFT.name(), getJoinEntity(TestGroups.ENTITY_4, 6))
                .containsEntry(MatchKey.RIGHT.name(), null);
        assertThat(loadedResults.get(2))
                .containsEntry(MatchKey.LEFT.name(), getJoinEntity(TestGroups.ENTITY_5, 5))
                .containsEntry(MatchKey.RIGHT.name(), null);
    }

    @Test
    public void shouldRightSideFullJoinUsingKeyFunctionMatch() throws OperationException {

        // Given
        final Map map = new Map.Builder<>().input(Lists.newArrayList(2L, 1L, 2L, 3L)).first(new Identity()).build();

        final ArrayList<Entity> input = Lists.newArrayList(
                getJoinEntity(TestGroups.ENTITY, 1),
                getJoinEntity(TestGroups.ENTITY_2, 2),
                getJoinEntity(TestGroups.ENTITY_2, 2),
                getJoinEntity(TestGroups.ENTITY_5, 5)
        );

        final Join<Object> leftJoin = new Join.Builder<>()
                .flatten(false)
                .matchKey(MatchKey.RIGHT)
                .joinType(JoinType.FULL)
                .operation(map)
                .matchMethod(new KeyFunctionMatch(new ToLong(), new ExtractProperty("count")))
                .input(input)
                .build();
        // When
        final Iterable<? extends MapTuple> results = graph.execute(leftJoin, user);
        final List<java.util.Map> loadedResults = new ArrayList<>();
        results.forEach(e -> loadedResults.add(e.getValues()));

        // Then

        assertThat(loadedResults).hasSize(4);

        assertThat(loadedResults.get(0)).containsEntry(MatchKey.LEFT.name(), Lists.newArrayList(getJoinEntity(TestGroups.ENTITY_2, 2), getJoinEntity(TestGroups.ENTITY_2, 2)))
                .containsEntry(MatchKey.RIGHT.name(), 2L);

        assertThat(loadedResults.get(1)).containsEntry(MatchKey.LEFT.name(), Lists.newArrayList(getJoinEntity(TestGroups.ENTITY, 1)))
                .containsEntry(MatchKey.RIGHT.name(), 1L);

        assertThat(loadedResults.get(2)).containsEntry(MatchKey.LEFT.name(), Lists.newArrayList(getJoinEntity(TestGroups.ENTITY_2, 2), getJoinEntity(TestGroups.ENTITY_2, 2)))
                .containsEntry(MatchKey.RIGHT.name(), 2L);

        assertThat(loadedResults.get(3)).containsEntry(MatchKey.LEFT.name(), Lists.newArrayList())
                .containsEntry(MatchKey.RIGHT.name(), 3L);
    }

    @Test
    public void testNestedViewCompletedIfNotSupplied() throws Exception {
        // Given
        Join<Element> joinOp = new Join.Builder<Element>()
                .input(inputElements)
                .operation(new GetAllElements())
                .joinType(JoinType.INNER)
                .matchKey(MatchKey.LEFT)
                .matchMethod(new ElementMatch(TestPropertyNames.COUNT))
                .build();

        // When / Then - no exceptions
        graph.execute(joinOp, getUser());
    }

    @Test
    public void shouldLeftKeyFullJoin() throws OperationException {
        // Given
        final List<Element> expectedResults = new ArrayList<>(inputElements);

        Join<Element> joinOp = new Join.Builder<Element>()
                .input(inputElements)
                .operation(rhsGetElementsOperation)
                .joinType(JoinType.FULL)
                .matchKey(MatchKey.LEFT)
                .matchMethod(new ElementMatch(TestPropertyNames.COUNT))
                .flatten(false)
                .build();

        // When
        final Iterable<? extends MapTuple> results = graph.execute(joinOp, getUser());

        // Then
        assertKeysExist(expectedResults, results, MatchKey.LEFT);
    }

    @Test
    public void shouldRightKeyFullJoin() throws OperationException {
        // Given
        final List<Element> expectedResults = innerJoinElements;
        expectedResults.add(getJoinEntity(TestGroups.ENTITY_3, 8));

        Join<Element> joinOp = new Join.Builder<Element>()
                .input(inputElements)
                .operation(rhsGetElementsOperation)
                .joinType(JoinType.FULL)
                .matchKey(MatchKey.RIGHT)
                .matchMethod(new ElementMatch(TestPropertyNames.COUNT))
                .flatten(false)
                .build();

        // When
        final Iterable<? extends MapTuple> results = graph.execute(joinOp, getUser());

        // Then
        assertKeysExist(expectedResults, results, MatchKey.RIGHT);
    }

    @Test
    public void shouldLeftKeyOuterJoin() throws OperationException {
        // Given
        final List<Element> expectedResults = new ArrayList<>(Arrays.asList(getJoinEntity(TestGroups.ENTITY_3, 6)));

        Join<Element> joinOp = new Join.Builder<Element>()
                .input(inputElements)
                .operation(rhsGetElementsOperation)
                .joinType(JoinType.OUTER)
                .matchKey(MatchKey.LEFT)
                .flatten(false)
                .matchMethod(new ElementMatch(TestPropertyNames.COUNT))
                .build();

        // When
        final Iterable<? extends MapTuple> results = graph.execute(joinOp, getUser());

        // Then
        assertKeysExist(expectedResults, results, MatchKey.LEFT);
    }

    @Test
    public void shouldRightKeyOuterJoin() throws OperationException {
        // Given
        final List<Element> expectedResults = new ArrayList<>(Arrays.asList(getJoinEntity(TestGroups.ENTITY_3, 8)));

        Join<Element> joinOp = new Join.Builder<Element>()
                .input(inputElements)
                .operation(rhsGetElementsOperation)
                .joinType(JoinType.OUTER)
                .matchKey(MatchKey.RIGHT)
                .flatten(false)
                .matchMethod(new ElementMatch(TestPropertyNames.COUNT))
                .build();

        // When
        final Iterable<? extends MapTuple> results = graph.execute(joinOp, getUser());

        // Then
        assertKeysExist(expectedResults, results, MatchKey.RIGHT);
    }

    @Test
    public void shouldLeftKeyInnerJoin() throws OperationException {
        // Given
        final List<Element> expectedResults = innerJoinElements;

        Join<Element> joinOp = new Join.Builder<Element>()
                .input(inputElements)
                .operation(rhsGetElementsOperation)
                .joinType(JoinType.INNER)
                .matchKey(MatchKey.LEFT)
                .flatten(false)
                .matchMethod(new ElementMatch(TestPropertyNames.COUNT))
                .build();

        // When
        final Iterable<? extends MapTuple> results = graph.execute(joinOp, getUser());

        // Then
        assertKeysExist(expectedResults, results, MatchKey.LEFT);
    }

    @Test
    public void shouldRightKeyInnerJoin() throws OperationException {
        // Given
        final List<Element> expectedResults = innerJoinElements;

        Join<Element> joinOp = new Join.Builder<Element>()
                .input(inputElements)
                .operation(rhsGetElementsOperation)
                .joinType(JoinType.INNER)
                .matchKey(MatchKey.RIGHT)
                .matchMethod(new ElementMatch(TestPropertyNames.COUNT))
                .flatten(false)
                .build();

        // When
        final Iterable<? extends MapTuple> results = graph.execute(joinOp, getUser());

        // Then
        assertKeysExist(expectedResults, results, MatchKey.RIGHT);
    }

    private void assertKeysExist(final Iterable<Element> expected, final Iterable<? extends MapTuple> actual, final MatchKey matchKey) {
        final List<Element> joinedKeys = new ArrayList<>();

        for (final MapTuple result : actual) {
            joinedKeys.add((Element) result.get(matchKey.name()));
        }

        ElementUtil.assertElementEquals(expected, joinedKeys);

    }

    @Override
    protected Schema createSchema() {
        return new Schema.Builder().merge(createDefaultSchema())
                .entity(TestGroups.ENTITY_3, new SchemaEntityDefinition.Builder()
                        .vertex(TestTypes.ID_STRING)
                        .property(TestPropertyNames.COUNT, TestTypes.PROP_COUNT)
                        .property(TestPropertyNames.SET, TestTypes.PROP_SET_STRING)
                        .aggregate(false)
                        .build())
                .build();
    }

    private void addJoinEntityElements(final String group) {
        for (int i = 1; i <= 4; i++) {
            final Entity entity = getJoinEntity(group, i);
            try {
                graph.execute(new AddElements.Builder().input(entity).build(), getUser());
            } catch (final OperationException e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }
        try {
            graph.execute(new AddElements.Builder().input(getJoinEntity(group, 8)).build(), getUser());
        } catch (final OperationException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private Entity getJoinEntity(final String group, final Integer countProperty) {
        return new Entity.Builder()
                .group(group)
                .vertex(VERTEX_PREFIXES[0] + 0)
                .property(TestPropertyNames.SET, CollectionUtil.treeSet("3"))
                .property(TestPropertyNames.COUNT, Long.parseLong(countProperty.toString()))
                .build();
    }
}
