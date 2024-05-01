/*
 * Copyright 2024 Crown Copyright
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

package uk.gov.gchq.gaffer.tinkerpop;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.tinkerpop.util.GafferPopFederatedTestUtil;

import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.atIndex;
import static org.assertj.core.api.Assertions.entry;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopTestUtil.AUTH_1;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopTestUtil.AUTH_2;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopTestUtil.TEST_CONFIGURATION_1;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopTestUtil.USER_ID;

public class GafferPopFederatedIT {
    private Graph federatedGraph;
    private GafferPopGraph gafferPopGraph;

    public static final String VERTEX_PERSON_1 = "p1";
    public static final String VERTEX_PERSON_2 = "p2";
    public static final String VERTEX_SOFTWARE_1 = "s1";
    public static final String VERTEX_SOFTWARE_2 = "s2";
    public static final String SOFTWARE_GROUP = "software";
    public static final String PERSON_GROUP = "person";
    public static final String CREATED_EDGE_GROUP = "created";
    public static final String NAME_PROPERTY = "name";
    public static final String WEIGHT_PROPERTY = "weight";
    public static final Map<Object, Object> EXPECTED_SOFTWARE_1_VERTEX_MAP = Stream.of(
            new SimpleEntry<>(T.id, VERTEX_SOFTWARE_1),
            new SimpleEntry<>(T.label, SOFTWARE_GROUP),
            new SimpleEntry<>(NAME_PROPERTY, "software1Name"))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    public static final Map<Object, Object> EXPECTED_SOFTWARE_2_VERTEX_MAP = Stream.of(
            new SimpleEntry<>(T.id, VERTEX_SOFTWARE_2),
            new SimpleEntry<>(T.label, SOFTWARE_GROUP),
            new SimpleEntry<>(NAME_PROPERTY, "software2Name"))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    public static final Map<Object, Object> EXPECTED_PERSON_1_VERTEX_MAP = Stream.of(
            new SimpleEntry<>(T.id, VERTEX_PERSON_1),
            new SimpleEntry<>(T.label, PERSON_GROUP),
            new SimpleEntry<>(NAME_PROPERTY, "person1Name"))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    public static final Map<Object, Object> EXPECTED_PERSON_2_VERTEX_MAP = Stream.of(
            new SimpleEntry<>(T.id, VERTEX_PERSON_2),
            new SimpleEntry<>(T.label, PERSON_GROUP),
            new SimpleEntry<>(NAME_PROPERTY, "person2Name"))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    public static final Map<Object, Object> EXPECTED_PERSON_3_VERTEX_MAP = Stream.of(
            new SimpleEntry<>(T.id, "p3"),
            new SimpleEntry<>(T.label, PERSON_GROUP),
            new SimpleEntry<>(NAME_PROPERTY, "person3Name"))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    @BeforeEach
    public void setUp() throws Exception {
        federatedGraph = GafferPopFederatedTestUtil.setUpFederatedGraph(GafferPopFederatedIT.class);
        gafferPopGraph = GafferPopGraph.open(TEST_CONFIGURATION_1, federatedGraph);
    }

    @AfterEach
    public void tearDown() {
        CacheServiceLoader.shutdown();
    }

    @Test
    void shouldConstructFederatedGafferPopGraph() {
        // When
        final Map<String, Object> variables = gafferPopGraph.variables().asMap();

        // Then
        assertThat(variables).contains(
                entry("userId", USER_ID),
                entry("dataAuths", new String[] {AUTH_1, AUTH_2}));
    }

    @Test
    void shouldGetVerticesById() {
        // Given
        GraphTraversalSource g = gafferPopGraph.traversal();

        // When
        List<Map<Object, Object>> vertex1 = g.V(VERTEX_PERSON_1).elementMap().toList();
        List<Map<Object, Object>> vertex2 = g.V(VERTEX_SOFTWARE_2).elementMap().toList();

        // Then
        assertThat(vertex1)
                .containsExactly(EXPECTED_PERSON_1_VERTEX_MAP);

        assertThat(vertex2)
                .containsExactly(EXPECTED_SOFTWARE_2_VERTEX_MAP);
    }

    @Test
    void shouldGetVertexPropertyValue() {
        // Given
        GraphTraversalSource g = gafferPopGraph.traversal();

        // When
        List<Object> vertex = g.V(VERTEX_SOFTWARE_1).values("name").toList();

        // Then
        assertThat(vertex)
                .containsExactly(EXPECTED_SOFTWARE_1_VERTEX_MAP.get("name"));
    }

    @Test
    void shouldFilterVertexesByPropertyValue() {
        // Given
        GraphTraversalSource g = gafferPopGraph.traversal();

        // When
        List<Object> result = g.V().outE(CREATED_EDGE_GROUP).has(WEIGHT_PROPERTY, P.gt(0.4))
                .values(WEIGHT_PROPERTY)
                .toList();

        // Then
        assertThat(result)
                .containsExactly(1.0, 0.8);
    }

    @Test
    void shouldFilterVertexesByLabel() {
        // // Given
        GraphTraversalSource g = gafferPopGraph.traversal();

        // When
        List<Map<Object, Object>> result = g.V().hasLabel(SOFTWARE_GROUP).elementMap().toList();

        // Then
        assertThat(result)
                .contains(EXPECTED_SOFTWARE_1_VERTEX_MAP, atIndex(0))
                .contains(EXPECTED_SOFTWARE_2_VERTEX_MAP, atIndex(1));
    }

    @Test
    void shouldReturnFilteredCountOfVertexes() {
        // Given
        GraphTraversalSource g = gafferPopGraph.traversal();

        // When
        List<Long> result = g.V().hasLabel(SOFTWARE_GROUP).count().toList();

        // Then
        assertThat(result)
                .containsExactly(2L);
    }

    @Test
    void shouldCountAllOutgoingEdgesFromVertex() {
        // Given
        GraphTraversalSource g = gafferPopGraph.traversal();

        // When
        List<Long> result = g.V(VERTEX_PERSON_1).outE().count().toList();

        // Then
        assertThat(result)
                .containsExactly(2L);
    }

    @Test
    void shouldGetAdjacentVerticesNameValues() {
        // Given
        GraphTraversalSource g = gafferPopGraph.traversal();

        // When
        List<Object> result = g.V(VERTEX_PERSON_1).out().values("name").toList();

        assertThat(result)
                .containsOnly(EXPECTED_PERSON_2_VERTEX_MAP.get("name"),
                        EXPECTED_SOFTWARE_1_VERTEX_MAP.get("name"));
    }

    @Test
    void shouldGroupVerticesByLabelAndProvideCount() {
        // Given
        GraphTraversalSource g = gafferPopGraph.traversal();

        // When
        List<Map<Object, Long>> result = g.V().groupCount().by(T.label).toList();

        assertThat(result)
                .first()
                .hasFieldOrPropertyWithValue(PERSON_GROUP, 4L)
                .hasFieldOrPropertyWithValue(SOFTWARE_GROUP, 2L);
    }

    @Test
    void shouldGetEdgesById() {
        // Given
        GraphTraversalSource g = gafferPopGraph.traversal();

        // When
        List<Edge> result = g.E("[p1, s1]", "[p3, s1]").toList();

        assertThat(result)
                .extracting(item -> item.id().toString())
                .containsExactly("[p1, s1]", "[p3, s1]");

        assertThat(result)
                .extracting(item -> item.label())
                .containsExactly(CREATED_EDGE_GROUP, CREATED_EDGE_GROUP);
    }

    @Test
    void shouldGetOutgoingEdges() {
        // Given
        GraphTraversalSource g = gafferPopGraph.traversal();

        // When
        List<Edge> result = g.V(VERTEX_PERSON_1).outE().toList();

        // Then
        assertThat(result)
                .extracting(item -> item.id().toString())
                .containsExactly("[p1, p2]", "[p1, s1]");

        assertThat(result)
                .extracting(item -> item.label())
                .containsExactly("knows", CREATED_EDGE_GROUP);
    }

    @Test
    void shouldGetIncomingEdgesByLabel() {
        // Given
        GraphTraversalSource g = gafferPopGraph.traversal();

        // When
        List<String> result = g.V(VERTEX_SOFTWARE_1).inE(CREATED_EDGE_GROUP).label().toList();

        // Then
        assertThat(result)
                .containsExactly(CREATED_EDGE_GROUP, CREATED_EDGE_GROUP);
    }

    @Test
    void shouldTraverseEdgesFromVertexAndReturnNames() {
        // Given
        GraphTraversalSource g = gafferPopGraph.traversal();

        // When
        List<Object> result = g.V(VERTEX_PERSON_1).out().values("name").toList();

        // Then
        assertThat(result)
                .containsExactly(EXPECTED_PERSON_2_VERTEX_MAP.get("name"),
                        EXPECTED_SOFTWARE_1_VERTEX_MAP.get("name"));
    }

    @Test
    void shouldReturnVerticesByLabelFromSpecificGraph() {
        // Given
        GraphTraversalSource g = gafferPopGraph.traversal();
        final List<String> graphOptions = Arrays.asList("gaffer.federatedstore.operation.graphIds:graphA");

        // When
        List<Map<Object, Object>> result = g.with(GafferPopGraphVariables.OP_OPTIONS, graphOptions).V()
                .hasLabel(PERSON_GROUP)
                .elementMap().toList();

        // Then
        assertThat(result)
                .containsExactlyInAnyOrder(EXPECTED_PERSON_1_VERTEX_MAP, EXPECTED_PERSON_2_VERTEX_MAP,
                        EXPECTED_PERSON_3_VERTEX_MAP);
    }

    @Test
    void shouldReturnFilteredVerticesFromSpecificGraph() {
        // Given
        GraphTraversalSource g = gafferPopGraph.traversal();
        final List<String> graphOptions = Arrays.asList("gaffer.federatedstore.operation.graphIds:graphA");

        // When
        List<Object> result = g.with(GafferPopGraphVariables.OP_OPTIONS, graphOptions).V()
                .outE(CREATED_EDGE_GROUP).has(WEIGHT_PROPERTY, P.gt(0.4))
                .values(WEIGHT_PROPERTY)
                .toList();

        // Then
        assertThat(result)
                .containsExactly(1.0);
    }

    @Test
    void shouldGroupVerticesByLabelAndProvideCountFromSpecificGraph() {
        // Given
        GraphTraversalSource g = gafferPopGraph.traversal();
        final List<String> graphOptions = Arrays.asList("gaffer.federatedstore.operation.graphIds:graphB");

        // When
        List<Map<Object, Long>> result = g.with(GafferPopGraphVariables.OP_OPTIONS, graphOptions).V().groupCount()
                .by(T.label).toList();

        assertThat(result)
                .first()
                .hasFieldOrPropertyWithValue(PERSON_GROUP, 1L)
                .hasFieldOrPropertyWithValue(SOFTWARE_GROUP, 1L);
    }

    @Test
    void shouldGetEdgesByIdFromSpecificGraph() {
        // Given
        GraphTraversalSource g = gafferPopGraph.traversal();
        final List<String> graphOptions = Arrays.asList("gaffer.federatedstore.operation.graphIds:graphB");

        // When
        List<Edge> result = g.with(GafferPopGraphVariables.OP_OPTIONS, graphOptions).E("[p4, s2]").toList();

        assertThat(result)
                .extracting(item -> item.id().toString())
                .containsExactly("[p4, s2]");

        assertThat(result)
                .extracting(item -> item.label())
                .containsExactly(CREATED_EDGE_GROUP);
    }

    @Test
    void shouldGetOutgoingEdgesFromSpecificGraph() {
        // Given
        GraphTraversalSource g = gafferPopGraph.traversal();
        final List<String> graphOptions = Arrays.asList("gaffer.federatedstore.operation.graphIds:graphB");

        // When
        List<Edge> result = g.with(GafferPopGraphVariables.OP_OPTIONS, graphOptions).V("p4").outE().toList();

        // Then
        assertThat(result)
                .extracting(item -> item.id().toString())
                .containsExactly("[p4, s2]");

        assertThat(result)
                .extracting(item -> item.label())
                .containsExactly(CREATED_EDGE_GROUP);
    }

    @Test
    void shouldGetIncomingEdgesFromSpecificGraph() {
        // Given
        GraphTraversalSource g = gafferPopGraph.traversal();
        final List<String> graphOptions = Arrays.asList("gaffer.federatedstore.operation.graphIds:graphA");

        // When
        List<Edge> result = g.with(GafferPopGraphVariables.OP_OPTIONS, graphOptions).V(VERTEX_PERSON_1).outE()
                .toList();

        // Then
        assertThat(result)
                .extracting(item -> item.id().toString())
                .containsExactly("[p1, p2]", "[p1, s1]");

        assertThat(result)
                .extracting(item -> item.label())
                .containsExactly("knows", CREATED_EDGE_GROUP);
    }
}
