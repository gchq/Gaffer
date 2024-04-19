/*
 * Copyright 2023-2024 Crown Copyright
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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.tinkerpop.util.GafferPopFederatedTestUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.atIndex;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopTestUtil.TEST_CONFIGURATION_1;

public class GafferPopFederatedIT {
    private static Graph federatedGraph;
    private static GafferPopGraph gafferPopGraph;

    public static final String VERTEX_PERSON_1 = "p1";
    public static final String VERTEX_PERSON_2 = "p2";
    public static final String VERTEX_SOFTWARE_1 = "s1";
    public static final String VERTEX_SOFTWARE_2 = "s2";
    public static final String SOFTWARE_GROUP = "software";
    public static final String PERSON_GROUP = "person";
    public static final String CREATED_EDGE_GROUP = "created";
    public static final String NAME_PROPERTY = "name";
    public static final String WEIGHT_PROPERTY = "weight";

    @Before
    public void setUp() throws Exception {
        federatedGraph = GafferPopFederatedTestUtil.setUpFederatedGraph(GafferPopFederatedIT.class);
        gafferPopGraph = GafferPopGraph.open(TEST_CONFIGURATION_1, federatedGraph);
    }

    @After
    public void tearDown() {
        CacheServiceLoader.shutdown();
    }

    @Test
    public void shouldConstructFederatedGafferPopGraph() {
        // When
        final Map<String, Object> variables = gafferPopGraph.variables().asMap();

        // Then
        assertThat(variables.get(GafferPopGraphVariables.SCHEMA))
                .isSameAs(federatedGraph.getSchema());

        assertThat(variables.get(GafferPopGraphVariables.USER))
                .hasFieldOrPropertyWithValue("userId", "user01");
    }

    @Test
    public void shouldGetVerticesById() {
        // Given
        GraphTraversalSource g = gafferPopGraph.traversal();

        // When
        List<Map<Object, Object>> vertex1 = g.V(VERTEX_PERSON_1).elementMap().toList();
        List<Map<Object, Object>> vertex2 = g.V(VERTEX_SOFTWARE_2).elementMap().toList();

        // expected
        Map<Object, String> expectedVertex1 = new HashMap<>();
        expectedVertex1.put(T.id, VERTEX_PERSON_1);
        expectedVertex1.put(T.label, PERSON_GROUP);
        expectedVertex1.put("name", "person1Name");

        Map<Object, String> expectedVertex2 = new HashMap<>();
        expectedVertex1.put(T.id, VERTEX_SOFTWARE_2);
        expectedVertex1.put(T.label, SOFTWARE_GROUP);
        expectedVertex1.put("name", "software2Name");

        // Then
        assertThat(vertex1)
                .hasSize(1)
                .first()
                .isEqualTo(expectedVertex1);

        assertThat(vertex2)
                .hasSize(1)
                .first()
                .isEqualTo(expectedVertex2);
    }

    @Test
    public void shouldGetVertexPropertyValue() {
        // Given
        GraphTraversalSource g = gafferPopGraph.traversal();

        // When
        List<Object> vertex = g.V(VERTEX_SOFTWARE_1).values("name").toList();

        // Then
        assertThat(vertex)
                .hasSize(1)
                .first()
                .hasToString("software1Name");
    }

    @Test
    public void shouldFilterVertexesByPropertyValue() {
        // Given
        GraphTraversalSource g = gafferPopGraph.traversal();

        // When
        List<Object> result = g.V().outE(CREATED_EDGE_GROUP).has(WEIGHT_PROPERTY, P.gt(0.4)).values(WEIGHT_PROPERTY)
                .toList();

        // Then
        assertThat(result)
                .hasSize(2)
                .contains(1.0, 0.8);
    }

    @Test
    public void shouldFilterVertexesByLabel() {
        // // Given
        GraphTraversalSource g = gafferPopGraph.traversal();

        // When
        List<Map<Object, Object>> result = g.V().hasLabel(SOFTWARE_GROUP).elementMap().toList();

        // expected
        Map<Object, Object> expectedVertex1 = new HashMap<>();
        expectedVertex1.put(T.id, VERTEX_SOFTWARE_1);
        expectedVertex1.put(T.label, SOFTWARE_GROUP);
        expectedVertex1.put("name", "person1Name");

        Map<Object, Object> expectedVertex2 = new HashMap<>();
        expectedVertex1.put(T.id, VERTEX_SOFTWARE_2);
        expectedVertex1.put(T.label, SOFTWARE_GROUP);
        expectedVertex1.put("name", "software2Name");

        // Then
        assertThat(result)
                .hasSize(2)
                .contains(expectedVertex1, atIndex(0))
                .contains(expectedVertex2, atIndex(1));
    }

    @Test
    public void shouldReturnFilteredCountOfVertexes() {
        // Given
        GraphTraversalSource g = gafferPopGraph.traversal();

        // When
        List<Long> result = g.V().hasLabel(SOFTWARE_GROUP).count().toList();

        // Then
        assertThat(result)
                .first()
                .isEqualTo(2L);
    }

    @Test
    public void shouldCountAllOutgoingEdgesFromVertex() {
        // Given
        GraphTraversalSource g = gafferPopGraph.traversal();

        // When
        List<Long> result = g.V(VERTEX_PERSON_1).outE().count().toList();

        // Then
        assertThat(result)
                .first()
                .isEqualTo(2L);
    }

    @Test
    public void shouldGetAdjacentVerticesNameValues() {
        // Given
        GraphTraversalSource g = gafferPopGraph.traversal();

        // When
        List<Object> result = g.V(VERTEX_PERSON_1).out().values("name").toList();

        assertThat(result)
                .hasSize(2)
                .contains("person2Name", "software1Name");
    }

    @Test
    public void shouldGroupVerticesByLabelAndProvideCount() {
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
    public void shouldGetEdgesById() {
        // Given
        GraphTraversalSource g = gafferPopGraph.traversal();

        // When
        List<Edge> result = g.E(VERTEX_SOFTWARE_1).toList();

        // Then
        assertThat(result)
                .hasSize(2)
                .extracting(item -> item.id().toString())
                .contains("[p1, s1]", "[p3, s1]");

        assertThat(result)
                .extracting(item -> item.label())
                .contains(CREATED_EDGE_GROUP, CREATED_EDGE_GROUP);
    }

    @Test
    public void shouldGetOutgoingEdges() {
        // Given
        GraphTraversalSource g = gafferPopGraph.traversal();

        // When
        List<Edge> result = g.V(VERTEX_PERSON_1).outE().toList();

        // Then
        assertThat(result)
                .hasSize(2)
                .extracting(item -> item.id().toString())
                .contains("[p1, p2]", "[p1, s1]");

        assertThat(result)
                .extracting(item -> item.label())
                .contains("knows", CREATED_EDGE_GROUP);
    }

    @Test
    public void shouldGetIncomingEdgesByLabel() {
        // Given
        GraphTraversalSource g = gafferPopGraph.traversal();

        // When
        List<String> result = g.V(VERTEX_SOFTWARE_1).inE(CREATED_EDGE_GROUP).label().toList();

        // Then
        assertThat(result)
                .hasSize(2)
                .contains(CREATED_EDGE_GROUP, CREATED_EDGE_GROUP);
    }

    @Test
    public void shouldTraverseEdgesFromVertexAndReturnNames() {
        // Given
        GraphTraversalSource g = gafferPopGraph.traversal();

        // When
        List<Object> result = g.V(VERTEX_PERSON_1).out().values("name").toList();

        // Then
        assertThat(result)
                .contains("person2Name", "software1Name");
    }
}
