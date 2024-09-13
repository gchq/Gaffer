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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.tinkerpop.util.GafferPopTestUtil.StoreType;
import uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernFederatedTestUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernFederatedTestUtils.CREATED_GRAPH_ID;
import static uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernFederatedTestUtils.KNOWS_GRAPH_ID;
import static uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils.JOSH;
import static uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils.LOP;
import static uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils.MARKO;
import static uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils.PERSON;
import static uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils.RIPPLE;
import static uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils.SOFTWARE;
import static uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils.VADAS;
import static uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils.WEIGHT;

/**
 * Test specific Federated features with Gremlin
 * General tests run against a federated graph should go in GafferPopGraphIT
 */
class GafferPopFederatedIT {
    private static GafferPopGraph gafferPopGraph;
    private final String graphIdOption = "gaffer.federatedstore.operation.graphIds:";
    private final List<String> knowsGraphOptions = Arrays.asList(graphIdOption + KNOWS_GRAPH_ID);
    private final List<String> createdGraphOptions = Arrays.asList(graphIdOption + CREATED_GRAPH_ID);


    @BeforeAll
    public static void setUp() throws Exception {
        gafferPopGraph = GafferPopModernFederatedTestUtils.createModernGraph(GafferPopFederatedIT.class, StoreType.MAP);
    }

    @Test
    void shouldReturnVerticesByLabelFromSpecificGraph() {
        // Given
        GraphTraversalSource g = gafferPopGraph.traversal();

        // When
        List<Map<Object, Object>> result = g.with(GafferPopGraphVariables.OP_OPTIONS, knowsGraphOptions)
                .V().hasLabel(PERSON).elementMap().toList();

        // Then
        assertThat(result)
                .containsExactlyInAnyOrder(
                        MARKO.getPropertyMap(),
                        JOSH.getPropertyMap(),
                        VADAS.getPropertyMap()
                );
    }

    @Test
    void shouldReturnFilteredVerticesFromSpecificGraph() {
        // Given
        GraphTraversalSource g = gafferPopGraph.traversal();

        // When
        List<Object> result = g.with(GafferPopGraphVariables.OP_OPTIONS, createdGraphOptions)
                .V()
                .outE()
                .has(WEIGHT, P.gt(0.2))
                .values(WEIGHT)
                .toList();

        // Then
        assertThat(result)
                .containsExactlyInAnyOrder(0.4, 0.4, 1.0);
    }

    @Test
    void shouldGroupVerticesByLabelAndProvideCountFromSpecificGraph() {
        // Given
        GraphTraversalSource g = gafferPopGraph.traversal();

        // When
        List<Map<Object, Long>> result = g.with(GafferPopGraphVariables.OP_OPTIONS, createdGraphOptions).V().groupCount()
                .by(T.label).toList();

        assertThat(result)
                .first()
                .hasFieldOrPropertyWithValue(PERSON, 3L)
                .hasFieldOrPropertyWithValue(SOFTWARE, 2L);
    }

    @Test
    void shouldGetEdgesByIdFromSpecificGraph() {
        // Given
        GraphTraversalSource g = gafferPopGraph.traversal();

        // When
        List<Edge> result = g.with(GafferPopGraphVariables.OP_OPTIONS, createdGraphOptions).E("[4, 3]").toList();

        assertThat(result)
                .extracting(item -> item.id())
                .containsExactly(JOSH.created(LOP));
    }

    @Test
    void shouldGetOutgoingEdgesFromSpecificGraph() {
        // Given
        GraphTraversalSource g = gafferPopGraph.traversal();

        // When
        List<Edge> result = g.with(GafferPopGraphVariables.OP_OPTIONS, knowsGraphOptions).V(MARKO.getId()).outE().toList();

        // Then
        assertThat(result)
                .extracting(item -> item.id())
                .containsExactlyInAnyOrder(MARKO.knows(VADAS), MARKO.knows(JOSH));
    }

    @Test
    void shouldGetIncomingEdgesFromSpecificGraph() {
        // Given
        GraphTraversalSource g = gafferPopGraph.traversal();

        // When
        List<Edge> result = g.with(GafferPopGraphVariables.OP_OPTIONS, createdGraphOptions).V(RIPPLE.getId()).inE()
                .toList();

        // Then
        assertThat(result)
                .extracting(item -> item.id())
                .containsExactlyInAnyOrder(JOSH.created(RIPPLE));
    }
}
