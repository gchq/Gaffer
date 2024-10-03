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
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernFederatedTestUtils.CREATED_GRAPH_ID;
import static uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernFederatedTestUtils.KNOWS_GRAPH_ID;
import static uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils.CREATED;
import static uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils.JOSH;
import static uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils.KNOWS;
import static uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils.LOP;
import static uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils.MARKO;
import static uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils.NAME;
import static uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils.PERSON;
import static uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils.PETER;
import static uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils.RIPPLE;
import static uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils.SOFTWARE;
import static uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils.VADAS;
import static uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils.WEIGHT;

public abstract class GafferPopFederationTests {
    private final String graphIdOption = "gaffer.federatedstore.operation.graphIds:";
    private final List<String> knowsGraphOptions = Arrays.asList(graphIdOption + KNOWS_GRAPH_ID);
    private final List<String> createdGraphOptions = Arrays.asList(graphIdOption + CREATED_GRAPH_ID);

    protected abstract GafferPopGraph getGraph() throws OperationException;

    @Test
    void shouldGetAllVertices() throws OperationException {
        GraphTraversalSource g = getGraph().traversal();
        final List<Vertex> result = g.V().toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(
                        MARKO.getId(),
                        VADAS.getId(),
                        JOSH.getId(),
                        PETER.getId(),
                        LOP.getId(),
                        RIPPLE.getId());
    }

    @Test
    void shouldTruncateGetAllVertices() throws OperationException {
        GraphTraversalSource g = getGraph().traversal();
        final List<Vertex> result = g.with("getElementsLimit", 2).V().toList();

        assertThat(result)
                .hasSize(2)
                .extracting(r -> r.id())
                .containsAnyOf(
                        MARKO.getId(),
                        VADAS.getId(),
                        JOSH.getId(),
                        PETER.getId(),
                        LOP.getId(),
                        RIPPLE.getId());
    }

    @Test
    void shouldGetVerticesById() throws OperationException {
        GraphTraversalSource g = getGraph().traversal();
        final List<Vertex> result = g.V(MARKO.getId(), RIPPLE.getId()).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(MARKO.getId(), RIPPLE.getId());
    }

    @Test
    void shouldGetVertexPropertyValues() throws OperationException {
        GraphTraversalSource g = getGraph().traversal();
        final List<Object> result = g.V(MARKO.getId()).values(NAME).toList();

        assertThat(result)
                .hasSize(1)
                .first()
                .isInstanceOf(String.class)
                .isEqualTo(MARKO.getName());
    }

    @Test
    void shouldGetVerticesByLabel() throws OperationException {
        GraphTraversalSource g = getGraph().traversal();
        final List<Vertex> result = g.V().hasLabel(PERSON).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(
                    MARKO.getId(),
                    VADAS.getId(),
                    JOSH.getId(),
                    PETER.getId()
                );
    }

    @Test
    void shouldGetAllOutgoingEdgesFromVertex() throws OperationException {
        GraphTraversalSource g = getGraph().traversal();
        final List<Edge> result = g.V(MARKO.getId()).outE().toList();

        assertThat(result)
            .extracting(r -> r.id())
            .containsExactlyInAnyOrder(
                MARKO.knows(VADAS),
                MARKO.knows(JOSH),
                MARKO.created(LOP)
            );
    }

    @Test
    void shouldCountAllOutgoingEdgesFromVertex() throws OperationException {
        GraphTraversalSource g = getGraph().traversal();
        final List<Long> result = g.V(MARKO.getId()).outE().count().toList();

        assertThat(result).containsExactlyInAnyOrder(3L);
    }

    @Test
    void shouldGetAllIncomingEdgesFromVertex() throws OperationException {
        GraphTraversalSource g = getGraph().traversal();
        final List<String> result = g.V(LOP.getId()).inE().label().toList();

        assertThat(result)
            .containsExactlyInAnyOrder(
                CREATED,
                CREATED,
                CREATED
            );
    }

    @Test
    void shouldGetAdjacentVerticesNameValues() throws OperationException {
        GraphTraversalSource g = getGraph().traversal();
        final List<Object> result = g.V(MARKO.getId()).out().values(NAME).toList();

        assertThat(result)
                .extracting(r -> (String) r)
                .containsExactlyInAnyOrder(
                    LOP.getName(),
                    VADAS.getName(),
                    JOSH.getName()
                );
    }

    @Test
    void shouldGroupVerticesByLabelAndProvideCount() throws OperationException {
        GraphTraversalSource g = getGraph().traversal();
        List<Map<Object, Long>> result = g.V().groupCount().by(T.label).toList();

        assertThat(result)
                .first()
                .hasFieldOrPropertyWithValue(PERSON, 4L)
                .hasFieldOrPropertyWithValue(SOFTWARE, 2L);
    }

    @Test
    void shouldGetAllVerticesConnectedToOutGoingEdgeOfGivenVertex() throws OperationException {
        GraphTraversalSource g = getGraph().traversal();
        List<Object> result = g.V(MARKO.getId()).outE().inV().values(NAME).toList();

        assertThat(result)
        .containsExactlyInAnyOrder(
            JOSH.getName(),
            LOP.getName(),
            VADAS.getName()
        );
    }

    @Test
    void shouldGetPropertiesOfIncomingVerticesForSpecificVertex() throws OperationException {
        GraphTraversalSource g = getGraph().traversal();
        List<Map<Object, Object>> result = g.V(JOSH.getId()).inE().outV().elementMap().toList();

        assertThat(result).containsExactly(MARKO.getPropertyMap());
    }

    @Test
    void shouldGetAllEdges() throws OperationException {
        GraphTraversalSource g = getGraph().traversal();
        final List<Edge> result = g.E().toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(
                        MARKO.knows(JOSH),
                        MARKO.knows(VADAS),
                        MARKO.created(LOP),
                        JOSH.created(LOP),
                        JOSH.created(RIPPLE),
                        PETER.created(LOP));
    }

    @Test
    void shouldTruncateGetAllEdges() throws OperationException {
        GraphTraversalSource g = getGraph().traversal();
        final List<Edge> result = g.with("getElementsLimit", 2).E().toList();

        assertThat(result)
                .hasSize(2)
                .extracting(r -> r.id())
                .containsAnyOf(
                        MARKO.knows(JOSH),
                        MARKO.knows(VADAS),
                        MARKO.created(LOP),
                        JOSH.created(LOP),
                        JOSH.created(RIPPLE),
                        PETER.created(LOP));
    }

    @Test
    void shouldGetEdgeById() throws OperationException {
        GraphTraversalSource g = getGraph().traversal();
        List<String> edgeIds = Stream.of(
            "[1,2]",
            "   [ 1  , 2   ]  ",
            "[1,knows,2]",
            "[1, knows, 2]",
            " [  1   ,knows  ,    2] "
        ).collect(Collectors.toList());

        edgeIds.forEach(id -> {
            final List<Edge> result = g.E(id).toList();

            assertThat(result)
                // .withFailMessage("(%s) Edge ID: %s returned %s", getGraph(), id, result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(MARKO.knows(VADAS));
        });
    }

    @Test
    void shouldAddV() throws OperationException {
        GraphTraversalSource g = getGraph().traversal();
        g.addV(PERSON).property(NAME, "stephen").property(T.id, "test").iterate();

        final List<Vertex> result = g.V().toList();
        assertThat(result)
                .extracting(r -> r.value(NAME))
                .contains("stephen");
        reset();
    }

    @Test
    void shouldAddE() throws OperationException {
        GraphTraversalSource g = getGraph().traversal();
        g.addE(KNOWS).from(__.V(VADAS.getId())).to(__.V(PETER.getId())).iterate();

        final List<Edge> result = g.E().toList();
        assertThat(result)
                .extracting(r -> r.id())
                .contains(VADAS.knows(PETER));
        reset();
    }

    @Test
    void shouldSeedWithVertexOnlyEdge() throws OperationException {
        GraphTraversalSource g = getGraph().traversal();
        // Edge has a vertex but not an entity in the graph - Gaffer only feature
        getGraph().addEdge(new GafferPopEdge("knows", GafferPopModernTestUtils.MARKO.getId(), "7", getGraph()));

        List<Vertex> result = g.V("7").toList();
        assertThat(result)
            .extracting(r -> r.id())
            .contains("7");
        reset();
    }

    @Test
    void shouldTraverseEdgeWithVertexOnlySeed() throws OperationException {
        GraphTraversalSource g = getGraph().traversal();
        // Edge has a vertex but not an entity in the graph - Gaffer only feature
        getGraph().addEdge(new GafferPopEdge("knows", GafferPopModernTestUtils.MARKO.getId(), "7", getGraph()));

        List<Map<Object, Object>> result = g.V("7").inE().outV().elementMap().toList();
        assertThat(result)
            .containsExactly(MARKO.getPropertyMap());
        reset();
    }

    @Test
    void shouldReturnVerticesByLabelFromSpecificGraph() throws OperationException {
        // Given
        GraphTraversalSource g = getGraph().traversal();

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
    void shouldReturnFilteredVerticesFromSpecificGraph() throws OperationException {
        // Given
        GraphTraversalSource g = getGraph().traversal();

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
    void shouldGroupVerticesByLabelAndProvideCountFromSpecificGraph() throws OperationException {
        // Given
        GraphTraversalSource g = getGraph().traversal();

        // When
        List<Map<Object, Long>> result = g.with(GafferPopGraphVariables.OP_OPTIONS, createdGraphOptions).V().groupCount()
                .by(T.label).toList();

        assertThat(result)
                .first()
                .hasFieldOrPropertyWithValue(PERSON, 3L)
                .hasFieldOrPropertyWithValue(SOFTWARE, 2L);
    }

    @Test
    void shouldGetEdgesByIdFromSpecificGraph() throws OperationException {
        // Given
        GraphTraversalSource g = getGraph().traversal();

        // When
        List<Edge> result = g.with(GafferPopGraphVariables.OP_OPTIONS, createdGraphOptions).E("[4, 3]").toList();

        assertThat(result)
                .extracting(item -> item.id())
                .containsExactly(JOSH.created(LOP));
    }

    @Test
    void shouldGetOutgoingEdgesFromSpecificGraph() throws OperationException {
        // Given
        GraphTraversalSource g = getGraph().traversal();

        // When
        List<Edge> result = g.with(GafferPopGraphVariables.OP_OPTIONS, knowsGraphOptions).V(MARKO.getId()).outE().toList();

        // Then
        assertThat(result)
                .extracting(item -> item.id())
                .containsExactlyInAnyOrder(MARKO.knows(VADAS), MARKO.knows(JOSH));
    }

    @Test
    void shouldGetIncomingEdgesFromSpecificGraph() throws OperationException {
        // Given
        GraphTraversalSource g = getGraph().traversal();

        // When
        List<Edge> result = g.with(GafferPopGraphVariables.OP_OPTIONS, createdGraphOptions).V(RIPPLE.getId()).inE()
                .toList();

        // Then
        assertThat(result)
                .extracting(item -> item.id())
                .containsExactlyInAnyOrder(JOSH.created(RIPPLE));
    }

    void reset() {
        // reset cache for federation
        CacheServiceLoader.shutdown();
    }
}
