/*
 * Copyright 2017-2024 Crown Copyright
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

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.tinkerpop.util.GafferPopTestUtil.StoreType;
import uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernTestUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
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

/**
 * Runs general gremlin queries
 * Each test runs against multiple graphs to ensure each store gets the same results
 */
class GafferPopGraphIT {
    private static final String TEST_NAME_FORMAT = "({0}) {displayName}";
    private static final String VERTEX_ONLY_ID_STRING = "7";
    private static GafferPopGraph mapStore;
    private static GafferPopGraph accumuloStore;

    @BeforeAll
    public static void createGraphs() {
        mapStore = GafferPopModernTestUtils.createModernGraph(GafferPopGraphIT.class, StoreType.MAP);
        accumuloStore = GafferPopModernTestUtils.createModernGraph(GafferPopGraphIT.class, StoreType.ACCUMULO);
    }

    private static Stream<Arguments> provideTraversals() {
        return Stream.of(
                Arguments.of("Map Store", mapStore.traversal()),
                Arguments.of("Accumulo Store", accumuloStore.traversal())
        );
    }

    @ParameterizedTest(name = TEST_NAME_FORMAT)
    @MethodSource("provideTraversals")
    void shouldGetAllVertices(String graph, GraphTraversalSource g) {
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

    @ParameterizedTest(name = TEST_NAME_FORMAT)
    @MethodSource("provideTraversals")
    void shouldTruncateGetAllVertices(String graph, GraphTraversalSource g) {
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

    @ParameterizedTest(name = TEST_NAME_FORMAT)
    @MethodSource("provideTraversals")
    void shouldGetVerticesById(String graph, GraphTraversalSource g) {
        final List<Vertex> result = g.V(MARKO.getId(), RIPPLE.getId()).toList();

        assertThat(result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(MARKO.getId(), RIPPLE.getId());
    }

    @ParameterizedTest(name = TEST_NAME_FORMAT)
    @MethodSource("provideTraversals")
    void shouldGetVertexPropertyValues(String graph, GraphTraversalSource g) {
        final List<Object> result = g.V(MARKO.getId()).values(NAME).toList();

        assertThat(result)
                .hasSize(1)
                .first()
                .isInstanceOf(String.class)
                .isEqualTo(MARKO.getName());
    }

    @ParameterizedTest(name = TEST_NAME_FORMAT)
    @MethodSource("provideTraversals")
    void shouldGetVerticesByLabel(String graph, GraphTraversalSource g) {
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

    @ParameterizedTest(name = TEST_NAME_FORMAT)
    @MethodSource("provideTraversals")
    void shouldGetAllOutgoingEdgesFromVertex(String graph, GraphTraversalSource g) {
        final List<Edge> result = g.V(MARKO.getId()).outE().toList();

        assertThat(result)
            .extracting(r -> r.id())
            .containsExactlyInAnyOrder(
                MARKO.knows(VADAS),
                MARKO.knows(JOSH),
                MARKO.created(LOP)
            );
    }

    @ParameterizedTest(name = TEST_NAME_FORMAT)
    @MethodSource("provideTraversals")
    void shouldCountAllOutgoingEdgesFromVertex(String graph, GraphTraversalSource g) {
        final List<Long> result = g.V(MARKO.getId()).outE().count().toList();

        assertThat(result).containsExactlyInAnyOrder(3L);
    }

    @ParameterizedTest(name = TEST_NAME_FORMAT)
    @MethodSource("provideTraversals")
    void shouldGetAllIncomingEdgesFromVertex(String graph, GraphTraversalSource g) {
        final List<String> result = g.V(LOP.getId()).inE().label().toList();

        assertThat(result)
            .containsExactlyInAnyOrder(
                CREATED,
                CREATED,
                CREATED
            );
    }

    @ParameterizedTest(name = TEST_NAME_FORMAT)
    @MethodSource("provideTraversals")
    void shouldGetAdjacentVerticesNameValues(String graph, GraphTraversalSource g) {
        final List<Object> result = g.V(MARKO.getId()).out().values(NAME).toList();

        assertThat(result)
                .extracting(r -> (String) r)
                .containsExactlyInAnyOrder(
                    LOP.getName(),
                    VADAS.getName(),
                    JOSH.getName()
                );
    }

    @ParameterizedTest(name = TEST_NAME_FORMAT)
    @MethodSource("provideTraversals")
    void shouldGroupVerticesByLabelAndProvideCount(String graph, GraphTraversalSource g) {
        List<Map<Object, Long>> result = g.V().groupCount().by(T.label).toList();

        assertThat(result)
                .first()
                .hasFieldOrPropertyWithValue(PERSON, 4L)
                .hasFieldOrPropertyWithValue(SOFTWARE, 2L);
    }

    @ParameterizedTest(name = TEST_NAME_FORMAT)
    @MethodSource("provideTraversals")
    void shouldGetAllVerticesConnectedToOutGoingEdgeOfGivenVertex(String graph, GraphTraversalSource g) {
        List<Object> result = g.V(MARKO.getId()).outE().inV().values(NAME).toList();

        assertThat(result)
        .containsExactlyInAnyOrder(
            JOSH.getName(),
            LOP.getName(),
            VADAS.getName()
        );
    }

    @ParameterizedTest(name = TEST_NAME_FORMAT)
    @MethodSource("provideTraversals")
    void shouldGetPropertiesOfIncomingVerticesForSpecificVertex(String graph, GraphTraversalSource g) {
        List<Map<Object, Object>> result = g.V(JOSH.getId()).inE().outV().elementMap().toList();

        assertThat(result).containsExactly(MARKO.getPropertyMap());
    }

    @ParameterizedTest(name = TEST_NAME_FORMAT)
    @MethodSource("provideTraversals")
    void shouldGetAllEdges(String graph, GraphTraversalSource g) {
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

    @ParameterizedTest(name = TEST_NAME_FORMAT)
    @MethodSource("provideTraversals")
    void shouldTruncateGetAllEdges(String graph, GraphTraversalSource g) {
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

    @ParameterizedTest(name = TEST_NAME_FORMAT)
    @MethodSource("provideTraversals")
    void shouldGetEdgeById(String graph, GraphTraversalSource g) {
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
                .withFailMessage("(%s) Edge ID: %s returned %s", graph, id, result)
                .extracting(r -> r.id())
                .containsExactlyInAnyOrder(MARKO.knows(VADAS));
        });
    }

    @ParameterizedTest(name = TEST_NAME_FORMAT)
    @MethodSource("provideTraversals")
    void shouldAddV(String graph, GraphTraversalSource g) {
        g.addV(PERSON).property(NAME, "stephen").property(T.id, "test").iterate();

        final List<Vertex> result = g.V().toList();
        assertThat(result)
                .extracting(r -> r.value(NAME))
                .contains("stephen");
        reset();
    }

    @ParameterizedTest(name = TEST_NAME_FORMAT)
    @MethodSource("provideTraversals")
    void shouldAddE(String graph, GraphTraversalSource g) {
        g.addE(KNOWS).from(__.V(VADAS.getId())).to(__.V(PETER.getId())).iterate();

        final List<Edge> result = g.E().toList();
        assertThat(result)
                .extracting(r -> r.id())
                .contains(VADAS.knows(PETER));
        reset();
    }

    @ParameterizedTest(name = TEST_NAME_FORMAT)
    @MethodSource("provideTraversals")
    void shouldSeedWithVertexOnlyEdge(String graph, GraphTraversalSource g) {
        // Edge has a vertex but not an entity in the graph - Gaffer only feature
        // [1 - knows -> 7]
        mapStore.addEdge(new GafferPopEdge("knows", GafferPopModernTestUtils.MARKO.getId(), VERTEX_ONLY_ID_STRING, mapStore));
        accumuloStore.addEdge(new GafferPopEdge("knows", GafferPopModernTestUtils.MARKO.getId(), VERTEX_ONLY_ID_STRING, accumuloStore));

        List<Vertex> result = g.V(VERTEX_ONLY_ID_STRING).toList();
        assertThat(result)
            .extracting(r -> r.id())
            .contains(VERTEX_ONLY_ID_STRING);
        reset();
    }

    @ParameterizedTest(name = TEST_NAME_FORMAT)
    @MethodSource("provideTraversals")
    void shouldTraverseEdgeWithVertexOnlyEdge(String graph, GraphTraversalSource g) {
        // Edge has a two vertices with no entities in the graph - Gaffer only feature
        // [8 - knows -> 7]
        mapStore.addEdge(new GafferPopEdge("knows", "8", VERTEX_ONLY_ID_STRING, mapStore));
        accumuloStore.addEdge(new GafferPopEdge("knows", "8", VERTEX_ONLY_ID_STRING, accumuloStore));

        List<Vertex> result = g.V(VERTEX_ONLY_ID_STRING).inE().inV().toList();
        assertThat(result)
            .extracting(r -> r.id())
            .contains(VERTEX_ONLY_ID_STRING);
        List<Vertex> result2 = g.V(VERTEX_ONLY_ID_STRING).inE().outV().toList();
        assertThat(result2)
            .extracting(r -> r.id())
            .contains("8");
        List<Vertex> result3 = g.V("8").outE().inV().toList();
        assertThat(result3)
            .extracting(r -> r.id())
            .contains(VERTEX_ONLY_ID_STRING);
        List<Vertex> result4 = g.V("8").outE().outV().toList();
        assertThat(result4)
            .extracting(r -> r.id())
            .contains("8");
        List<Vertex> resultLabel = g.V("8").out("knows").toList();
        assertThat(resultLabel)
            .extracting(r -> r.id())
            .containsOnly(VERTEX_ONLY_ID_STRING);
        reset();
    }

    void reset() {
        // reset cache for federation
        CacheServiceLoader.shutdown();
        // recreate the graphs
        createGraphs();
    }

}
