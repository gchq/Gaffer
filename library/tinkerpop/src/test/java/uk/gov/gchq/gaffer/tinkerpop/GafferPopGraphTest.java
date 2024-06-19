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

import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.impl.add.AddElementsFromSocket;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraph.HasStepFilterStage;
import uk.gov.gchq.gaffer.tinkerpop.util.GafferPopTestUtil;
import uk.gov.gchq.gaffer.tinkerpop.util.GafferPopTstvTestUtils;
import uk.gov.gchq.gaffer.user.User;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopTestUtil.AUTH_1;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopTestUtil.AUTH_2;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopTestUtil.TEST_CONFIGURATION_1;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopTestUtil.TEST_CONFIGURATION_2;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopTestUtil.TEST_CONFIGURATION_3;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopTestUtil.getTestUser;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopTstvTestUtils.TSTV_ID;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopTstvTestUtils.TSTV_ID_STRING;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopModernTestUtils.CREATED;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopModernTestUtils.JOSH;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopModernTestUtils.KNOWS;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopModernTestUtils.LOP;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopModernTestUtils.MARKO;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopModernTestUtils.NAME;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopModernTestUtils.PERSON;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopModernTestUtils.SOFTWARE;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopModernTestUtils.VADAS;
import static uk.gov.gchq.gaffer.tinkerpop.util.GafferPopModernTestUtils.WEIGHT;

public class GafferPopGraphTest {
    public static final String TSTV_GROUP = "tstv";

    private static final AccumuloProperties PROPERTIES = AccumuloProperties
            .loadStoreProperties(StreamUtil.openStream(GafferPopGraphTest.class, "/gaffer/store.properties"));

    @Test
    public void shouldConstructGafferPopGraphWithOnlyConfig() {
        // Given
        final User expectedUser = getTestUser(AUTH_1, AUTH_2);

        // When
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION_2);

        // Then
        final Map<String, Object> variables = graph.variables().asMap();
        assertThat(variables.get(GafferPopGraphVariables.USER_ID)).isEqualTo(expectedUser.getUserId());
        assertThat(variables.get(GafferPopGraphVariables.GET_ALL_ELEMENTS_LIMIT)).isEqualTo(1);
        assertThat(variables.get(GafferPopGraphVariables.HAS_STEP_FILTER_STAGE))
                .isEqualTo(HasStepFilterStage.POST_TRANSFORM.toString());

        final Map<String, String> opOptions = (Map<String, String>) variables.get(GafferPopGraphVariables.OP_OPTIONS);
        assertThat(opOptions).containsEntry("key1", "value1").containsEntry("key2", "value2").hasSize(2);
        assertThat(variables.size()).isEqualTo(5);
    }

    @Test
    public void shouldConstructGafferPopGraphWithConfigFile() {
        // Given
        final User expectedUser = getTestUser();

        // when
        final GafferPopGraph graph = GafferPopGraph
                .open(GafferPopGraphTest.class.getClassLoader().getResource("gafferpop-test.properties").getPath());

        // Then
        final Map<String, Object> variables = graph.variables().asMap();
        assertThat(variables.get(GafferPopGraphVariables.USER_ID)).isEqualTo(expectedUser.getUserId());
        assertThat(variables.get(GafferPopGraphVariables.GET_ALL_ELEMENTS_LIMIT)).isEqualTo(2);
        assertThat(variables.get(GafferPopGraphVariables.HAS_STEP_FILTER_STAGE))
                .isEqualTo(HasStepFilterStage.POST_AGGREGATION.toString());

        final Map<String, String> opOptions = (Map<String, String>) variables.get(GafferPopGraphVariables.OP_OPTIONS);
        assertThat(opOptions).containsEntry("key1", "value1").hasSize(1);
        assertThat(variables.size()).isEqualTo(5);
    }

    @Test
    public void shouldConstructGafferPopGraph() {
        // Given
        final Graph gafferGraph = getGafferGraph();
        final User expectedUser = getTestUser(AUTH_1, AUTH_2);

        // When
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION_1, gafferGraph);

        // Then
        final Map<String, Object> variables = graph.variables().asMap();
        assertThat(variables)
                .containsEntry(GafferPopGraphVariables.DATA_AUTHS, expectedUser.getDataAuths().toArray())
                .containsEntry(GafferPopGraphVariables.USER_ID, expectedUser.getUserId())
                .containsEntry(GafferPopGraphVariables.GET_ALL_ELEMENTS_LIMIT,
                        GafferPopGraph.DEFAULT_GET_ALL_ELEMENTS_LIMIT)
                .containsEntry(GafferPopGraphVariables.HAS_STEP_FILTER_STAGE,
                        GafferPopGraph.DEFAULT_HAS_STEP_FILTER_STAGE.toString());

        final Map<String, String> opOptions = (Map<String, String>) variables.get(GafferPopGraphVariables.OP_OPTIONS);
        assertThat(opOptions).containsEntry("key1", "value1").containsEntry("key2", "value2").hasSize(2);
        assertThat(variables.size()).isEqualTo(5);
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionForNoGraphId() {

        // Given/Then
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> new GafferPopGraph(TEST_CONFIGURATION_3))
                .withMessageMatching("gaffer.graphId property is required");
    }

    @Test
    public void shouldThrowUnsupportedExceptionForCompute() {
        // Given
        final Graph gafferGraph = getGafferGraph();
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION_1, gafferGraph);

        // When / Then
        assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> graph.compute());
    }

    @Test
    public void shouldThrowUnsupportedExceptionForComputeWithClass() {
        // Given
        final Graph gafferGraph = getGafferGraph();
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION_1, gafferGraph);

        // When / Then
        assertThatExceptionOfType(UnsupportedOperationException.class)
                .isThrownBy(() -> graph.compute(GraphComputer.class));
    }

    @Test
    public void shouldThrowUnsupportedExceptionForTx() {
        // Given
        final Graph gafferGraph = getGafferGraph();
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION_1, gafferGraph);

        // When / Then
        assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> graph.tx());
    }

    @Test
    public void shouldAssignDefaultLabelWhenNoVertexLabel() {
        // Given
        final Graph gafferGraph = getGafferGraph();
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION_1, gafferGraph);

        // Then
        assertThat(graph.addVertex(T.id, MARKO.getId()).label()).isEqualTo(Vertex.DEFAULT_LABEL);
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionForNoVertexId() {
        // Given
        final Graph gafferGraph = getGafferGraph();
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION_1, gafferGraph);

        // Then
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> graph.addVertex(T.label, SOFTWARE))
                .withMessageMatching("ID is required");
    }

    @Test
    public void shouldAddAndGetVertex() {
        // Given
        final Graph gafferGraph = getGafferGraph();
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION_1, gafferGraph);

        // When
        addLopVertex(graph);
        final Iterator<GafferPopVertex> vertices = graph.vertices(Arrays.asList(LOP.getId(), VADAS.getId()), SOFTWARE);

        // Then
        testLopVertex(vertices);
    }

    @Test
    public void shouldAddAndGetVertexWithNullViewAndVertexList() {
        // Given
        final Graph gafferGraph = getGafferGraph();
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION_1, gafferGraph);
        final Vertex lop = addLopVertex(graph);

        // When
        final Iterator<GafferPopVertex> vertices = graph.verticesWithView(Arrays.asList(lop), null);

        // Then
        testLopVertex(vertices);
    }

    @Test
    public void shouldAddAndGetVertexWithNullView() {
        // Given
        final Graph gafferGraph = getGafferGraph();
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION_1, gafferGraph);

        // When
        addLopVertex(graph);
        final Iterator<GafferPopVertex> vertices = graph.verticesWithView(Arrays.asList(LOP.getId()), null);

        // Then
        testLopVertex(vertices);
    }

    @Test
    public void shouldAddAndGetVertexWithViewWithEdges() {
        // Given
        final Graph gafferGraph = getGafferGraph();
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION_1, gafferGraph);
        final View view = new View.Builder()
                .edge(KNOWS)
                .build();

        // When
        addLopVertex(graph);
        final Iterator<GafferPopVertex> vertices = graph.verticesWithView(Arrays.asList(), view);

        // Then
        testLopVertex(vertices);
    }

    @Test
    public void shouldGetAllVertices() {
        // Given
        final Graph gafferGraph = getGafferGraph();
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION_1, gafferGraph);

        // When
        addLopVertex(graph);
        graph.addVertex(T.label, PERSON, T.id, VADAS.getId(), NAME, VADAS.getName());
        final Iterator<Vertex> vertices = graph.vertices();

        // Then
        assertThat(vertices)
                .toIterable()
                .contains(
                        new GafferPopVertex(SOFTWARE, LOP.getId(), graph),
                        new GafferPopVertex(PERSON, VADAS.getId(), graph));
    }

    @Test
    public void shouldTruncateGetAllVertices() {
        // Given
        final Graph gafferGraph = getGafferGraph();
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION_2, gafferGraph);

        // When
        addLopVertex(graph);
        graph.addVertex(T.label, PERSON, T.id, VADAS.getId(), NAME, VADAS.getName());
        final Iterator<Vertex> vertices = graph.vertices();

        // Then
        assertThat(vertices).toIterable().hasSize(1);
    }

    @Test
    public void shouldGetVerticesById() {
        // Given
        final Graph gafferGraph = getGafferGraph();
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION_1, gafferGraph);

        // When
        addLopVertex(graph);
        final Iterator<Vertex> vertices = graph.vertices(LOP.getId());

        // Then
        testLopVertex(vertices);
    }

    @Test
    void shouldGetVerticesWithTSTV() {
        // Given
        final GafferPopGraph graph = GafferPopTstvTestUtils.createTstvGraph();
        Iterator<Vertex> result = graph.vertices(TSTV_ID_STRING);

        // Then
        assertThat(result).toIterable()
                .hasSize(1)
                .extracting(r -> r.id())
                .containsExactly(TSTV_ID);
    }

    @Test
    public void shouldGetAllVerticesInGroup() {
        // Given
        final Graph gafferGraph = getGafferGraph();
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION_1, gafferGraph);

        // When
        addLopVertex(graph);
        graph.addVertex(T.label, PERSON, T.id, VADAS.getId(), NAME, VADAS.getName());
        final Iterator<GafferPopVertex> vertices = graph.vertices(null, SOFTWARE);

        // Then
        assertThat(vertices).toIterable()
                .containsExactly(new GafferPopVertex(SOFTWARE, LOP.getId(), graph));
    }

    @Test
    public void shouldGetVertexWithJsonView() {
        // Given
        final Graph gafferGraph = getGafferGraph();
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION_1, gafferGraph);
        final View view = new View.Builder()
                .entity(SOFTWARE)
                .build();

        // When
        graph.addVertex(T.label, SOFTWARE, T.id, LOP.getId());
        graph.addVertex(T.label, PERSON, T.id, VADAS.getId());

        final Iterator<GafferPopVertex> vertices = graph.verticesWithView(Arrays.asList(LOP.getId(), VADAS.getId()),
                view);

        // Then
        assertThat(vertices)
                .toIterable()
                .hasSize(1)
                .first()
                .hasFieldOrPropertyWithValue("id", LOP.getId())
                .hasFieldOrPropertyWithValue("label", SOFTWARE);
    }

    @Test
    public void shouldAddAndGetEdge() {
        // Given
        final Graph gafferGraph = getGafferGraph();
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION_1, gafferGraph);
        final GafferPopVertex marko = new GafferPopVertex(GafferPopGraph.ID_LABEL, MARKO.getId(), graph);
        final GafferPopVertex lop = new GafferPopVertex(GafferPopGraph.ID_LABEL, LOP.getId(), graph);
        final GafferPopEdge edge = new GafferPopEdge(CREATED, marko, lop, graph);
        graph.addEdge(edge);

        // When
        final Iterator<Edge> edges = graph.edges(MARKO.created(LOP), Direction.OUT, CREATED);

        // Then
        assertThat(edges)
                .toIterable()
                .extracting(e -> e.id())
                .containsExactly(MARKO.created(LOP));
    }

    @Test
    public void shouldAddAndGetEdgeWithLabelInId() {
        // Given
        final Graph gafferGraph = getGafferGraph();
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION_1, gafferGraph);
        final GafferPopVertex marko = new GafferPopVertex(GafferPopGraph.ID_LABEL, MARKO.getId(), graph);
        final GafferPopVertex lop = new GafferPopVertex(GafferPopGraph.ID_LABEL, LOP.getId(), graph);
        final GafferPopEdge edgeToAdd = new GafferPopEdge(CREATED, marko, lop, graph);
        edgeToAdd.property(WEIGHT, 1.5);
        graph.addEdge(edgeToAdd);

        // When
        Iterator<Edge> edges = graph.edges("[" + MARKO.getId() + "," + CREATED + "," + LOP.getId() + "]");

        // Then
        assertThat(edges).toIterable()
                .extracting(edge -> edge.toString())
                .containsExactly(edgeToAdd.toString());
    }

    @Test
    public void shouldGetEdgeInGroup() {
        // Given
        final Graph gafferGraph = getGafferGraph();
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION_1, gafferGraph);
        final GafferPopEdge edge = new GafferPopEdge(CREATED, MARKO.getId(), LOP.getId(), graph);
        graph.addEdge(edge);

        // When
        final Iterator<Edge> edges = graph.edges(null, Direction.BOTH, CREATED);

        // Then
        assertThat(edges).toIterable().contains(edge);
    }

    @Test
    public void shouldGetEdgeInGroupWithViewWithEntity() {
        // Given
        final Graph gafferGraph = getGafferGraph();
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION_1, gafferGraph);
        final GafferPopEdge edge = new GafferPopEdge(CREATED, MARKO.getId(), LOP.getId(), graph);
        graph.addEdge(edge);
        final View view = new View.Builder()
                .entity(CREATED)
                .build();

        // When
        final Iterator<Edge> edges = graph.edgesWithView(MARKO.getId(), Direction.OUT, view);

        // Then
        assertThat(edges).toIterable().contains(edge);
    }

    @Test
    public void shouldGetEdgesWithEdgeIdsPassedIn() {
        // Given
        final Graph gafferGraph = getGafferGraph();
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION_1, gafferGraph);
        final GafferPopEdge createdEdge = new GafferPopEdge(CREATED, MARKO.getId(), LOP.getId(), graph);
        final GafferPopEdge knowsEdge = new GafferPopEdge(KNOWS, MARKO.getId(), JOSH.getId(), graph);
        graph.addEdge(createdEdge);
        graph.addEdge(knowsEdge);
        final View view = new View.Builder()
                .edge(CREATED)
                .build();

        // When
        final Iterator<Edge> edges = graph.edgesWithView(Arrays.asList(createdEdge.id(), knowsEdge.id()), Direction.OUT,
                view);

        // Then
        assertThat(edges).toIterable().containsExactly(createdEdge);
    }

    @Test
    public void shouldGetEdgeWithDirectionAndNullView() {
        // Given
        final Graph gafferGraph = getGafferGraph();
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION_1, gafferGraph);
        final GafferPopEdge outEdge = new GafferPopEdge(CREATED, JOSH.getId(), LOP.getId(), graph);
        final GafferPopEdge inEdge = new GafferPopEdge(KNOWS, MARKO.getId(), JOSH.getId(), graph);
        graph.addEdge(outEdge);
        graph.addEdge(inEdge);

        // When
        final Iterator<Edge> edges = graph.edgesWithView(JOSH.getId(), Direction.OUT, null);

        // Then
        assertThat(edges).toIterable().containsExactly(outEdge);
    }

    @Test
    public void shouldGetEdgeWithDirectionAndNullViewAndEdgeList() {
        // Given
        final Graph gafferGraph = getGafferGraph();
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION_1, gafferGraph);
        final GafferPopEdge outEdge = new GafferPopEdge(CREATED, JOSH.getId(), LOP.getId(), graph);
        final GafferPopEdge inEdge = new GafferPopEdge(KNOWS, MARKO.getId(), JOSH.getId(), graph);
        graph.addEdge(outEdge);
        graph.addEdge(inEdge);

        // When
        final Iterator<Edge> edges = graph.edgesWithView(Arrays.asList(outEdge), Direction.OUT, null);

        // Then
        assertThat(edges).toIterable().containsExactly(outEdge);
    }

    @Test
    public void shouldGetAllEdges() {
        // Given
        final Graph gafferGraph = getGafferGraph();
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION_1, gafferGraph);
        final GafferPopEdge createdEdge = new GafferPopEdge(CREATED, MARKO.getId(), LOP.getId(), graph);
        final GafferPopEdge knowsEdge = new GafferPopEdge(KNOWS, MARKO.getId(), JOSH.getId(), graph);
        graph.addEdge(createdEdge);
        graph.addEdge(knowsEdge);

        // When
        final Iterator<Edge> edges = graph.edges();

        // Then
        assertThat(edges).toIterable().containsExactly(createdEdge, knowsEdge);
    }

    @Test
    public void shouldTruncateGetAllEdges() {
        // Given
        final Graph gafferGraph = getGafferGraph();
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION_2, gafferGraph);
        final GafferPopEdge createdEdge = new GafferPopEdge(CREATED, MARKO.getId(), LOP.getId(), graph);
        final GafferPopEdge knowsEdge = new GafferPopEdge(KNOWS, MARKO.getId(), JOSH.getId(), graph);
        graph.addEdge(createdEdge);
        graph.addEdge(knowsEdge);

        // When
        final Iterator<Edge> edges = graph.edges();

        // Then
        assertThat(edges).toIterable().hasSize(1);
    }

    @Test
    public void shouldGetAllEdgesInGroup() {
        // Given
        final Graph gafferGraph = getGafferGraph();
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION_1, gafferGraph);
        final GafferPopEdge createdEdge = new GafferPopEdge(CREATED, JOSH.getId(), LOP.getId(), graph);
        final GafferPopEdge knowsEdge = new GafferPopEdge(KNOWS, MARKO.getId(), JOSH.getId(), graph);
        graph.addEdge(createdEdge);
        graph.addEdge(knowsEdge);

        // When
        final Iterator<Edge> edges = graph.edges(null, Direction.BOTH, CREATED);

        // Then
        assertThat(edges).toIterable().contains(createdEdge);
    }

    @Test
    public void shouldGetAdjacentVertices() {
        // Given
        final Graph gafferGraph = getGafferGraph();
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION_1, gafferGraph);
        final Vertex lop = addLopVertex(graph);
        final Vertex marko = graph.addVertex(T.label, PERSON, T.id, MARKO.getId(), NAME, MARKO.getName());
        marko.addEdge(CREATED, lop);

        // When
        final Iterator<Vertex> vertices = graph.adjVertices(LOP.getId(), Direction.BOTH);

        // Then
        assertThat(vertices).toIterable()
                .hasSize(1)
                .first()
                .hasFieldOrPropertyWithValue("id", MARKO.getId())
                .hasFieldOrPropertyWithValue("label", PERSON)
                .extracting(v -> v.property(NAME).value())
                .isEqualTo(MARKO.getName());
    }

    @Test
    public void shouldGetAdjacentVerticesWithList() {
        // Given
        final Graph gafferGraph = getGafferGraph();
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION_1, gafferGraph);
        final GafferPopVertex lop = (GafferPopVertex) addLopVertex(graph);
        final GafferPopVertex marko = (GafferPopVertex) graph.addVertex(T.label, PERSON, T.id, MARKO.getId(), NAME,
                MARKO.getName());
        marko.addEdge(CREATED, lop);

        // When
        final Iterator<Vertex> vertices = graph.adjVertices(Arrays.asList(LOP.getId(), MARKO.getId()), Direction.BOTH);

        // Then
        assertThat(vertices).toIterable()
                .contains(lop)
                .contains(marko);
    }

    @Test
    public void shouldThrowExceptionIfGetAdjacentVerticesWithNoSeeds() {
        // Given
        final Graph gafferGraph = getGafferGraph();
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION_1, gafferGraph);

        // When / Then
        assertThatExceptionOfType(UnsupportedOperationException.class)
                .isThrownBy(() -> graph.adjVertices(Collections.emptyList(), Direction.BOTH));
    }

    @Test
    void shouldThrowExceptionWhenPassedInvalidOpChain() {
        // Given
        final Graph gafferGraph = getGafferGraph();
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION_1, gafferGraph);
        final OperationChain<?> invalidOperationChain = new OperationChain.Builder()
                .first(new AddElementsFromSocket())
                .then(new GetElements())
                .build();

        // When / Then
        assertThatExceptionOfType(RuntimeException.class)
                .isThrownBy(() -> graph.execute(invalidOperationChain))
                .withMessageContaining("GafferPop operation failed");
    }

    private Graph getGafferGraph() {
        return GafferPopTestUtil.getGafferGraph(this.getClass(), PROPERTIES);
    }

    private static Vertex addLopVertex(GafferPopGraph graph) {
        return graph.addVertex(T.label, SOFTWARE, T.id, LOP.getId(), NAME, LOP.getName());
    }

    private static void testLopVertex(Iterator<? extends Vertex> vertices) {
        assertThat(vertices)
                .toIterable()
                .hasSize(1)
                .first()
                .hasFieldOrPropertyWithValue("id", LOP.getId())
                .hasFieldOrPropertyWithValue("label", SOFTWARE)
                .extracting(v -> v.property(NAME).value())
                .isEqualTo(LOP.getName());
    }
}