/*
 * Copyright 2017-2023 Crown Copyright
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

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
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
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.user.User;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class GafferPopGraphTest {
    public static final String VERTEX_1 = "1";
    public static final String VERTEX_2 = "2";
    public static final String SOFTWARE_NAME_GROUP = "software";
    public static final String PERSON_GROUP = "person";
    public static final String DEPENDS_ON_EDGE_GROUP = "dependsOn";
    public static final String CREATED_EDGE_GROUP = "created";
    public static final String NAME_PROPERTY = "name";
    public static final String WEIGHT_PROPERTY = "weight";
    public static final String USER_ID = "user01";
    public static final String AUTH_1 = "auth1";
    public static final String AUTH_2 = "auth2";

    private static final AccumuloProperties PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.openStream(GafferPopGraphTest.class, "/gaffer/store.properties"));

    private static final Configuration TEST_CONFIGURATION_1 = new BaseConfiguration() {
        {
            this.setProperty(GafferPopGraph.GRAPH, GafferPopGraph.class.getName());
            this.setProperty(GafferPopGraph.OP_OPTIONS, new String[] {"key1:value1", "key2:value2" });
            this.setProperty(GafferPopGraph.USER_ID, USER_ID);
            this.setProperty(GafferPopGraph.DATA_AUTHS, new String[]{AUTH_1, AUTH_2});
        }
    };

    private static final Configuration TEST_CONFIGURATION_2 = new BaseConfiguration() {
        {
            this.setProperty(GafferPopGraph.OP_OPTIONS, new String[] {"key1:value1", "key2:value2" });
            this.setProperty(GafferPopGraph.USER_ID, USER_ID);
            this.setProperty(GafferPopGraph.DATA_AUTHS, new String[]{AUTH_1, AUTH_2});
            this.setProperty(GafferPopGraph.GRAPH_ID, "Graph1");
            this.setProperty(GafferPopGraph.STORE_PROPERTIES, GafferPopGraphTest.class.getClassLoader().getResource("gaffer/store.properties").getPath().toString());
        }
    };

    private static final Configuration TEST_CONFIGURATION_3 = new BaseConfiguration() {
        {
            this.setProperty(GafferPopGraph.OP_OPTIONS, new String[] {"key1:value1", "key2:value2" });
            this.setProperty(GafferPopGraph.USER_ID, USER_ID);
            this.setProperty(GafferPopGraph.DATA_AUTHS, new String[]{AUTH_1, AUTH_2});
        }
    };


    @Test
    public void shouldConstructGafferPopGraphWithOnlyConfig() {
        // Given
        final User expectedUser = new User.Builder()
                .userId(USER_ID)
                .dataAuths(AUTH_1, AUTH_2)
                .build();

        // When
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION_2);

        // Then
        final Map<String, Object> variables = graph.variables().asMap();
        assertEquals(expectedUser, variables.get(GafferPopGraphVariables.USER));

        final Map<String, String> opOptions = (Map<String, String>) variables.get(GafferPopGraphVariables.OP_OPTIONS);
        assertEquals("value1", opOptions.get("key1"));
        assertEquals("value2", opOptions.get("key2"));
        assertEquals(2, opOptions.size());
        assertEquals(3, variables.size());
    }

    @Test
    public void shouldConstructGafferPopGraphWithConfigFile() {
        // Given
        final User expectedUser = new User.Builder()
                .userId(USER_ID)
                .build();

        // when
        final GafferPopGraph graph = GafferPopGraph.open(GafferPopGraphTest.class.getClassLoader().getResource("gafferpop-tinkerpop-modern.properties").getPath().toString());

        // Then
        final Map<String, Object> variables = graph.variables().asMap();
        assertThat(variables.get(GafferPopGraphVariables.USER)).isEqualTo(expectedUser);

        final Map<String, String> opOptions = (Map<String, String>) variables.get(GafferPopGraphVariables.OP_OPTIONS);
        assertEquals("value1", opOptions.get("key1"));
        assertEquals(1, opOptions.size());
        assertEquals(3, variables.size());
    }

    @Test
    public void shouldConstructGafferPopGraph() {
        // Given
        final Graph gafferGraph = getGafferGraph();
        final User expectedUser = new User.Builder()
                .userId(USER_ID)
                .dataAuths(AUTH_1, AUTH_2)
                .build();

        // When
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION_1, gafferGraph);

        // Then
        final Map<String, Object> variables = graph.variables().asMap();
        assertEquals(gafferGraph.getSchema(), variables.get(GafferPopGraphVariables.SCHEMA));
        assertEquals(expectedUser, variables.get(GafferPopGraphVariables.USER));

        final Map<String, String> opOptions = (Map<String, String>) variables.get(GafferPopGraphVariables.OP_OPTIONS);
        assertEquals("value1", opOptions.get("key1"));
        assertEquals("value2", opOptions.get("key2"));
        assertEquals(2, opOptions.size());
        assertEquals(3, variables.size());
    }

    @Test
    public void shouldThrowUnsupportedExceptionForNoGraphId() {

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
        assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> graph.compute(GraphComputer.class));
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
    public void shouldThrowIllegalArgumentExceptionForNoVertexLabel() {
        // Given
        final Graph gafferGraph = getGafferGraph();
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION_1, gafferGraph);

        //Then
        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> graph.addVertex(T.id, VERTEX_1))
            .withMessageMatching("Label is required");
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionForNoVertexId() {
        // Given
        final Graph gafferGraph = getGafferGraph();
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION_1, gafferGraph);

        //Then
        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> graph.addVertex(T.label, SOFTWARE_NAME_GROUP))
            .withMessageMatching("ID is required");
    }

    @Test
    public void shouldAddAndGetVertex() {
        // Given
        final Graph gafferGraph = getGafferGraph();
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION_1, gafferGraph);

        // When
        graph.addVertex(T.label, SOFTWARE_NAME_GROUP, T.id, VERTEX_1, NAME_PROPERTY, "GafferPop");
        final Iterator<GafferPopVertex> vertices = graph.vertices(Arrays.asList(VERTEX_1, VERTEX_2), SOFTWARE_NAME_GROUP);

        // Then
        final GafferPopVertex vertex = vertices.next();
        assertFalse(vertices.hasNext());
        assertEquals(VERTEX_1, vertex.id());
        assertEquals(SOFTWARE_NAME_GROUP, vertex.label());
        assertEquals("GafferPop", vertex.property(NAME_PROPERTY).value());
    }

    @Test
    public void shouldAddAndGetVertexWithNullViewAndVertexList() {
        // Given
        final Graph gafferGraph = getGafferGraph();
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION_1, gafferGraph);
        final Vertex vertex1 = graph.addVertex(T.label, SOFTWARE_NAME_GROUP, T.id, VERTEX_1, NAME_PROPERTY, "GafferPop");


        // When
        final Iterator<GafferPopVertex> vertices = graph.verticesWithView(Arrays.asList(vertex1), null);

        // Then
        final GafferPopVertex vertex = vertices.next();
        assertFalse(vertices.hasNext()); // there is only 1 vertex
        assertEquals(VERTEX_1, vertex.id());
        assertEquals(SOFTWARE_NAME_GROUP, vertex.label());
        assertEquals("GafferPop", vertex.property(NAME_PROPERTY).value());
    }


    @Test
    public void shouldAddAndGetVertexWithNullView() {
        // Given
        final Graph gafferGraph = getGafferGraph();
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION_1, gafferGraph);


        // When
        graph.addVertex(T.label, SOFTWARE_NAME_GROUP, T.id, VERTEX_1, NAME_PROPERTY, "GafferPop");
        final Iterator<GafferPopVertex> vertices = graph.verticesWithView(Arrays.asList(VERTEX_1), null);

        // Then
        final GafferPopVertex vertex = vertices.next();
        assertFalse(vertices.hasNext()); // there is only 1 vertex
        assertEquals(VERTEX_1, vertex.id());
        assertEquals(SOFTWARE_NAME_GROUP, vertex.label());
        assertEquals("GafferPop", vertex.property(NAME_PROPERTY).value());
    }

    @Test
    public void shouldAddAndGetVertexWithViewWithEdges() {
        // Given
        final Graph gafferGraph = getGafferGraph();
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION_1, gafferGraph);
        final View view = new View.Builder()
                .edge(SOFTWARE_NAME_GROUP)
                .build();



        // When
        graph.addVertex(T.label, SOFTWARE_NAME_GROUP, T.id, VERTEX_1, NAME_PROPERTY, "GafferPop");
        final Iterator<GafferPopVertex> vertices = graph.verticesWithView(Arrays.asList(), view);

        // Then
        final GafferPopVertex vertex = vertices.next();
        assertFalse(vertices.hasNext()); // there is only 1 vertex
        assertEquals(VERTEX_1, vertex.id());
        assertEquals(SOFTWARE_NAME_GROUP, vertex.label());
        assertEquals("GafferPop", vertex.property(NAME_PROPERTY).value());
    }

    @Test
    public void shouldGetAllVertices() {
        // Given
        final Graph gafferGraph = getGafferGraph();
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION_1, gafferGraph);

        // When
        graph.addVertex(T.label, SOFTWARE_NAME_GROUP, T.id, VERTEX_1, NAME_PROPERTY, "GafferPop");
        graph.addVertex(T.label, PERSON_GROUP, T.id, VERTEX_2, NAME_PROPERTY, "Gaffer");
        final Iterator<Vertex> vertices = graph.vertices();

        // Then
        final List<Vertex> verticesList = new ArrayList<>();
        while (vertices.hasNext()) {
            verticesList.add(vertices.next());
        }
        assertThat(verticesList).contains(
                new GafferPopVertex(SOFTWARE_NAME_GROUP, VERTEX_1, graph),
                new GafferPopVertex(SOFTWARE_NAME_GROUP, VERTEX_2, graph)
        );
    }

    @Test
    public void shouldGetVerticesByNonIterableObject() {
        // Given
        final Graph gafferGraph = getGafferGraph();
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION_1, gafferGraph);

        // When
        graph.addVertex(T.label, SOFTWARE_NAME_GROUP, T.id, VERTEX_1, NAME_PROPERTY, "GafferPop");
        final Iterator<Vertex> vertices = graph.vertices(VERTEX_1);


        // Then
        final Vertex vertex = vertices.next();
        assertFalse(vertices.hasNext()); // there is only 1 vertex
        assertEquals(VERTEX_1, vertex.id());
        assertEquals(SOFTWARE_NAME_GROUP, vertex.label());
        assertEquals("GafferPop", vertex.property(NAME_PROPERTY).value());
    }

    @Test
    public void shouldGetAllVerticesInGroup() {
        // Given
        final Graph gafferGraph = getGafferGraph();
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION_1, gafferGraph);

        // When
        graph.addVertex(T.label, SOFTWARE_NAME_GROUP, T.id, VERTEX_1, NAME_PROPERTY, "GafferPop");
        graph.addVertex(T.label, PERSON_GROUP, T.id, VERTEX_2, NAME_PROPERTY, "Gaffer");
        final Iterator<GafferPopVertex> vertices = graph.vertices(null, SOFTWARE_NAME_GROUP);

        // Then
        final List<GafferPopVertex> verticesList = new ArrayList<>();
        while (vertices.hasNext()) {
            verticesList.add(vertices.next());
        }
        assertThat(verticesList).contains(
                    new GafferPopVertex(SOFTWARE_NAME_GROUP, VERTEX_1, graph)
        );
    }

    @Test
    public void shouldGetVertexWithJsonView() {
        // Given
        final Graph gafferGraph = getGafferGraph();
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION_1, gafferGraph);
        final View view = new View.Builder()
                .entity(SOFTWARE_NAME_GROUP)
                .build();

        // When
        graph.addVertex(T.label, SOFTWARE_NAME_GROUP, T.id, VERTEX_1);
        graph.addVertex(T.label, PERSON_GROUP, T.id, VERTEX_2);

        final Iterator<GafferPopVertex> vertices = graph.verticesWithView(Arrays.asList(VERTEX_1, VERTEX_2), view);

        // Then
        final GafferPopVertex vertex = vertices.next();
        assertFalse(vertices.hasNext()); // there is only 1 vertex
        assertEquals(VERTEX_1, vertex.id());
        assertEquals(SOFTWARE_NAME_GROUP, vertex.label());
    }

    @Test
    public void shouldAddAndGetEdge() {
        // Given
        final Graph gafferGraph = getGafferGraph();
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION_1, gafferGraph);
        final GafferPopVertex gafferPopOutVertex = new GafferPopVertex(GafferPopGraph.ID_LABEL, VERTEX_1, graph);
        final GafferPopVertex gafferPopInVertex = new GafferPopVertex(GafferPopGraph.ID_LABEL, VERTEX_2, graph);
        final GafferPopEdge edgeToAdd = new GafferPopEdge(CREATED_EDGE_GROUP, gafferPopOutVertex, gafferPopInVertex, graph);
        edgeToAdd.property(WEIGHT_PROPERTY, 1.5);

        // When
        graph.addEdge(edgeToAdd);
        final Iterator<Edge> edges = graph.edges(Arrays.asList(VERTEX_1, VERTEX_2));

        // Then
        final Edge edge = edges.next();
        assertFalse(edges.hasNext()); // there is only 1 vertex
        assertEquals(VERTEX_1, ((List) edge.id()).get(0));
        assertEquals(VERTEX_2, ((List) edge.id()).get(1));
        assertEquals(CREATED_EDGE_GROUP, edge.label());
        assertEquals(gafferPopInVertex, edge.inVertex());
        assertEquals(gafferPopOutVertex, edge.outVertex());
        assertEquals(1.5, (Double) edge.property(WEIGHT_PROPERTY).value(), 0);
    }

    @Test
    public void shouldGetEdgeInGroup() {
        // Given
        final Graph gafferGraph = getGafferGraph();
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION_1, gafferGraph);
        final GafferPopEdge edgeToAdd1 = new GafferPopEdge(CREATED_EDGE_GROUP, VERTEX_1, VERTEX_2, graph);
        graph.addEdge(edgeToAdd1);

        // When
        final Iterator<GafferPopEdge> edges = graph.edges(VERTEX_1, Direction.OUT, CREATED_EDGE_GROUP);

        // Then
        final List<Edge> edgesList = new ArrayList<>();
        while (edges.hasNext()) {
            edgesList.add(edges.next());
        }
        assertThat(edgesList).contains(edgeToAdd1);
    }

    @Test
    public void shouldGetEdgeInGroupWithViewWithEntity() {
        // Given
        final Graph gafferGraph = getGafferGraph();
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION_1, gafferGraph);
        final GafferPopEdge edgeToAdd1 = new GafferPopEdge(CREATED_EDGE_GROUP, VERTEX_1, VERTEX_2, graph);
        graph.addEdge(edgeToAdd1);
        final View view = new View.Builder()
                .entity(CREATED_EDGE_GROUP)
                .build();

        // When
        final Iterator<GafferPopEdge> edges = graph.edgesWithView(VERTEX_1, Direction.OUT, view);

        // Then
        final List<Edge> edgesList = new ArrayList<>();
        while (edges.hasNext()) {
            edgesList.add(edges.next());
        }
        assertThat(edgesList).contains(edgeToAdd1);
    }

    @Test
    public void shouldGetEdgesWithEdgeIdsPassedIn() {
        // Given
        final Graph gafferGraph = getGafferGraph();
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION_1, gafferGraph);
        final GafferPopEdge edgeToAdd1 = new GafferPopEdge(CREATED_EDGE_GROUP, VERTEX_1, VERTEX_2, graph);
        final GafferPopEdge edgeToAdd2 = new GafferPopEdge(DEPENDS_ON_EDGE_GROUP, VERTEX_2, VERTEX_1, graph);
        graph.addEdge(edgeToAdd1);
        graph.addEdge(edgeToAdd2);
        final View view = new View.Builder()
                .entity(CREATED_EDGE_GROUP)
                .build();

        // When
        final Iterator<GafferPopEdge> edges = graph.edgesWithView(Arrays.asList(edgeToAdd1.id(), edgeToAdd2.id()), Direction.OUT, view);

        // Then
        final List<Edge> edgesList = new ArrayList<>();
        while (edges.hasNext()) {
            edgesList.add(edges.next());
        }

        assertThat(edgesList).contains(edgeToAdd1, edgeToAdd2);
    }
    @Test
    public void shouldGetEdgeInGroupWithNullView() {
        // Given
        final Graph gafferGraph = getGafferGraph();
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION_1, gafferGraph);
        final GafferPopEdge edgeToAdd1 = new GafferPopEdge(CREATED_EDGE_GROUP, VERTEX_1, VERTEX_2, graph);
        graph.addEdge(edgeToAdd1);

        // When
        final Iterator<GafferPopEdge> edges = graph.edgesWithView(VERTEX_1, Direction.OUT, null);

        // Then
        final List<Edge> edgesList = new ArrayList<>();
        while (edges.hasNext()) {
            edgesList.add(edges.next());
        }
        assertThat(edgesList).contains(edgeToAdd1);
    }

    @Test
    public void shouldGetEdgeInGroupWithNullViewAndEdgeList() {
        // Given
        final Graph gafferGraph = getGafferGraph();
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION_1, gafferGraph);
        final GafferPopEdge edgeToAdd1 = new GafferPopEdge(CREATED_EDGE_GROUP, VERTEX_1, VERTEX_2, graph);
        graph.addEdge(edgeToAdd1);

        // When
        final Iterator<GafferPopEdge> edges = graph.edgesWithView(Arrays.asList(edgeToAdd1), Direction.OUT, null);

        // Then
        final List<Edge> edgesList = new ArrayList<>();
        while (edges.hasNext()) {
            edgesList.add(edges.next());
        }
        assertThat(edgesList).contains(edgeToAdd1);
    }

    @Test
    public void shouldGetAllEdges() {
        // Given
        final Graph gafferGraph = getGafferGraph();
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION_1, gafferGraph);
        final GafferPopEdge edgeToAdd1 = new GafferPopEdge(CREATED_EDGE_GROUP, VERTEX_1, VERTEX_2, graph);
        final GafferPopEdge edgeToAdd2 = new GafferPopEdge(DEPENDS_ON_EDGE_GROUP, VERTEX_2, VERTEX_1, graph);
        graph.addEdge(edgeToAdd1);
        graph.addEdge(edgeToAdd2);

        // When
        final Iterator<Edge> edges = graph.edges();

        // Then
        final List<Edge> edgesList = new ArrayList<>();
        while (edges.hasNext()) {
            edgesList.add(edges.next());
        }
        assertThat(edgesList).contains(edgeToAdd1, edgeToAdd2);
    }

    @Test
    public void shouldGetAllEdgesInGroup() {
        // Given
        final Graph gafferGraph = getGafferGraph();
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION_1, gafferGraph);
        final GafferPopEdge edgeToAdd1 = new GafferPopEdge(CREATED_EDGE_GROUP, VERTEX_1, VERTEX_2, graph);
        final GafferPopEdge edgeToAdd2 = new GafferPopEdge(DEPENDS_ON_EDGE_GROUP, VERTEX_2, VERTEX_1, graph);
        graph.addEdge(edgeToAdd1);
        graph.addEdge(edgeToAdd2);

        // When
        final Iterator<GafferPopEdge> edges = graph.edges(null, Direction.OUT, CREATED_EDGE_GROUP);

        // Then
        final List<Edge> edgesList = new ArrayList<>();
        while (edges.hasNext()) {
            edgesList.add(edges.next());
        }
        assertThat(edgesList).contains(edgeToAdd1);
    }

    @Test
    public void shouldGetAdjacentVertices() {
        // Given
        final Graph gafferGraph = getGafferGraph();
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION_1, gafferGraph);
        final Vertex vertex1 = graph.addVertex(T.label, SOFTWARE_NAME_GROUP, T.id, VERTEX_1, NAME_PROPERTY, "GafferPop");
        final Vertex vertex2 = graph.addVertex(T.label, SOFTWARE_NAME_GROUP, T.id, VERTEX_2, NAME_PROPERTY, "Gaffer");
        vertex1.addEdge(DEPENDS_ON_EDGE_GROUP, vertex2);

        // When
        final Iterator<GafferPopVertex> vertices = graph.adjVertices(VERTEX_1, Direction.BOTH);

        // Then
        final GafferPopVertex vertex = vertices.next();
        assertFalse(vertices.hasNext()); // there is only 1 vertex
        assertEquals(VERTEX_2, vertex.id());
        assertEquals(SOFTWARE_NAME_GROUP, vertex.label());
        assertEquals("Gaffer", vertex.property(NAME_PROPERTY).value());
    }

    @Test
    public void shouldGetAdjacentVerticesWithList() {
        // Given
        final Graph gafferGraph = getGafferGraph();
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION_1, gafferGraph);
        final Vertex vertex1 = graph.addVertex(T.label, SOFTWARE_NAME_GROUP, T.id, VERTEX_1, NAME_PROPERTY, "GafferPop");
        final Vertex vertex2 = graph.addVertex(T.label, SOFTWARE_NAME_GROUP, T.id, VERTEX_2, NAME_PROPERTY, "Gaffer");
        vertex1.addEdge(DEPENDS_ON_EDGE_GROUP, vertex2);

        // When
        final Iterator<GafferPopVertex> vertices = graph.adjVertices(Arrays.asList(VERTEX_1, VERTEX_2), Direction.BOTH);

        // Then
        final List<Vertex>  verticesList = new ArrayList<>();
        while (vertices.hasNext()) {
            verticesList.add(vertices.next());
        }
        assertThat(verticesList).contains(vertex1);
        assertThat(verticesList).contains(vertex2);
    }



    @Test
    public void shouldThrowExceptionIfGetAdjacentVerticesWithNoSeeds() {
        // Given
        final Graph gafferGraph = getGafferGraph();
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION_1, gafferGraph);

        // When / Then
        assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> graph.adjVertices(Collections.emptyList(), Direction.BOTH));
    }

    private Graph getGafferGraph() {
        return new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("graph1")
                        .build())
                .storeProperties(PROPERTIES)
                .addSchemas(StreamUtil.openStreams(this.getClass(), "/gaffer/schema"))
                .build();
    }

}
