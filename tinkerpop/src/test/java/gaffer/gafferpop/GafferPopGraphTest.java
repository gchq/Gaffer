/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package gaffer.gafferpop;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import gaffer.commonutil.StreamUtil;
import gaffer.data.elementdefinition.view.View;
import gaffer.graph.Graph;
import gaffer.user.User;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.hamcrest.core.IsCollectionContaining;
import org.junit.Test;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class GafferPopGraphTest {
    public static final int VERTEX_1 = 1;
    public static final int VERTEX_2 = 2;
    public static final String SOFTWARE_NAME_GROUP = "software";
    public static final String PERSON_GROUP = "person";
    public static final String DEPENDS_ON_EDGE_GROUP = "dependsOn";
    public static final String CREATED_EDGE_GROUP = "created";
    public static final String NAME_PROPERTY = "name";
    public static final String WEIGHT_PROPERTY = "weight";
    public static final String USER_ID = "user01";
    public static final String AUTH_1 = "auth1";
    public static final String AUTH_2 = "auth2";

    private static final Configuration TEST_CONFIGURATION = new BaseConfiguration() {{
        this.setProperty(GafferPopGraph.GRAPH, GafferPopGraph.class.getName());
        this.setProperty(GafferPopGraph.OP_OPTIONS, new String[]{"key1:value1", "key2:value2"});
        this.setProperty(GafferPopGraph.USER_ID, USER_ID);
        this.setProperty(GafferPopGraph.DATA_AUTHS, new String[]{AUTH_1, AUTH_2});
    }};

    @Test
    public void shouldConstructGafferPopGraph() {
        // Given
        final Graph gafferGraph = getGafferGraph();
        final User expectedUser = new User.Builder()
                .userId(USER_ID)
                .dataAuths(AUTH_1, AUTH_2)
                .build();

        // When
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION, gafferGraph);

        // Then - there is 1 vertex and no edges
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
    public void shouldThrowUnsupportedExceptionForCompute() {
        // Given
        final Graph gafferGraph = getGafferGraph();
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION, gafferGraph);

        // When / Then
        try {
            graph.compute();
            fail("Exception expected");
        } catch (final UnsupportedOperationException e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void shouldThrowUnsupportedExceptionForComputeWithClass() {
        // Given
        final Graph gafferGraph = getGafferGraph();
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION, gafferGraph);

        // When / Then
        try {
            graph.compute(GraphComputer.class);
            fail("Exception expected");
        } catch (final UnsupportedOperationException e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void shouldThrowUnsupportedExceptionForTx() {
        // Given
        final Graph gafferGraph = getGafferGraph();
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION, gafferGraph);

        // When / Then
        try {
            graph.tx();
            fail("Exception expected");
        } catch (final UnsupportedOperationException e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void shouldAddAndGetVertex() {
        // Given
        final Graph gafferGraph = getGafferGraph();
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION, gafferGraph);

        // When
        graph.addVertex(T.label, SOFTWARE_NAME_GROUP, T.id, VERTEX_1, NAME_PROPERTY, "GafferPop");
        final Iterator<GafferPopVertex> vertices = graph.vertices(Arrays.asList(VERTEX_1, VERTEX_2), SOFTWARE_NAME_GROUP);

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
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION, gafferGraph);

        // When
        graph.addVertex(T.label, SOFTWARE_NAME_GROUP, T.id, VERTEX_1, NAME_PROPERTY, "GafferPop");
        graph.addVertex(T.label, PERSON_GROUP, T.id, VERTEX_2, NAME_PROPERTY, "Gaffer");
        final Iterator<Vertex> vertices = graph.vertices();

        // Then
        final List<Vertex> verticesList = new ArrayList<>();
        while (vertices.hasNext()) {
            verticesList.add(vertices.next());
        }
        assertThat(verticesList, IsCollectionContaining.hasItems(
                new GafferPopVertex(SOFTWARE_NAME_GROUP, VERTEX_1, graph),
                new GafferPopVertex(SOFTWARE_NAME_GROUP, VERTEX_2, graph)));
    }

    @Test
    public void shouldGetAllVerticesInGroup() {
        // Given
        final Graph gafferGraph = getGafferGraph();
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION, gafferGraph);

        // When
        graph.addVertex(T.label, SOFTWARE_NAME_GROUP, T.id, VERTEX_1, NAME_PROPERTY, "GafferPop");
        graph.addVertex(T.label, PERSON_GROUP, T.id, VERTEX_2, NAME_PROPERTY, "Gaffer");
        final Iterator<GafferPopVertex> vertices = graph.vertices(null, SOFTWARE_NAME_GROUP);

        // Then
        final List<GafferPopVertex> verticesList = new ArrayList<>();
        while (vertices.hasNext()) {
            verticesList.add(vertices.next());
        }
        assertThat(verticesList, IsCollectionContaining.hasItems(
                new GafferPopVertex(SOFTWARE_NAME_GROUP, VERTEX_1, graph)));
    }

    @Test
    public void shouldGetVertexWithJsonView() {
        // Given
        final Graph gafferGraph = getGafferGraph();
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION, gafferGraph);
        final View view = new View.Builder()
                .entity(SOFTWARE_NAME_GROUP)
                .build();

        // When
        graph.addVertex(T.label, SOFTWARE_NAME_GROUP, T.id, VERTEX_1);
        graph.addVertex(T.label, PERSON_GROUP, T.id, VERTEX_2);

        final Iterator<GafferPopVertex> vertices = graph.vertices(Arrays.asList(VERTEX_1, VERTEX_2), view.toString());

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
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION, gafferGraph);
        final GafferPopEdge edgeToAdd = new GafferPopEdge(CREATED_EDGE_GROUP, VERTEX_1, VERTEX_2, graph);
        edgeToAdd.property(WEIGHT_PROPERTY, 1.5);

        // When
        graph.addEdge(edgeToAdd);
        final Iterator<Edge> edges = graph.edges(new EdgeId(VERTEX_1, VERTEX_2));

        // Then
        final Edge edge = edges.next();
        assertFalse(edges.hasNext()); // there is only 1 vertex
        assertEquals(VERTEX_1, ((EdgeId) edge.id()).getSource());
        assertEquals(VERTEX_2, ((EdgeId) edge.id()).getDest());
        assertEquals(CREATED_EDGE_GROUP, edge.label());
        assertEquals(1.5, (Double) edge.property(WEIGHT_PROPERTY).value(), 0);
    }

    @Test
    public void shouldGetAllEdges() {
        // Given
        final Graph gafferGraph = getGafferGraph();
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION, gafferGraph);
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
        assertThat(edgesList, IsCollectionContaining.hasItems(
                edgeToAdd1, edgeToAdd2));
    }

    @Test
    public void shouldGetAllEdgesInGroup() {
        // Given
        final Graph gafferGraph = getGafferGraph();
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION, gafferGraph);
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
        assertThat(edgesList, IsCollectionContaining.hasItems(
                edgeToAdd1));
    }

    @Test
    public void shouldGetAdjacentVertices() {
        // Given
        final Graph gafferGraph = getGafferGraph();
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION, gafferGraph);
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
    public void shouldThrowExceptionIfGetAdjacentVerticesWithNoSeeds() {
        // Given
        final Graph gafferGraph = getGafferGraph();
        final GafferPopGraph graph = GafferPopGraph.open(TEST_CONFIGURATION, gafferGraph);

        // When / Then
        try {
            graph.adjVertices(Collections.emptyList(), Direction.BOTH);
            fail("Exception expected");
        } catch (final UnsupportedOperationException e) {
            assertNotNull(e.getMessage());
        }
    }

    private Graph getGafferGraph() {
        return new Graph.Builder()
                .storeProperties(StreamUtil.openStream(this.getClass(), "/gaffer/store.properties", true))
                .addSchemas(StreamUtil.openStreams(this.getClass(), "/gaffer/schema", true))
                .build();
    }

}