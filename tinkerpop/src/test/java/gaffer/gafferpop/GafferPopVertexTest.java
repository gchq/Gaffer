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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

import com.google.common.collect.Lists;
import gaffer.commonutil.TestGroups;
import gaffer.commonutil.TestPropertyNames;
import gaffer.commonutil.iterable.CloseableIterator;
import gaffer.data.elementdefinition.view.View;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality;
import org.hamcrest.core.IsCollectionContaining;
import org.junit.Test;
import java.util.ArrayList;
import java.util.Iterator;

public class GafferPopVertexTest {
    @Test
    public void shouldConstructVertex() {
        // Given
        final String id = GafferPopGraph.ID_LABEL;
        final GafferPopGraph graph = mock(GafferPopGraph.class);

        // When
        final GafferPopVertex vertex = new GafferPopVertex(TestGroups.ENTITY, id, graph);

        // Then
        assertEquals(id, vertex.id());
        assertSame(graph, vertex.graph());
        assertTrue(vertex.keys().isEmpty());
    }

    @Test
    public void shouldAddAndGetVertexProperties() {
        // Given
        final String id = GafferPopGraph.ID_LABEL;
        final GafferPopGraph graph = mock(GafferPopGraph.class);
        final GafferPopVertex vertex = new GafferPopVertex(TestGroups.ENTITY, id, graph);
        final String propValue1 = "propValue1";
        final int propValue2 = 10;

        // When
        vertex.property(Cardinality.list, TestPropertyNames.STRING, propValue1);
        vertex.property(Cardinality.list, TestPropertyNames.INT, propValue2);

        // Then
        assertEquals(propValue1, vertex.property(TestPropertyNames.STRING).value());
        assertEquals(propValue2, vertex.property(TestPropertyNames.INT).value());
    }

    @Test
    public void shouldGetIterableOfVertexProperties() {
        // Given
        final String id = GafferPopGraph.ID_LABEL;
        final GafferPopGraph graph = mock(GafferPopGraph.class);
        final GafferPopVertex vertex = new GafferPopVertex(TestGroups.ENTITY, id, graph);
        final String propValue1 = "propValue1";
        final int propValue2 = 10;
        vertex.property(Cardinality.list, TestPropertyNames.STRING, propValue1);
        vertex.property(Cardinality.list, TestPropertyNames.INT, propValue2);

        // When
        final Iterator<VertexProperty<Object>> props = vertex.properties(TestPropertyNames.STRING, TestPropertyNames.INT);

        // Then
        final ArrayList<VertexProperty> propList = Lists.newArrayList(props);
        assertThat(propList, IsCollectionContaining.hasItems(
                new GafferPopVertexProperty<>(vertex, TestPropertyNames.STRING, propValue1),
                new GafferPopVertexProperty<>(vertex, TestPropertyNames.INT, propValue2)
        ));
    }

    @Test
    public void shouldDelegateEdgesToGraph() {
        // Given
        final GafferPopGraph graph = mock(GafferPopGraph.class);
        final GafferPopVertex vertex = new GafferPopVertex(TestGroups.ENTITY, GafferPopGraph.ID_LABEL, graph);
        final Iterator<GafferPopEdge> edges = mock(Iterator.class);
        given(graph.edges(GafferPopGraph.ID_LABEL, Direction.IN, TestGroups.ENTITY)).willReturn(edges);

        // When
        final Iterator<Edge> resultEdges = vertex.edges(Direction.IN, TestGroups.ENTITY);

        // Then
        assertSame(edges, resultEdges);
    }

    @Test
    public void shouldDelegateEdgesWithViewToGraph() {
        // Given
        final GafferPopGraph graph = mock(GafferPopGraph.class);
        final GafferPopVertex vertex = new GafferPopVertex(TestGroups.ENTITY, GafferPopGraph.ID_LABEL, graph);
        final CloseableIterator<GafferPopEdge> edges = mock(CloseableIterator.class);
        final View view = mock(View.class);
        given(graph.edgesWithView(GafferPopGraph.ID_LABEL, Direction.IN, view)).willReturn(edges);

        // When
        final Iterator<GafferPopEdge> resultEdges = vertex.edges(Direction.IN, view);

        // Then
        assertSame(edges, resultEdges);
    }

    @Test
    public void shouldDelegateVerticesToGraph() {
        // Given
        final GafferPopGraph graph = mock(GafferPopGraph.class);
        final GafferPopVertex vertex = new GafferPopVertex(TestGroups.ENTITY, GafferPopGraph.ID_LABEL, graph);
        final CloseableIterator<GafferPopVertex> vertices = mock(CloseableIterator.class);
        given(graph.adjVertices(GafferPopGraph.ID_LABEL, Direction.IN, TestGroups.EDGE)).willReturn(vertices);

        // When
        final Iterator<Vertex> resultVertices = vertex.vertices(Direction.IN, TestGroups.EDGE);

        // Then
        assertSame(vertices, resultVertices);
    }


    @Test
    public void shouldDelegateVerticesWithViewToGraph() {
        // Given
        final GafferPopGraph graph = mock(GafferPopGraph.class);
        final GafferPopVertex vertex = new GafferPopVertex(TestGroups.ENTITY, GafferPopGraph.ID_LABEL, graph);
        final CloseableIterator<GafferPopVertex> vertices = mock(CloseableIterator.class);
        final View view = mock(View.class);
        given(graph.adjVerticesWithView(GafferPopGraph.ID_LABEL, Direction.IN, view)).willReturn(vertices);

        // When
        final Iterator<GafferPopVertex> resultVertices = vertex.vertices(Direction.IN, view);

        // Then
        assertSame(vertices, resultVertices);
    }

    @Test
    public void shouldCreateReadableToString() {
        // Given
        final GafferPopGraph graph = mock(GafferPopGraph.class);
        final GafferPopVertex vertex = new GafferPopVertex(TestGroups.ENTITY, GafferPopGraph.ID_LABEL, graph);

        // When
        final String toString = vertex.toString();

        // Then
        assertEquals("v[BasicEntity-id]", toString);
    }
}