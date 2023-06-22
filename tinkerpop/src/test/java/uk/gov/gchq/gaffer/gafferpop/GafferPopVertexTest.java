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
package uk.gov.gchq.gaffer.gafferpop;

import com.google.common.collect.Lists;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph.Features;
import org.apache.tinkerpop.gremlin.structure.Graph.Features.VertexFeatures;
import org.apache.tinkerpop.gremlin.structure.Graph.Features.VertexPropertyFeatures;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;

import java.util.ArrayList;
import java.util.Iterator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

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
        final Features features = mock(Features.class);
        final VertexFeatures vertexFeatures = mock(VertexFeatures.class);
        final VertexPropertyFeatures vertexPropertyFeatures = mock(VertexPropertyFeatures.class);

        final GafferPopVertex vertex = new GafferPopVertex(TestGroups.ENTITY, id, graph);
        final String propValue1 = "propValue1";
        final int propValue2 = 10;

        given(graph.features()).willReturn(features);
        given(features.vertex()).willReturn(vertexFeatures);
        given(vertexFeatures.supportsNullPropertyValues()).willReturn(true);
        given(vertexFeatures.properties()).willReturn(vertexPropertyFeatures);
        given(vertexPropertyFeatures.supportsNullPropertyValues()).willReturn(true);

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
        final Features features = mock(Features.class);
        final VertexFeatures vertexFeatures = mock(VertexFeatures.class);
        final VertexPropertyFeatures vertexPropertyFeatures = mock(VertexPropertyFeatures.class);

        final GafferPopVertex vertex = new GafferPopVertex(TestGroups.ENTITY, id, graph);
        final String propValue1 = "propValue1";
        final int propValue2 = 10;

        given(graph.features()).willReturn(features);
        given(features.vertex()).willReturn(vertexFeatures);
        given(vertexFeatures.supportsNullPropertyValues()).willReturn(true);
        given(vertexFeatures.properties()).willReturn(vertexPropertyFeatures);
        given(vertexPropertyFeatures.supportsNullPropertyValues()).willReturn(true);

        vertex.property(Cardinality.list, TestPropertyNames.STRING, propValue1);
        vertex.property(Cardinality.list, TestPropertyNames.INT, propValue2);

        // When
        final Iterator<VertexProperty<Object>> props = vertex.properties(TestPropertyNames.STRING, TestPropertyNames.INT);

        // Then
        final ArrayList<VertexProperty> propList = Lists.newArrayList(props);
        assertThat(propList).contains(
                new GafferPopVertexProperty<>(vertex, TestPropertyNames.STRING, propValue1),
                new GafferPopVertexProperty<>(vertex, TestPropertyNames.INT, propValue2)
        );
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
        final Iterator<GafferPopEdge> edges = mock(Iterator.class);
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
        final Iterator<GafferPopVertex> vertices = mock(Iterator.class);
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
        final Iterator<GafferPopVertex> vertices = mock(Iterator.class);
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
