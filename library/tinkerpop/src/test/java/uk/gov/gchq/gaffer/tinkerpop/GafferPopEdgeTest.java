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

import com.google.common.collect.Lists;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;

import java.util.ArrayList;
import java.util.Iterator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

public class GafferPopEdgeTest {

    public static String source = "source";
    public static String dest = "dest";
    public static GafferPopGraph graph = mock(GafferPopGraph.class);
    public static String propValue1 = "propValue1";
    public static int propValue2 = 10;

    @Test
    public void shouldConstructEdge() {
        // Given
        final GafferPopVertex outVertex = new GafferPopVertex(GafferPopGraph.ID_LABEL, source, graph);
        final GafferPopVertex inVertex = new GafferPopVertex(GafferPopGraph.ID_LABEL, dest, graph);

        // When
        final GafferPopEdge edge = new GafferPopEdge(TestGroups.EDGE, outVertex, inVertex, graph);

        // Then
        assertEquals(source, edge.outVertex().id());
        assertEquals(dest, edge.inVertex().id());
        assertSame(outVertex, edge.outVertex());
        assertSame(inVertex, edge.inVertex());
        final Iterator<Vertex> vertices = edge.bothVertices();
        assertSame(outVertex, vertices.next());
        assertSame(inVertex, vertices.next());
        assertSame(graph, edge.graph());
        assertTrue(edge.keys().isEmpty());
    }

    @Test
    public void shouldChangeVertexLabelToGraphLabel() {
        // Given
        final GafferPopVertex outVertex = new GafferPopVertex("label", source, graph);
        final GafferPopVertex inVertex = new GafferPopVertex("label", dest, graph);

        // When
        final GafferPopEdge edge = new GafferPopEdge(TestGroups.EDGE, outVertex, inVertex, graph);

        // Then
        assertSame("id", edge.outVertex().label());
        assertSame("id", edge.inVertex().label());
    }

    @Test
    public void shouldAddAndGetEdgeProperties() {
        // Given
        final GafferPopEdge edge = new GafferPopEdge(TestGroups.EDGE, source, dest, graph);

        // When
        edge.property(TestPropertyNames.STRING, propValue1);
        edge.property(TestPropertyNames.INT, propValue2);

        // Then
        assertEquals(propValue1, edge.property(TestPropertyNames.STRING).value());
        assertEquals(propValue2, edge.property(TestPropertyNames.INT).value());
    }

    @Test
    public void shouldGetIterableOfEdgeProperties() {
        // Given
        final GafferPopEdge edge = new GafferPopEdge(TestGroups.EDGE, source, dest, graph);
        edge.property(TestPropertyNames.STRING, propValue1);
        edge.property(TestPropertyNames.INT, propValue2);

        // When
        final Iterator<Property<Object>> props = edge.properties(TestPropertyNames.STRING, TestPropertyNames.INT);

        // Then
        final ArrayList<Property> propList = Lists.newArrayList(props);
        assertThat(propList).contains(
                new GafferPopProperty<>(edge, TestPropertyNames.STRING, propValue1),
                new GafferPopProperty<>(edge, TestPropertyNames.INT, propValue2)
        );
    }


    @Test
    public void shouldGetIterableOfSingleEdgeProperty() {
        // Given
        final GafferPopEdge edge = new GafferPopEdge(TestGroups.EDGE, source, dest, graph);
        edge.property(TestPropertyNames.STRING, propValue1);

        // When
        final Iterator<Property<Object>> props = edge.properties(TestPropertyNames.STRING);

        // Then
        final ArrayList<Property> propList = Lists.newArrayList(props);
        assertThat(propList).contains(
                new GafferPopProperty<>(edge, TestPropertyNames.STRING, propValue1)
        );
    }


    @Test
    public void shouldCreateReadableToString() {
        // Given
        final GafferPopEdge edge = new GafferPopEdge(TestGroups.EDGE, source, dest, graph);

        // When
        final String toString = edge.toString();

        // Then
        assertEquals("e[source-BasicEdge->dest]", toString);
    }

    @Test
    public void shouldReturnOutVertex() {
        // Given
        final GafferPopVertex outVertex = new GafferPopVertex(GafferPopGraph.ID_LABEL, source, graph);
        final GafferPopVertex inVertex = new GafferPopVertex(GafferPopGraph.ID_LABEL, dest, graph);

        // When
        final GafferPopEdge edge = new GafferPopEdge(TestGroups.EDGE, outVertex, inVertex, graph);

        // Then
        assertThat(edge.vertices(Direction.OUT)).toIterable().containsExactly(outVertex);
    }

    @Test
    public void shouldReturnInVertex() {
        // Given
        final GafferPopVertex outVertex = new GafferPopVertex(GafferPopGraph.ID_LABEL, source, graph);
        final GafferPopVertex inVertex = new GafferPopVertex(GafferPopGraph.ID_LABEL, dest, graph);

        // When
        final GafferPopEdge edge = new GafferPopEdge(TestGroups.EDGE, outVertex, inVertex, graph);

        // Then
        assertThat(edge.vertices(Direction.IN)).toIterable().containsExactly(inVertex);
    }

    @Test
    public void shouldThrowExceptionWhenReadOnlyIsTrue() {
        // Given
        final GafferPopEdge edge = new GafferPopEdge(TestGroups.EDGE, source, dest, graph);

        // When
        edge.setReadOnly();

        // Then
        assertThatExceptionOfType(UnsupportedOperationException.class)
            .isThrownBy(() -> edge.property(TestPropertyNames.STRING, propValue1))
            .withMessageMatching("Updates are not supported");

    }
}
