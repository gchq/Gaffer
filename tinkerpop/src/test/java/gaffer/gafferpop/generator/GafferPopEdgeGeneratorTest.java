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
package gaffer.gafferpop.generator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import com.google.common.collect.Lists;
import gaffer.commonutil.TestGroups;
import gaffer.commonutil.TestPropertyNames;
import gaffer.data.element.Edge;
import gaffer.gafferpop.GafferPopEdge;
import gaffer.gafferpop.GafferPopGraph;
import org.junit.Test;

public class GafferPopEdgeGeneratorTest {
    @Test
    public void shouldConvertGafferEdgeToGafferPopReadOnlyEdge() {
        // Given
        final GafferPopGraph graph = mock(GafferPopGraph.class);

        final String source = "source";
        final String dest = "dest";
        final String propValue = "property value";
        final Edge edge = new Edge(TestGroups.EDGE, source, dest, true);
        edge.putProperty(TestPropertyNames.STRING, propValue);
        edge.putProperty(TestPropertyNames.INT, null); // should be skipped

        final GafferPopEdgeGenerator generator = new GafferPopEdgeGenerator(graph, true);

        // When
        final GafferPopEdge gafferPopEdge = generator.getObject(edge);

        // Then
        assertEquals(TestGroups.EDGE, gafferPopEdge.label());
        assertEquals(source, gafferPopEdge.id().getSource());
        assertEquals(dest, gafferPopEdge.id().getDest());
        assertEquals(propValue, gafferPopEdge.property(TestPropertyNames.STRING).value());
        assertEquals(1, Lists.newArrayList(gafferPopEdge.properties()).size());
        assertSame(graph, gafferPopEdge.graph());
        assertTrue(gafferPopEdge.isReadOnly());
    }

    @Test
    public void shouldConvertGafferEdgeToGafferPopReadWriteEdge() {
        // Given
        final GafferPopGraph graph = mock(GafferPopGraph.class);

        final String source = "source";
        final String dest = "dest";
        final String propValue = "property value";
        final Edge edge = new Edge(TestGroups.EDGE, source, dest, true);
        edge.putProperty(TestPropertyNames.STRING, propValue);
        edge.putProperty(TestPropertyNames.INT, null); // should be skipped

        final GafferPopEdgeGenerator generator = new GafferPopEdgeGenerator(graph, false);

        // When
        final GafferPopEdge gafferPopEdge = generator.getObject(edge);

        // Then
        assertEquals(TestGroups.EDGE, gafferPopEdge.label());
        assertEquals(source, gafferPopEdge.id().getSource());
        assertEquals(dest, gafferPopEdge.id().getDest());
        assertEquals(propValue, gafferPopEdge.property(TestPropertyNames.STRING).value());
        assertEquals(1, Lists.newArrayList(gafferPopEdge.properties()).size());
        assertSame(graph, gafferPopEdge.graph());
        assertFalse(gafferPopEdge.isReadOnly());
    }

    @Test
    public void shouldConvertGafferPopEdgeToGafferEdge() {
        // Given
        final GafferPopGraph graph = mock(GafferPopGraph.class);

        final String source = "source";
        final String dest = "dest";
        final String propValue = "property value";
        final GafferPopEdge gafferPopEdge = new GafferPopEdge(TestGroups.EDGE, source, dest, graph);
        gafferPopEdge.property(TestPropertyNames.STRING, propValue);

        final GafferPopEdgeGenerator generator = new GafferPopEdgeGenerator(graph);

        // When
        final Edge edge = generator.getElement(gafferPopEdge);

        // Then
        assertEquals(TestGroups.EDGE, edge.getGroup());
        assertEquals(source, edge.getSource());
        assertEquals(dest, edge.getDestination());
        assertTrue(edge.isDirected());
        assertEquals(1, edge.getProperties().size());
        assertEquals(propValue, edge.getProperty(TestPropertyNames.STRING));
    }

}