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

package uk.gov.gchq.gaffer.tinkerpop.generator;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopEdge;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

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
        final GafferPopEdge gafferPopEdge = generator._apply(edge);

        // Then
        assertEquals(TestGroups.EDGE, gafferPopEdge.label());
        assertEquals(source, gafferPopEdge.outVertex().id());
        assertEquals(dest, gafferPopEdge.inVertex().id());
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
        final GafferPopEdge gafferPopEdge = generator._apply(edge);

        // Then
        assertEquals(TestGroups.EDGE, gafferPopEdge.label());
        assertEquals(source, gafferPopEdge.outVertex().id());
        assertEquals(dest, gafferPopEdge.inVertex().id());
        assertEquals(propValue, gafferPopEdge.property(TestPropertyNames.STRING).value());
        assertEquals(1, Lists.newArrayList(gafferPopEdge.properties()).size());
        assertSame(graph, gafferPopEdge.graph());
        assertFalse(gafferPopEdge.isReadOnly());
    }
}
