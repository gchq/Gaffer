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
package uk.gov.gchq.gaffer.gafferpop.generator;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.gafferpop.GafferPopEdge;
import uk.gov.gchq.gaffer.gafferpop.GafferPopGraph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

public class GafferEdgeGeneratorTest {
    @Test
    public void shouldConvertGafferPopEdgeToGafferEdge() {
        // Given
        final GafferPopGraph graph = mock(GafferPopGraph.class);

        final String source = "source";
        final String dest = "dest";
        final String propValue = "property value";
        final GafferPopEdge gafferPopEdge = new GafferPopEdge(TestGroups.EDGE, source, dest, graph);
        gafferPopEdge.property(TestPropertyNames.STRING, propValue);

        final GafferEdgeGenerator generator = new GafferEdgeGenerator();

        // When
        final Edge edge = generator._apply(gafferPopEdge);

        // Then
        assertEquals(TestGroups.EDGE, edge.getGroup());
        assertEquals(source, edge.getSource());
        assertEquals(dest, edge.getDestination());
        assertTrue(edge.isDirected());
        assertEquals(1, edge.getProperties().size());
        assertEquals(propValue, edge.getProperty(TestPropertyNames.STRING));
    }

}
