/*
 * Copyright 2023 Crown Copyright
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopEdge;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopElement;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraph;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopVertex;

public class GafferPopElementGeneratorTest {

    @Test
    public void shouldReturnGafferPopReadOnlyEntity(){
        // Mock graph
        final GafferPopGraph graph = mock(GafferPopGraph.class);

        // Mock element as an entity
        final Element element = new Entity.Builder().group(TestGroups.ENTITY).build();

        // Mock a GafferPopVertex
        final String id = GafferPopGraph.ID_LABEL;
        final GafferPopVertex vertex = new GafferPopVertex(TestGroups.ENTITY, id, graph);

        // Pass element to the generator
        final GafferPopElementGenerator generator = new GafferPopElementGenerator(graph, true);
        final GafferPopElement gafferPopElement = generator._apply(element);

        assertThat(gafferPopElement.label()).isEqualTo(vertex.label());
        assertThat(gafferPopElement.isReadOnly()).isTrue();
    }

    @Test
    public void shouldReturnGafferPopReadOnlyEdge(){
        // Mock graph
        final GafferPopGraph graph = mock(GafferPopGraph.class);

        // Mock element as an edge
        final Element element = new Edge.Builder().group(TestGroups.EDGE).build();

        // Mock a GafferPopEdge
        final String source = "source";
        final String dest = "dest";
        final GafferPopVertex outVertex = new GafferPopVertex(GafferPopGraph.ID_LABEL, source, graph);
        final GafferPopVertex inVertex = new GafferPopVertex(GafferPopGraph.ID_LABEL, dest, graph);
        final GafferPopEdge edge = new GafferPopEdge(TestGroups.EDGE, outVertex, inVertex, graph);

        // Pass element to the generator
        final GafferPopElementGenerator generator = new GafferPopElementGenerator(graph, true);
        final GafferPopElement gafferPopElement = generator._apply(element);

        assertThat(gafferPopElement.label()).isEqualTo(edge.label());
        assertThat(gafferPopElement.isReadOnly()).isTrue();
    }

    @Test
    public void shouldThrowExceptionForInvalidElement(){
        final GafferPopGraph graph = mock(GafferPopGraph.class);

        // Mock invalid element
        final Element element = mock(Element.class);
        
        // Pass element to the generator
        final GafferPopElementGenerator generator = new GafferPopElementGenerator(graph, true);

        //Check exception is thrown
        assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> generator._apply(element)).withMessageMatching("GafferPopElement has to be Edge or Entity");
    }
}
