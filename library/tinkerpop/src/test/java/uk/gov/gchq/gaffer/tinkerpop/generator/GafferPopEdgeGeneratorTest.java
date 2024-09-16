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

package uk.gov.gchq.gaffer.tinkerpop.generator;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopEdge;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraph;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;

public class GafferPopEdgeGeneratorTest {
    @Test
    public void shouldConvertGafferEdgeToGafferPopReadOnlyEdge() {
        // Given
        final GafferPopGraph graph = mock(GafferPopGraph.class);
        when(graph.execute(Mockito.any())).thenReturn(new ArrayList<>());

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
        assertThat(gafferPopEdge.label()).isEqualTo(TestGroups.EDGE);
        assertThat(gafferPopEdge.outVertex().id()).isEqualTo(source);
        assertThat(gafferPopEdge.inVertex().id()).isEqualTo(dest);
        assertThat(gafferPopEdge.property(TestPropertyNames.STRING).value()).isEqualTo(propValue);
        assertThat(gafferPopEdge.properties()).toIterable().hasSize(1);
        assertThat(gafferPopEdge.graph()).isSameAs(graph);
        assertThat(gafferPopEdge.isReadOnly()).isTrue();
    }

    @Test
    public void shouldConvertGafferEdgeToGafferPopReadWriteEdge() {
        // Given
        final GafferPopGraph graph = mock(GafferPopGraph.class);
        when(graph.execute(Mockito.any())).thenReturn(new ArrayList<>());

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
        assertThat(gafferPopEdge.label()).isEqualTo(TestGroups.EDGE);
        assertThat(gafferPopEdge.outVertex().id()).isEqualTo(source);
        assertThat(gafferPopEdge.inVertex().id()).isEqualTo(dest);
        assertThat(gafferPopEdge.property(TestPropertyNames.STRING).value()).isEqualTo(propValue);
        assertThat(gafferPopEdge.properties()).toIterable().hasSize(1);
        assertThat(gafferPopEdge.graph()).isSameAs(graph);
        assertThat(gafferPopEdge.isReadOnly()).isFalse();
    }

    @Test
    public void shouldThrowExceptionWhenPassedEntity() {
        // Given
        final GafferPopGraph graph = mock(GafferPopGraph.class);
        final Element element = new Entity.Builder().group(TestGroups.ENTITY).build();
        final GafferPopEdgeGenerator generator = new GafferPopEdgeGenerator(graph, true);

        // Then
        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> generator._apply(element));
    }
}
