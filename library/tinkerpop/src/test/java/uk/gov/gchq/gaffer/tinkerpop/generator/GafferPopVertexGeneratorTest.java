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
import org.apache.tinkerpop.gremlin.structure.Graph.Features;
import org.apache.tinkerpop.gremlin.structure.Graph.Features.VertexFeatures;
import org.apache.tinkerpop.gremlin.structure.Graph.Features.VertexPropertyFeatures;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraph;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopVertex;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class GafferPopVertexGeneratorTest {

    @Test
    public void shouldConvertGafferEntityToGafferPopReadOnlyEntity() {
        // Given
        final GafferPopGraph graph = mock(GafferPopGraph.class);
        final Features features = mock(Features.class);
        final VertexFeatures vertexFeatures = mock(VertexFeatures.class);
        final VertexPropertyFeatures vertexPropertyFeatures = mock(VertexPropertyFeatures.class);
        given(graph.features()).willReturn(features);
        given(features.vertex()).willReturn(vertexFeatures);
        given(vertexFeatures.supportsNullPropertyValues()).willReturn(true);
        given(vertexFeatures.properties()).willReturn(vertexPropertyFeatures);
        given(vertexPropertyFeatures.supportsNullPropertyValues()).willReturn(true);

        final String vertex = "vertex";
        final String propValue = "property value";
        final Entity entity = new Entity(TestGroups.ENTITY, vertex);
        entity.putProperty(TestPropertyNames.STRING, propValue);
        entity.putProperty(TestPropertyNames.INT, null); // should be skipped

        final GafferPopVertexGenerator generator = new GafferPopVertexGenerator(graph, true);

        // When
        final GafferPopVertex gafferPopVertex = generator._apply(entity);

        // Then
        assertThat(gafferPopVertex.label()).isEqualTo(TestGroups.ENTITY);
        assertThat(gafferPopVertex.id()).isEqualTo(vertex);
        assertThat(gafferPopVertex.property(TestPropertyNames.STRING).value()).isEqualTo(propValue);
        assertThat(gafferPopVertex.properties()).toIterable().hasSize(1);
        assertThat(gafferPopVertex.graph()).isSameAs(graph);
        assertThat(gafferPopVertex.isReadOnly()).isTrue();
    }

    @Test
    public void shouldConvertGafferEntityToGafferPopReadWriteEntity() {
        // Given
        final GafferPopGraph graph = mock(GafferPopGraph.class);
        final Features features = mock(Features.class);
        final VertexFeatures vertexFeatures = mock(VertexFeatures.class);
        final VertexPropertyFeatures vertexPropertyFeatures = mock(VertexPropertyFeatures.class);
        given(graph.features()).willReturn(features);
        given(features.vertex()).willReturn(vertexFeatures);
        given(vertexFeatures.supportsNullPropertyValues()).willReturn(true);
        given(vertexFeatures.properties()).willReturn(vertexPropertyFeatures);
        given(vertexPropertyFeatures.supportsNullPropertyValues()).willReturn(true);

        final String vertex = "vertex";
        final String propValue = "property value";
        final Entity entity = new Entity(TestGroups.ENTITY, vertex);
        entity.putProperty(TestPropertyNames.STRING, propValue);
        entity.putProperty(TestPropertyNames.INT, null); // should be skipped

        final GafferPopVertexGenerator generator = new GafferPopVertexGenerator(graph, false);

        // When
        final GafferPopVertex gafferPopVertex = generator._apply(entity);

        // Then
        assertThat(gafferPopVertex.label()).isEqualTo(TestGroups.ENTITY);
        assertThat(gafferPopVertex.id()).isEqualTo(vertex);
        assertThat(gafferPopVertex.property(TestPropertyNames.STRING).value()).isEqualTo(propValue);
        assertThat(gafferPopVertex.properties()).toIterable().hasSize(1);
        assertThat(gafferPopVertex.graph()).isSameAs(graph);
        assertThat(gafferPopVertex.isReadOnly()).isFalse();
    }

    @Test
    public void shouldThrowExceptionWhenPassedEdge() {
        // Given
        final GafferPopGraph graph = mock(GafferPopGraph.class);
        final Element element = new Edge.Builder().group(TestGroups.EDGE).build();
        final GafferPopVertexGenerator generator = new GafferPopVertexGenerator(graph, true);

        // Then
        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> generator._apply(element));
    }

}
