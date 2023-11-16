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

import org.apache.tinkerpop.gremlin.structure.Graph.Features;
import org.apache.tinkerpop.gremlin.structure.Graph.Features.VertexFeatures;
import org.apache.tinkerpop.gremlin.structure.Graph.Features.VertexPropertyFeatures;
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraph;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopVertex;

import java.util.AbstractMap.SimpleEntry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

class GafferEntityGeneratorTest {

    @Test
    void shouldConvertBasicGafferPopVertex() {
        // Given
        final String vertexNoPropsId = "vertexNoProps";
        final String vertexWithPropsId = "vertexWithProps";
        final String propValue = "propertyValue";

        // Mock relevant bits so we can make a vertex with properties
        final GafferPopGraph graph = mock(GafferPopGraph.class);
        final Features features = mock(Features.class);
        final VertexFeatures vertexFeatures = mock(VertexFeatures.class);
        final VertexPropertyFeatures vertexPropertyFeatures = mock(VertexPropertyFeatures.class);
        given(graph.features()).willReturn(features);
        given(features.vertex()).willReturn(vertexFeatures);
        given(vertexFeatures.supportsNullPropertyValues()).willReturn(true);
        given(vertexFeatures.properties()).willReturn(vertexPropertyFeatures);
        given(vertexPropertyFeatures.supportsNullPropertyValues()).willReturn(true);

        // Create vertexes with and without properties
        final GafferPopVertex vertexNoProps = new GafferPopVertex(TestGroups.ENTITY, vertexNoPropsId, graph);
        final GafferPopVertex vertexWithProps = new GafferPopVertex(TestGroups.ENTITY, vertexWithPropsId, graph);
        vertexWithProps.property(Cardinality.list, TestPropertyNames.STRING, propValue);

        final GafferEntityGenerator generator = new GafferEntityGenerator();
        // When
        final Entity entityNoProps = generator._apply(vertexNoProps);
        final Entity entityWithProps = generator._apply(vertexWithProps);

        // Then
        assertThat(entityNoProps.getGroup()).isEqualTo(TestGroups.ENTITY);
        assertThat(entityNoProps.getVertex()).isEqualTo(vertexNoPropsId);
        assertThat(entityWithProps.getGroup()).isEqualTo(TestGroups.ENTITY);
        assertThat(entityWithProps.getVertex()).isEqualTo(vertexWithPropsId);
        assertThat(entityWithProps.getProperties()).containsOnly(
            new SimpleEntry<String, String>(TestPropertyNames.STRING, propValue));
    }

    @Test
    void shouldConvertGafferPopVertexWithNestedProperties() {
        // Given
        final String vertexNestedPropsId = "vertexNestedProps";
        final String outerPropKey = "outerPropKey";
        final String outerPropValue = "outerPropValue";
        final String nestedPropKey = "nestedPropKey";
        final String nestedPropValue = "nestedPropValue";

        // Mock relevant bits so we can make a vertex with properties
        final GafferPopGraph graph = mock(GafferPopGraph.class);
        final Features features = mock(Features.class);
        final VertexFeatures vertexFeatures = mock(VertexFeatures.class);
        final VertexPropertyFeatures vertexPropertyFeatures = mock(VertexPropertyFeatures.class);
        given(graph.features()).willReturn(features);
        given(features.vertex()).willReturn(vertexFeatures);
        given(vertexFeatures.supportsNullPropertyValues()).willReturn(true);
        given(vertexFeatures.properties()).willReturn(vertexPropertyFeatures);
        given(vertexPropertyFeatures.supportsNullPropertyValues()).willReturn(true);

        final GafferPopVertex vertexNestedProps = new GafferPopVertex(TestGroups.ENTITY, vertexNestedPropsId, graph);
        // Add an outer property key value pair then also nest a property with in it
        vertexNestedProps.property(Cardinality.list, outerPropKey, outerPropValue);
        vertexNestedProps.property(outerPropKey).property(nestedPropKey, nestedPropValue);

        final GafferEntityGenerator generator = new GafferEntityGenerator();
        // When
        final Entity entityNestedProps = generator._apply(vertexNestedProps);

        // Then
        assertThat(entityNestedProps.getGroup()).isEqualTo(TestGroups.ENTITY);
        assertThat(entityNestedProps.getVertex()).isEqualTo(vertexNestedPropsId);
        assertThat(entityNestedProps.getProperties()).containsOnly(
            new SimpleEntry<String, String>(outerPropKey, outerPropValue),
            new SimpleEntry<String, String>(nestedPropKey, nestedPropValue));
    }

    @Test
    void shouldNotConvertNullObject() {
        final GafferEntityGenerator generator = new GafferEntityGenerator();

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> generator._apply(null));
    }
}
