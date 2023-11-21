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

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Graph.Features;
import org.apache.tinkerpop.gremlin.structure.Graph.Features.VertexFeatures;
import org.apache.tinkerpop.gremlin.structure.Graph.Features.VertexPropertyFeatures;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;

import java.util.Iterator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

class GafferPopVertexTest {
    @Test
    void shouldConstructVertex() {
        // Given
        final GafferPopGraph graph = mock(GafferPopGraph.class);

        // When
        final GafferPopVertex vertex = new GafferPopVertex(TestGroups.ENTITY, GafferPopGraph.ID_LABEL, graph);

        // Then
        assertThat(vertex.id()).isEqualTo(GafferPopGraph.ID_LABEL);
        assertThat(vertex.graph()).isEqualTo(graph);
        assertThat(vertex.keys()).isEmpty();
    }

    @Test
    void shouldOnlyAddAndGetValidVertexProperties() {
        // Given
        final GafferPopGraph graph = mock(GafferPopGraph.class);
        final Features features = mock(Features.class);
        final VertexFeatures vertexFeatures = mock(VertexFeatures.class);
        final VertexPropertyFeatures vertexPropertyFeatures = mock(VertexPropertyFeatures.class);

        final GafferPopVertex vertex = new GafferPopVertex(TestGroups.ENTITY, GafferPopGraph.ID_LABEL, graph);
        final String stringProp1 = "stringProp1";
        final String stringProp2 = "stringProp2";
        final int intProp = 10;

        given(graph.features()).willReturn(features);
        given(features.vertex()).willReturn(vertexFeatures);
        given(vertexFeatures.supportsNullPropertyValues()).willReturn(true);
        given(vertexFeatures.properties()).willReturn(vertexPropertyFeatures);
        given(vertexPropertyFeatures.supportsNullPropertyValues()).willReturn(true);

        // Ensure adding and getting all properties works as expected
        // Check currently blank
        assertThat(vertex.properties())
            .toIterable()
            .isEmpty();

        // Add string property
        assertThat(vertex.property(Cardinality.list, TestPropertyNames.STRING, stringProp1))
            .isInstanceOf(VertexProperty.class);

        // Verify contains
        assertThat(vertex.properties())
            .toIterable()
            .map(VertexProperty::value)
            .containsExactlyInAnyOrder(stringProp1);

        // Add int property
        assertThat(vertex.property(Cardinality.list, TestPropertyNames.INT, intProp))
            .isInstanceOf(VertexProperty.class);

        // Verify contains
        assertThat(vertex.properties())
            .toIterable()
            .map(VertexProperty::value)
            .containsExactlyInAnyOrder(stringProp1, intProp);

        // Add null property
        assertThat(vertex.property(Cardinality.list, TestPropertyNames.NULL, null))
            .isInstanceOf(VertexProperty.class);

        // Verify filtering works
        assertThat(vertex.properties(TestPropertyNames.STRING))
            .toIterable()
            .map(VertexProperty::value)
            .containsExactlyInAnyOrder(stringProp1);

        // Add another value to a key
        assertThat(vertex.property(Cardinality.list, TestPropertyNames.STRING, stringProp2))
            .isInstanceOf(VertexProperty.class);

        // Verify filtering works
        assertThat(vertex.properties(TestPropertyNames.STRING))
            .toIterable()
            .map(VertexProperty::value)
            .containsExactlyInAnyOrder(stringProp1, stringProp2);

        // Make sure can't access when multiple properties exist for a key
        assertThatExceptionOfType(IllegalStateException.class)
            .isThrownBy(() -> vertex.property(TestPropertyNames.STRING));

        // Make sure can get key values that have only one property
        assertThat(vertex.property(TestPropertyNames.INT).value()).isEqualTo(intProp);
        assertThat(vertex.property(TestPropertyNames.NULL).value()).isNull();

        // Check can get the keys
        assertThat(vertex.keys())
            .containsExactlyInAnyOrder(TestPropertyNames.STRING, TestPropertyNames.INT, TestPropertyNames.NULL);
    }

    @Test
    void shouldNotAddOrGetIllegalProperties() {
        // Given
        final GafferPopGraph graph = mock(GafferPopGraph.class);
        final Features features = mock(Features.class);
        final VertexFeatures vertexFeatures = mock(VertexFeatures.class);
        final VertexPropertyFeatures vertexPropertyFeatures = mock(VertexPropertyFeatures.class);
        given(graph.features()).willReturn(features);
        given(features.vertex()).willReturn(vertexFeatures);
        given(vertexFeatures.properties()).willReturn(vertexPropertyFeatures);
        // Make new vertex with the mocked bits
        final GafferPopVertex vertex = new GafferPopVertex(TestGroups.ENTITY, GafferPopGraph.ID_LABEL, graph);

        // Ensure can't add illegal properties
        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> vertex.property(Cardinality.list, "illegalKeyValueArray", "bad", "bad"));
        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> vertex.property(Cardinality.list, null, null));
        // Ensure can't get a bad property
        assertThat(vertex.property("THIS_DOES_NOT_EXIST")).isEqualTo(VertexProperty.empty());
    }

    @Test
    void shouldOnlyCreateValidGafferPopVertexPropertyObjects() {
        // Given
        final GafferPopGraph graph = mock(GafferPopGraph.class);
        final Features features = mock(Features.class);
        final VertexFeatures vertexFeatures = mock(VertexFeatures.class);
        final VertexPropertyFeatures vertexPropertyFeatures = mock(VertexPropertyFeatures.class);
        given(graph.features()).willReturn(features);
        given(features.vertex()).willReturn(vertexFeatures);
        given(vertexFeatures.properties()).willReturn(vertexPropertyFeatures);
        final String propValue = "propValue";
        // Make new vertex with the mocked bits
        final GafferPopVertex vertex = new GafferPopVertex(TestGroups.ENTITY, GafferPopGraph.ID_LABEL, graph);
        // Make some values to compare against
        final GafferPopVertexProperty<Object> equalProp = new GafferPopVertexProperty<Object>(vertex, TestPropertyNames.STRING, propValue);
        final String notAProp = "notAProp";
        final String nestedKey = "nestedKey";
        final String nestedVal = "nestedVal";

        // Set and get the property
        vertex.property(Cardinality.list, TestPropertyNames.STRING, propValue);
        GafferPopVertexProperty<Object> prop = (GafferPopVertexProperty<Object>) vertex.property(TestPropertyNames.STRING);

        // Then
        assertThat(prop.element()).isEqualTo(vertex);
        assertThat(prop.isPresent()).isTrue();
        assertThat(prop)
                .hasToString("vp[" + TestPropertyNames.STRING + "->" + propValue + "]")
                .isEqualTo(equalProp)
                .hasSameHashCodeAs(equalProp)
                .isNotEqualTo(notAProp)
                .doesNotHaveSameHashCodeAs(notAProp);
        assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> prop.remove());
        // Check nested properties work
        assertThat(prop.property(nestedKey, nestedVal)).isEqualTo(new GafferPopProperty<Object>(vertex, nestedKey, nestedVal));
        assertThat(prop.keys()).containsExactlyInAnyOrder(nestedKey);
        assertThat(prop.property(nestedKey)).isEqualTo(new GafferPopProperty<Object>(prop, nestedKey, nestedVal));
        // Check can't make a bad property
        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> new GafferPopVertexProperty<Object>(vertex, "InvalidNumberOfArgs", "val1", "KeyNoVal"));
        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> new GafferPopVertexProperty<Object>(vertex, "BadKeyType", "val1", 1, "BadKeysValue"));
    }

    @Test
    void shouldNotAllowChangesWhenVertexReadOnly() {
        // Given
        final GafferPopGraph graph = mock(GafferPopGraph.class);
        final Features features = mock(Features.class);
        final VertexFeatures vertexFeatures = mock(VertexFeatures.class);
        final VertexPropertyFeatures vertexPropertyFeatures = mock(VertexPropertyFeatures.class);
        given(graph.features()).willReturn(features);
        given(features.vertex()).willReturn(vertexFeatures);
        given(vertexFeatures.properties()).willReturn(vertexPropertyFeatures);
        // Make new vertex with the mocked bits
        final GafferPopVertex vertex = new GafferPopVertex(TestGroups.ENTITY, GafferPopGraph.ID_LABEL, graph);

        // Set and get a property
        vertex.property(Cardinality.list, TestPropertyNames.STRING, "propValue");
        GafferPopVertexProperty<Object> prop = (GafferPopVertexProperty<Object>) vertex.property(TestPropertyNames.STRING);

        // Set the vertex to read only
        vertex.setReadOnly();

        // Attempt to add a properties
        assertThatExceptionOfType(UnsupportedOperationException.class)
            .isThrownBy(() -> vertex.property(Cardinality.list, TestPropertyNames.STRING, "propValue"));
        assertThatExceptionOfType(UnsupportedOperationException.class)
            .isThrownBy(() -> prop.property(TestPropertyNames.STRING, "nestedPropValue"));
    }

    @Test
    void shouldNotAllowAddingInvalidEdges() {
        // Given
        final GafferPopGraph graph = mock(GafferPopGraph.class);
        final GafferPopVertex vertex = new GafferPopVertex(TestGroups.ENTITY, GafferPopGraph.ID_LABEL, graph);
        // Try add a null edge
        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> vertex.addEdge("null", null, "null"));
    }

    @Test
    void shouldDelegateEdgesToGraph() {
        // Given
        final GafferPopGraph graph = mock(GafferPopGraph.class);
        final GafferPopVertex vertex = new GafferPopVertex(TestGroups.ENTITY, GafferPopGraph.ID_LABEL, graph);
        final Iterator<GafferPopEdge> edges = mock(Iterator.class);
        given(graph.edges(GafferPopGraph.ID_LABEL, Direction.IN, TestGroups.ENTITY)).willReturn(edges);

        // Then
        assertThat(vertex.edges(Direction.IN, TestGroups.ENTITY)).isEqualTo(edges);
    }

    @Test
    void shouldDelegateEdgesWithViewToGraph() {
        // Given
        final GafferPopGraph graph = mock(GafferPopGraph.class);
        final GafferPopVertex vertex = new GafferPopVertex(TestGroups.ENTITY, GafferPopGraph.ID_LABEL, graph);
        final Iterator<GafferPopEdge> edges = mock(Iterator.class);
        final View view = mock(View.class);
        given(graph.edgesWithView(GafferPopGraph.ID_LABEL, Direction.IN, view)).willReturn(edges);

        // Then
        assertThat(vertex.edges(Direction.IN, view)).isEqualTo(edges);
    }

    @Test
    void shouldDelegateVerticesToGraph() {
        // Given
        final GafferPopGraph graph = mock(GafferPopGraph.class);
        final GafferPopVertex vertex = new GafferPopVertex(TestGroups.ENTITY, GafferPopGraph.ID_LABEL, graph);
        final Iterator<GafferPopVertex> vertices = mock(Iterator.class);
        given(graph.adjVertices(GafferPopGraph.ID_LABEL, Direction.IN, TestGroups.EDGE)).willReturn(vertices);

        // Then
        assertThat(vertex.vertices(Direction.IN, TestGroups.EDGE)).isEqualTo(vertices);
    }


    @Test
    void shouldDelegateVerticesWithViewToGraph() {
        // Given
        final GafferPopGraph graph = mock(GafferPopGraph.class);
        final GafferPopVertex vertex = new GafferPopVertex(TestGroups.ENTITY, GafferPopGraph.ID_LABEL, graph);
        final Iterator<GafferPopVertex> vertices = mock(Iterator.class);
        final View view = mock(View.class);
        given(graph.adjVerticesWithView(GafferPopGraph.ID_LABEL, Direction.IN, view)).willReturn(vertices);

        // Then
        assertThat(vertex.vertices(Direction.IN, view)).isEqualTo(vertices);
    }

    @Test
    void shouldCreateReadableToString() {
        // Given
        final GafferPopGraph graph = mock(GafferPopGraph.class);
        final GafferPopVertex vertex = new GafferPopVertex(TestGroups.ENTITY, GafferPopGraph.ID_LABEL, graph);

        // Then
        assertThat(vertex).hasToString("v[BasicEntity-id]");
    }
}
