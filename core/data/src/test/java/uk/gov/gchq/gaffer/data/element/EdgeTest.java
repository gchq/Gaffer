/*
 * Copyright 2016 Crown Copyright
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

package uk.gov.gchq.gaffer.data.element;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
public class EdgeTest extends ElementTest {

    @Override
    @Test
    public void shouldSetAndGetFields() {
        // Given
        final Edge.Builder builder = new Edge.Builder().group("group");

        // When
        builder.source("source vertex");
        builder.destination("destination vertex");
        builder.directed(true);

        final Edge edge = builder.build();

        // Then
        assertEquals("group", edge.getGroup());
        assertEquals("source vertex", edge.getSource());
        assertEquals("destination vertex", edge.getDestination());
        assertTrue(edge.isDirected());
    }

    @Test
    public void shouldBuildEdge() {
        // Given
        final String source = "source vertex";
        final String destination = "destination vertex";
        final boolean directed = true;
        final String propValue = "propValue";

        // When
        final Edge edge = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source(source)
                .destination(destination)
                .directed(directed)
                .property(TestPropertyNames.STRING, propValue)
                .build();

        // Then
        assertEquals(TestGroups.EDGE, edge.getGroup());
        assertEquals(source, edge.getSource());
        assertEquals(destination, edge.getDestination());
        assertTrue(edge.isDirected());
        assertEquals(propValue, edge.getProperty(TestPropertyNames.STRING));
    }

    @Test
    public void shouldConstructEdge() {
        // Given
        final String source = "source vertex";
        final String destination = "destination vertex";
        final boolean directed = true;
        final String propValue = "propValue";

        // When
        final Edge edge = new Edge(TestGroups.EDGE, source, destination, directed);
        edge.putProperty(TestPropertyNames.STRING, propValue);

        // Then
        assertEquals(TestGroups.EDGE, edge.getGroup());
        assertEquals(source, edge.getSource());
        assertEquals(destination, edge.getDestination());
        assertTrue(edge.isDirected());
        assertEquals(propValue, edge.getProperty(TestPropertyNames.STRING));
    }

    @Test
    public void shouldCloneEdge() {
        // Given
        final String source = "source vertex";
        final String destination = "destination vertex";
        final boolean directed = true;
        final String propValue = "propValue";

        // When
        final Edge edge = new Edge(TestGroups.EDGE, source, destination, directed);
        final Edge clone = edge.emptyClone();

        // Then
        assertEquals(edge, clone);
    }

    @Override
    @Test
    public void shouldReturnTrueForEqualsWithTheSameInstance() {
        // Given
        final Edge edge = new Edge.Builder().group("group")
                                            .source("source vertex")
                                            .destination("destination vertex")
                                            .directed(true)
                                            .build();

        // When
        boolean isEqual = edge.equals(edge);

        // Then
        assertTrue(isEqual);
        assertEquals(edge.hashCode(), edge.hashCode());
    }

    @Test
    public void shouldReturnTrueForShallowEqualsWhenAllCoreFieldsAreEqual() {
        // Given
        final Edge edge1 = new Edge.Builder().group("group")
                                             .source("source vertex")
                                             .destination("destination vertex")
                                             .directed(true)
                                             .property("some property", "some value")
                                             .build();

        final Edge edge2 = cloneCoreFields(edge1).property("some different property", "some other value")
                                                 .build();

        // When
        boolean isEqual = edge1.shallowEquals((Object) edge2);

        // Then
        assertTrue(isEqual);
    }

    @Override
    @Test
    public void shouldReturnTrueForEqualsWhenAllCoreFieldsAreEqual() {
        // Given
        final Edge edge1 = new Edge.Builder().group("group")
                                             .source("source vertex")
                                             .destination("destination vertex")
                                             .directed(true)
                                             .property("some property", "some value")
                                             .build();

        final Edge edge2 = cloneCoreFields(edge1).property("some property", "some value")
                                                 .build();

        // When
        boolean isEqual = edge1.equals((Object) edge2);

        // Then
        assertTrue(isEqual);
        assertEquals(edge1.hashCode(), edge2.hashCode());
    }

    @Test
    public void shouldReturnFalseForEqualsWhenPropertyIsDifferent() {
        // Given
        final Edge edge1 = new Edge.Builder().group("group")
                                             .source("source vertex")
                                             .destination("destination vertex")
                                             .directed(true)
                                             .property("some property", "some value")
                                             .build();

        final Edge edge2 = cloneCoreFields(edge1).property("some property", "some other value")
                                                 .build();

        // When
        boolean isEqual = edge1.equals((Object) edge2);

        // Then
        assertFalse(isEqual);
        assertNotEquals(edge1.hashCode(), edge2.hashCode());
    }

    @Override
    @Test
    public void shouldReturnFalseForEqualsWhenGroupIsDifferent() {
        // Given
        final Edge edge1 = new Edge.Builder().group("group")
                                             .source("source vertex")
                                             .destination("destination vertex")
                                             .directed(true)
                                             .build();

        final Edge edge2 = new Edge.Builder().group("a different group")
                                             .source(edge1.getSource())
                                             .destination(edge1.getDestination())
                                             .directed(edge1.isDirected())
                                             .build();

        // When
        boolean isEqual = edge1.equals((Object) edge2);

        // Then
        assertFalse(isEqual);
        assertFalse(edge1.hashCode() == edge2.hashCode());
    }

    @Test
    public void shouldReturnFalseForEqualsWhenDirectedIsDifferent() {
        // Given
        final Edge edge1 = new Edge.Builder().group("group")
                                             .source("source vertex")
                                             .destination("destination vertex")
                                             .directed(true).build();

        final Edge edge2 = cloneCoreFields(edge1).directed(!edge1.isDirected())
                                                 .build();

        // When
        boolean isEqual = edge1.equals((Object) edge2);

        // Then
        assertFalse(isEqual);
        assertFalse(edge1.hashCode() == edge2.hashCode());
    }

    @Test
    public void shouldReturnFalseForEqualsWhenSourceIsDifferent() {
        // Given
        final Edge edge1 = new Edge.Builder().group("group")
                                             .source("source vertex")
                                             .destination("destination vertex")
                                             .directed(true).build();

        final Edge edge2 = cloneCoreFields(edge1).source("different source")
                                                 .build();

        // When
        boolean isEqual = edge1.equals((Object) edge2);

        // Then
        assertFalse(isEqual);
        assertFalse(edge1.hashCode() == edge2.hashCode());
    }

    @Test
    public void shouldReturnFalseForEqualsWhenDestinationIsDifferent() {
        // Given
        final Edge edge1 = new Edge.Builder().group("group")
                                             .source("source vertex")
                                             .destination("destination vertex")
                                             .directed(true).build();

        final Edge edge2 = cloneCoreFields(edge1).destination("different destination vertex")
                                                 .build();

        // When
        boolean isEqual = edge1.equals((Object) edge2);

        // Then
        assertFalse(isEqual);
        assertFalse(edge1.hashCode() == edge2.hashCode());
    }

    @Test
    public void shouldReturnTrueForEqualsWhenUndirectedIdentifiersFlipped() {
        // Given
        final Edge edge1 = new Edge.Builder().group("group")
                                             .source("source vertex")
                                             .destination("destination vertex")
                                             .directed(false).build();

        // Given
        final Edge edge2 = new Edge.Builder().group("group")
                                             .source("destination vertex")
                                             .destination("source vertex")
                                             .directed(false).build();

        // When
        boolean isEqual = edge1.equals((Object) edge2);

        // Then
        assertTrue(isEqual);
        assertTrue(edge1.hashCode() == edge2.hashCode());
    }

    @Test
    public void shouldReturnFalseForEqualsWhenDirectedIdentifiersFlipped() {
        // Given
        final Edge edge1 = new Edge.Builder().group("group")
                                             .source("source vertex")
                                             .destination("destination vertex")
                                             .directed(true)
                                             .build();

        // Given
        final Edge edge2 = new Edge.Builder().group("group")
                                             .source("destination vertex")
                                             .destination("source vertex")
                                             .directed(true).build();

        // When
        boolean isEqual = edge1.equals((Object) edge2);

        // Then
        assertFalse(isEqual);
        assertFalse(edge1.hashCode() == edge2.hashCode());
    }

    @Override
    @Test
    public void shouldSerialiseAndDeserialiseIdentifiers() throws SerialisationException {
        // Given
        final Edge edge = new Edge.Builder().group("group")
                                            .source(1L)
                                            .destination(new Date(2L))
                                            .directed(true)
                                            .build();

        final JSONSerialiser serialiser = new JSONSerialiser();

        // When
        final byte[] serialisedElement = serialiser.serialise(edge);
        final Edge deserialisedElement = serialiser.deserialise(serialisedElement, edge
                .getClass());

        // Then
        assertEquals(edge, deserialisedElement);
    }

    @Test
    public void shouldSwapVerticesIfSourceIsGreaterThanDestination_toString() {
        // Given
        final Edge edge = new Edge.Builder().group(TestGroups.EDGE)
                                            .directed(false)
                                            .source(new Vertex("2"))
                                            .destination(new Vertex("1"))
                                            .build();

        // Then
        assertThat(edge.getSource(), equalTo(new Vertex("1")));
        assertThat(edge.getDestination(), equalTo(new Vertex("2")));
    }

    @Test
    public void shouldNotSwapVerticesIfSourceIsLessThanDestination_toString() {
        // Given
        final Edge edge = new Edge.Builder().group(TestGroups.EDGE)
                                            .directed(false)
                                            .source(new Vertex("1"))
                                            .destination(new Vertex("2"))
                                            .build();

        // Then
        assertThat(edge.getSource(), equalTo(new Vertex("1")));
        assertThat(edge.getDestination(), equalTo(new Vertex("2")));
    }

    @Test
    public void shouldSwapVerticesIfSourceIsGreaterThanDestination_comparable() {
        // Given
        final Edge edge = new Edge.Builder().group(TestGroups.EDGE)
                                            .directed(false)
                                            .source(new Integer(2))
                                            .destination(new Integer(1))
                                            .build();

        // Then
        assertThat(edge.getSource(), equalTo(new Integer(1)));
        assertThat(edge.getDestination(), equalTo(new Integer(2)));
    }

    @Test
    public void shouldNotSwapVerticesIfSourceIsLessThanDestination_comparable() {
        // Given
        final Edge edge = new Edge.Builder().group(TestGroups.EDGE)
                                            .directed(false)
                                            .source(new Integer(1))
                                            .destination(new Integer(2))
                                            .build();

        // Then
        assertThat(edge.getSource(), equalTo(new Integer(1)));
        assertThat(edge.getDestination(), equalTo(new Integer(2)));
    }

    @Test
    public void shouldFailToConsistentlySwapVerticesWithNoToStringImplementation() {
        // Given
        final List<Edge> edges = new ArrayList<>();
        final List<Vertex2> sources = new ArrayList<>();
        final List<Vertex2> destinations = new ArrayList<>();

        // Create a load of edges with Vertex2 objects as source and destination.
        // Vertex2 has no toString method and does not implement Comparable, so
        // this should result in Edges being created with different sources and
        // destinations.
        for (int i = 0;  i < 1000; i ++) {
            final Vertex2 source = new Vertex2("1");
            final Vertex2 destination = new Vertex2("2");

            sources.add(source);
            destinations.add(destination);
        }

        for (int i = 0;  i < 1000; i ++) {
            final Edge edge = new Edge.Builder().group(TestGroups.EDGE)
                                                .directed(false)
                                                .source(sources.get(i))
                                                .destination(destinations.get(i))
                                                .build();

            edges.add(edge);
        }

        // Then
        assertThat(edges.stream().map(Edge::getSource).distinct().count(), greaterThan(1L));
        assertThat(edges.stream().map(Edge::getDestination).distinct().count(), greaterThan(1L));
    }

    @Test
    public void shouldNotFailToConsistentlySwapVerticesWithStringImplementation() {
        // Opposite to shouldFailToConsistentlySwapVerticesWithNoToStringImplementation(),
        // showing that Edges which implement toString, equals and hashCode are
        // consistently created with source and destination the correct way round

        // Given
        final List<Edge> edges = new ArrayList<>();
        final List<Vertex> sources = new ArrayList<>();
        final List<Vertex> destinations = new ArrayList<>();

        for (int i = 0;  i < 1000; i ++) {
            final Vertex source = new Vertex("1");
            final Vertex destination = new Vertex("2");

            sources.add(source);
            destinations.add(destination);
        }

        for (int i = 0;  i < 1000; i ++) {
            final Edge edge = new Edge.Builder().group(TestGroups.EDGE)
                                                .directed(false)
                                                .source(sources.get(i))
                                                .destination(destinations.get(i))
                                                .build();

            edges.add(edge);
        }

        // Then
        assertThat(edges.stream().map(Edge::getSource).distinct().count(), equalTo(1L));
        assertThat(edges.stream().map(Edge::getDestination).distinct().count(), equalTo(1L));
    }

    @Test
    public void shouldReinitialise() {
        // Given
        final Edge edge1 = new Edge(TestGroups.EDGE, 1, 2, false);
        final Edge edge2 = new Edge(TestGroups.EDGE_2, 3, 4, false);

        // When
        edge1.reinitialise(TestGroups.EDGE_2, 3, 4, false);

        // Then
        assertThat(edge1, equalTo(edge2));
    }

    @Test
    public void shouldFallbackToToStringComparisonIfSourceAndDestinationHaveDifferentTypes() {
        // Given
        final Edge edge1 = new Edge(TestGroups.EDGE, 1, "2", false);
        final Edge edge2 = new Edge(TestGroups.EDGE, "2", 1, false);

        // Then
        assertThat(edge1, equalTo(edge2));
    }

    @Test
    public void shouldSkipVertexOrderingIfEdgeIsReversed() {
        // Given
        final Edge edge = new Edge.Builder().group(TestGroups.EDGE)
                                                    .source("2")
                                                    .destination("1")
                                                    .directed(false)
                                                    .build();

        final Edge reversedEdge = new Edge.Builder().group(TestGroups.EDGE)
                                                    .source("2")
                                                    .destination("1")
                                                    .directed(false)
                                                    .reversed(true)
                                                    .build();
        // Then
        assertThat(edge.getSource(), equalTo("1"));
        assertThat(edge.getDestination(), equalTo("2"));
        assertThat(reversedEdge.getSource(), equalTo("2"));
        assertThat(reversedEdge.getDestination(), equalTo("1"));
    }

    @Override
    protected Edge newElement(final String group) {
        return new Edge.Builder().group(group).build();
    }

    @Override
    protected Edge newElement() {
        return new Edge.Builder().build();
    }

    private Edge.Builder cloneCoreFields(final Edge edge) {
        final Edge.Builder newEdge = new Edge.Builder().group(edge.getGroup())
                                                       .source(edge.getSource())
                                                       .destination(edge.getDestination())
                                                       .directed(edge.isDirected());

        return newEdge;
    }

    private Edge cloneAllFields(final Edge edge) {
        final Edge newEdge = cloneCoreFields(edge).build();

        final Properties properties = edge.getProperties();
        for (final Entry<String, Object> entry : properties.entrySet()) {
            newEdge.putProperty(entry.getKey(), entry.getValue());
        }

        return newEdge;
    }

    private class Vertex {
        private final String property;

        public Vertex(final String property) {
            this.property = property;
        }

        public String getProperty() {
            return property;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final Vertex vertex = (Vertex) o;

            return new EqualsBuilder()
                    .append(property, vertex.property)
                    .isEquals();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37)
                    .append(property)
                    .toHashCode();
        }

        @Override
        public String toString() {
            return "Vertex[property=" + property + "]";
        }
    }

    private class Vertex2 {
        private final String property;

        public Vertex2(final String property) {
            this.property = property;
        }

        public String getProperty() {
            return property;
        }
    }
}
