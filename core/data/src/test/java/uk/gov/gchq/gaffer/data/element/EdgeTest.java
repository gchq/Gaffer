/*
 * Copyright 2016-2019 Crown Copyright
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

import uk.gov.gchq.gaffer.commonutil.StringUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.id.EdgeId;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class EdgeTest extends ElementTest {

    @Override
    public void shouldSetAndGetFields() {
        // Given
        final Edge edge = new Edge.Builder()
                .group("group")
                .source("source vertex")
                .dest("destination vertex")
                .directed(true)
                .build();

        // When/Then
        assertEquals("group", edge.getGroup());
        assertEquals("source vertex", edge.getSource());
        assertEquals("destination vertex", edge.getDestination());
        assertTrue(edge.isDirected());
    }

    @Test
    public void shouldSetAndGetIdentifiersWithMatchedSource() {
        // Given
        final Edge edge = new Edge.Builder()
                .group("group")
                .source("source vertex")
                .dest("destination vertex")
                .directed(true)
                .matchedVertex(EdgeId.MatchedVertex.SOURCE)
                .build();

        // When/Then
        assertEquals("source vertex", edge.getMatchedVertexValue());
        assertEquals("source vertex", edge.getIdentifier(IdentifierType.MATCHED_VERTEX));
        assertEquals("destination vertex", edge.getAdjacentMatchedVertexValue());
        assertEquals("destination vertex", edge.getIdentifier(IdentifierType.ADJACENT_MATCHED_VERTEX));
        assertTrue(edge.isDirected());
    }

    @Test
    public void shouldSetAndGetIdentifiersWithMatchedDestination() {
        // Given
        final Edge edge = new Edge.Builder()
                .group("group")
                .source("source vertex")
                .dest("destination vertex")
                .directed(true)
                .matchedVertex(EdgeId.MatchedVertex.DESTINATION)
                .build();

        // When/Then
        assertEquals("destination vertex", edge.getMatchedVertexValue());
        assertEquals("destination vertex", edge.getIdentifier(IdentifierType.MATCHED_VERTEX));
        assertEquals("source vertex", edge.getIdentifier(IdentifierType.ADJACENT_MATCHED_VERTEX));
        assertEquals("source vertex", edge.getAdjacentMatchedVertexValue());
        assertTrue(edge.isDirected());
    }

    @Test
    public void shouldSetAndGetIdentifiersWithMatchedSourceIsNull() {
        // Given
        final Edge edge = new Edge.Builder()
                .group("group")
                .source("source vertex")
                .dest("destination vertex")
                .directed(true)
                .matchedVertex(null)
                .build();

        // When/Then
        assertEquals("source vertex", edge.getMatchedVertexValue());
        assertEquals("source vertex", edge.getIdentifier(IdentifierType.MATCHED_VERTEX));
        assertEquals("destination vertex", edge.getIdentifier(IdentifierType.ADJACENT_MATCHED_VERTEX));
        assertEquals("destination vertex", edge.getIdentifier(IdentifierType.ADJACENT_MATCHED_VERTEX));
        assertTrue(edge.isDirected());
    }

    @Test
    public void shouldBuildEdge() {
        // Given
        final String source = "source vertex";
        final String destination = "dest vertex";
        final boolean directed = true;
        final String propValue = "propValue";

        // When
        final Edge edge = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source(source)
                .dest(destination)
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
        final String destination = "dest vertex";
        final boolean directed = true;
        final String propValue = "propValue";

        // When
        final Edge edge = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source(source)
                .dest(destination)
                .directed(directed)
                .build();

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
        final String destination = "dest vertex";
        final boolean directed = true;

        // When
        final Edge edge = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source(source)
                .dest(destination)
                .directed(directed)
                .build();

        final Edge clone = edge.emptyClone();

        // Then
        assertEquals(edge, clone);
    }

    @Override
    public void shouldReturnTrueForEqualsWithTheSameInstance() {
        // Given
        final Edge edge = new Edge.Builder()
                .group("group")
                .source("source vertex")
                .dest("dest vertex")
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
        final Edge edge1 = new Edge.Builder()
                .group("group")
                .source("source vertex")
                .dest("dest vertex")
                .directed(true)
                .property("some property", "some value")
                .build();

        final Edge edge2 = cloneCoreFields(edge1);
        edge2.putProperty("some different property", "some other value");

        // When
        boolean isEqual = edge1.shallowEquals((Object) edge2);

        // Then
        assertTrue(isEqual);
    }

    @Override
    public void shouldReturnTrueForEqualsWhenAllCoreFieldsAreEqual() {
        final Edge edge1 = new Edge.Builder()
                .group("group")
                .source("source vertex")
                .dest("dest vertex")
                .directed(true)
                .property("some property", "some value")
                .build();

        final Edge edge2 = cloneCoreFields(edge1);
        edge2.putProperty("some property", "some value");

        // When
        boolean isEqual = edge1.equals((Object) edge2);

        // Then
        assertTrue(isEqual);
        assertEquals(edge1.hashCode(), edge2.hashCode());
    }

    @Test
    public void shouldReturnFalseForEqualsWhenPropertyIsDifferent() {
        // Given
        final Edge edge1 = new Edge.Builder()
                .group("group")
                .source("source vertex")
                .dest("dest vertex")
                .directed(true)
                .property("some property", "some value")
                .build();

        final Edge edge2 = cloneCoreFields(edge1);
        edge2.putProperty("some property", "some other value");

        // When
        boolean isEqual = edge1.equals((Object) edge2);

        // Then
        assertFalse(isEqual);
        assertNotEquals(edge1.hashCode(), edge2.hashCode());
    }

    @Override
    public void shouldReturnFalseForEqualsWhenGroupIsDifferent() {
        // Given
        final Edge edge1 = new Edge.Builder()
                .group("group")
                .source("source vertex")
                .dest("dest vertex")
                .directed(true)
                .build();

        final Edge edge2 = new Edge.Builder()
                .group("a different group")
                .source(edge1.getSource())
                .dest(edge1.getDestination())
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
        final Edge edge1 = new Edge.Builder()
                .group("group")
                .source("source vertex")
                .dest("dest vertex")
                .directed(true)
                .build();

        final Edge edge2 = new Edge.Builder()
                .group(edge1.getGroup())
                .source(edge1.getSource())
                .dest(edge1.getDestination())
                .directed(!edge1.isDirected())
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
        final Edge edge1 = new Edge.Builder()
                .group("group")
                .source("source vertex")
                .dest("dest vertex")
                .directed(true)
                .build();

        final Edge edge2 = new Edge.Builder()
                .group(edge1.getGroup())
                .source("different source")
                .dest(edge1.getDestination())
                .directed(edge1.isDirected())
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
                .dest("dest vertex")
                .directed(true)
                .build();

        final Edge edge2 = new Edge.Builder()
                .group(edge1.getGroup())
                .source(edge1.getSource())
                .dest("different destination")
                .directed(edge1.isDirected())
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
        final Edge edge1 = new Edge.Builder()
                .group("group")
                .source("source vertex")
                .dest("dest vertex")
                .directed(false)
                .build();

        // Given
        final Edge edge2 = new Edge.Builder()
                .group("group")
                .source("dest vertex")
                .dest("source vertex")
                .directed(false)
                .build();

        // When
        boolean isEqual = edge1.equals((Object) edge2);

        // Then
        assertTrue(isEqual);
        assertTrue(edge1.hashCode() == edge2.hashCode());
    }

    @Test
    public void shouldReturnFalseForEqualsWhenDirectedIdentifiersFlipped() {
        // Given
        final Edge edge1 = new Edge.Builder()
                .group("group")
                .source("source vertex")
                .dest("dest vertex")
                .directed(true)
                .build();

        // Given
        final Edge edge2 = new Edge.Builder()
                .group("group")
                .source("dest vertex")
                .dest("source vertex")
                .directed(true)
                .build();

        // When
        boolean isEqual = edge1.equals((Object) edge2);

        // Then
        assertFalse(isEqual);
        assertFalse(edge1.hashCode() == edge2.hashCode());
    }

    @Override
    public void shouldSerialiseAndDeserialiseIdentifiers() throws SerialisationException {
        // Given
        final Edge edge = new Edge.Builder()
                .group("group")
                .source(1L)
                .dest(new Date(2L))
                .directed(true)
                .build();

        // When
        final byte[] serialisedElement = JSONSerialiser.serialise(edge);
        final Edge deserialisedElement = JSONSerialiser.deserialise(serialisedElement, edge
                .getClass());

        // Then
        assertEquals(edge, deserialisedElement);
        assertTrue(StringUtil.toString(serialisedElement).contains("{\"java.lang.Long\":1}"));
        assertTrue(StringUtil.toString(serialisedElement).contains("{\"java.util.Date\":2}"));
    }

    @Override
    protected Edge newElement(final String group) {
        return new Edge.Builder().group(group).build();
    }

    @Override
    protected Edge newElement() {
        return new Edge.Builder().build();
    }

    @Test
    public void shouldSwapVerticesIfSourceIsGreaterThanDestination_toString() {
        // Given
        final Edge edge = new Edge.Builder().group(TestGroups.EDGE)
                .directed(false)
                .source(new Vertex("2"))
                .dest(new Vertex("1"))
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
                .dest(new Vertex("2"))
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
                .dest(new Integer(1))
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
                .dest(new Integer(2))
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
        for (int i = 0; i < 1000; i++) {
            final Vertex2 source = new Vertex2("1");
            final Vertex2 destination = new Vertex2("2");

            sources.add(source);
            destinations.add(destination);
        }

        for (int i = 0; i < 1000; i++) {
            final Edge edge = new Edge.Builder().group(TestGroups.EDGE)
                    .directed(false)
                    .source(sources.get(i))
                    .dest(destinations.get(i))
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

        for (int i = 0; i < 1000; i++) {
            final Vertex source = new Vertex("1");
            final Vertex destination = new Vertex("2");

            sources.add(source);
            destinations.add(destination);
        }

        for (int i = 0; i < 1000; i++) {
            final Edge edge = new Edge.Builder().group(TestGroups.EDGE)
                    .directed(false)
                    .source(sources.get(i))
                    .dest(destinations.get(i))
                    .build();

            edges.add(edge);
        }

        // Then
        assertThat(edges.stream().map(Edge::getSource).distinct().count(), equalTo(1L));
        assertThat(edges.stream().map(Edge::getDestination).distinct().count(), equalTo(1L));
    }

    @Test
    public void shouldSetIdentifiers() {
        // Given
        final Edge edge1 = new Edge(TestGroups.EDGE, 1, 2, false);
        final Edge edge2 = new Edge(TestGroups.EDGE_2, 4, 3, false);

        // When
        edge1.setIdentifiers(4, 3, false);
        edge1.setGroup(TestGroups.EDGE_2);

        // Then
        assertEquals(3, edge1.getSource());
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
    public void shouldDeserialiseFromJsonUsingDirectedTrueField() throws SerialisationException {
        // Given
        final String json = "{\"class\": \"uk.gov.gchq.gaffer.data.element.Edge\", \"directed\": true}";

        // When
        final Edge deserialisedEdge = JSONSerialiser.deserialise(json.getBytes(), Edge.class);

        // Then
        assertTrue(deserialisedEdge.isDirected());
    }

    @Test
    public void shouldDeserialiseFromJsonUsingDirectedFalseField() throws SerialisationException {
        // Given
        final String json = "{\"class\": \"uk.gov.gchq.gaffer.data.element.Edge\", \"directed\": false}";

        // When
        final Edge deserialisedEdge = JSONSerialiser.deserialise(json.getBytes(), Edge.class);

        // Then
        assertFalse(deserialisedEdge.isDirected());
    }

    @Test
    public void shouldDeserialiseFromJsonWhenDirectedTypeIsDirected() throws SerialisationException {
        // Given
        final String json = "{\"class\": \"uk.gov.gchq.gaffer.data.element.Edge\", \"directedType\": \"DIRECTED\"}";

        // When
        final Edge deserialisedEdge = JSONSerialiser.deserialise(json.getBytes(), Edge.class);

        // Then
        assertTrue(deserialisedEdge.isDirected());
    }

    @Test
    public void shouldDeserialiseFromJsonWhenDirectedTypeIsUndirected() throws SerialisationException {
        // Given
        final String json = "{\"class\": \"uk.gov.gchq.gaffer.data.element.Edge\", \"directedType\": \"UNDIRECTED\"}";

        // When
        final Edge deserialisedEdge = JSONSerialiser.deserialise(json.getBytes(), Edge.class);

        // Then
        assertFalse(deserialisedEdge.isDirected());
    }

    @Test
    public void shouldDeserialiseFromJsonWhenDirectedTypeIsEither() throws SerialisationException {
        // Given
        final String json = "{\"class\": \"uk.gov.gchq.gaffer.data.element.Edge\", \"directedType\": \"EITHER\"}";

        // When
        final Edge deserialisedEdge = JSONSerialiser.deserialise(json.getBytes(), Edge.class);

        // Then
        assertTrue(deserialisedEdge.isDirected());
    }

    @Test
    public void shouldThrowExceptionWhenDeserialiseFromJsonUsingDirectedAndDirectedType() {
        // Given
        final String json = "{\"class\": \"uk.gov.gchq.gaffer.data.element.Edge\", \"directed\": true, \"directedType\": \"DIRECTED\"}";

        // When / Then
        try {
            JSONSerialiser.deserialise(json.getBytes(), Edge.class);
            fail("Exception expected");
        } catch (final Exception e) {
            assertTrue(e.getMessage().contains("not both"));
        }
    }

    private Edge cloneCoreFields(final Edge edge) {
        return new Edge.Builder()
                .group(edge.getGroup())
                .source(edge.getSource())
                .dest(edge.getDestination())
                .directed(edge.isDirected())
                .build();
    }

    private class Vertex {
        private final String property;

        Vertex(final String property) {
            this.property = property;
        }

        public String getProperty() {
            return property;
        }

        @Override
        public boolean equals(final Object obj) {
            if (this == obj) {
                return true;
            }

            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }

            final Vertex vertex = (Vertex) obj;

            return new EqualsBuilder()
                    .append(property, vertex.property)
                    .isEquals();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(19, 23)
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

        Vertex2(final String property) {
            this.property = property;
        }

        public String getProperty() {
            return property;
        }
    }
}
