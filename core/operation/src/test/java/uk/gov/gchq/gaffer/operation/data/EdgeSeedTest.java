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

package uk.gov.gchq.gaffer.operation.data;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.junit.Test;

import uk.gov.gchq.gaffer.JSONSerialisationTest;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.element.id.EdgeId;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class EdgeSeedTest extends JSONSerialisationTest<EdgeSeed> {
    @Test
    public void shouldBeRelatedToEntityIdWhenSourceEqualsVertex() {
        // Given
        final String source = "source";
        final String destination = "destination";
        final boolean directed = true;
        final EdgeId seed = new EdgeSeed(source, destination, directed);
        final EntityId relatedSeed = mock(EntityId.class);

        given(relatedSeed.getVertex()).willReturn(source);

        // When
        final boolean isRelated = seed.isRelated((ElementId) relatedSeed).isMatch();

        // Then
        assertTrue(isRelated);
    }

    @Test
    public void shouldBeRelatedToEntityIdWhenDestinationEqualsVertex() {
        // Given
        final String source = "source";
        final String destination = "destination";
        final boolean directed = true;
        final EdgeId seed = new EdgeSeed(source, destination, directed);
        final EntityId relatedSeed = mock(EntityId.class);

        given(relatedSeed.getVertex()).willReturn(destination);

        // When
        final boolean isRelated = seed.isRelated((ElementId) relatedSeed).isMatch();

        // Then
        assertTrue(isRelated);
    }

    @Test
    public void shouldBeRelatedToEntityIdWhenSourceAndVertexAreNull() {
        // Given
        final String source = null;
        final String destination = "destination";
        final boolean directed = true;
        final EdgeId seed = new EdgeSeed(source, destination, directed);
        final EntityId relatedSeed = mock(EntityId.class);

        given(relatedSeed.getVertex()).willReturn(source);

        // When
        final boolean isRelated = seed.isRelated((ElementId) relatedSeed).isMatch();

        // Then
        assertTrue(isRelated);
    }

    @Test
    public void shouldBeRelatedToEntityIdWhenDestinationAndVertexAreNull() {
        // Given
        final String source = "source";
        final String destination = null;
        final boolean directed = true;
        final EdgeId seed = new EdgeSeed(source, destination, directed);
        final EntityId relatedSeed = mock(EntityId.class);

        given(relatedSeed.getVertex()).willReturn(source);

        // When
        final boolean isRelated = seed.isRelated((ElementId) relatedSeed).isMatch();

        // Then
        assertTrue(isRelated);
    }

    @Test
    public void shouldNotBeRelatedToEntityIdWhenIdentifierNotEqualToSourceOrDestination() {
        // Given
        final String source = "source";
        final String destination = "destination";
        final boolean directed = true;
        final EdgeId seed = new EdgeSeed(source, destination, directed);
        final EntityId unrelatedSeed = mock(EntityId.class);

        given(unrelatedSeed.getVertex()).willReturn("other identifier");

        // When
        final boolean isRelated = seed.isRelated((ElementId) unrelatedSeed).isMatch();

        // Then
        assertFalse(isRelated);
    }

    @Test
    public void shouldBeRelatedToEdgeId() {
        // Given
        final String source = "source";
        final String destination = "destination";
        final boolean directed = true;
        final EdgeId seed1 = new EdgeSeed(source, destination, directed);
        final EdgeId seed2 = new EdgeSeed(source, destination, directed);

        // When
        final boolean isRelated = seed1.isRelated(seed2).isMatch();

        // Then
        assertTrue(isRelated);
    }

    @Test
    public void shouldBeEqualWhenSourceDestinationAndDirectedEqual() {
        // Given
        final String source = "source";
        final String destination = "destination";
        final boolean directed = true;
        final EdgeId seed1 = new EdgeSeed(source, destination, directed);
        final EdgeId seed2 = new EdgeSeed(source, destination, directed);

        // When
        final boolean isEqual = seed1.equals(seed2);

        // Then
        assertTrue(isEqual);
        assertEquals(seed1.hashCode(), seed2.hashCode());
    }

    @Test
    public void shouldBeNotEqualWhenSourceNotEqual() {
        // Given
        final String source = "source";
        final String destination = "destination";
        final boolean directed = true;
        final EdgeId seed1 = new EdgeSeed(source, destination, directed);
        final EdgeId seed2 = new EdgeSeed("different source", destination, directed);

        // When
        final boolean isEqual = seed1.equals(seed2);

        // Then
        assertFalse(isEqual);
        assertNotEquals(seed1.hashCode(), seed2.hashCode());
    }

    @Test
    public void shouldBeNotEqualWhenDestinationNotEqual() {
        // Given
        final String source = "source";
        final String destination = "destination";
        final boolean directed = true;
        final EdgeId seed1 = new EdgeSeed(source, destination, directed);
        final EdgeId seed2 = new EdgeSeed(source, "different destination", directed);

        // When
        final boolean isEqual = seed1.equals(seed2);

        // Then
        assertFalse(isEqual);
    }

    @Test
    public void shouldBeNotEqualWhenDirectedNotEqual() {
        // Given
        final String source = "source";
        final String destination = "destination";
        final boolean directed = true;
        final EdgeId seed1 = new EdgeSeed(source, destination, directed);
        final EdgeId seed2 = new EdgeSeed(source, destination, false);

        // When
        final boolean isEqual = seed1.equals(seed2);

        // Then
        assertFalse(isEqual);
    }

    @Test
    public void shouldBeEqualWhenUndirectedAndSourceAndDestinationFlipped() {
        // Given
        final String source = "source";
        final String destination = "destination";
        final EdgeId seed1 = new EdgeSeed(source, destination, false);
        final EdgeId seed2 = new EdgeSeed(destination, source, false);

        // When
        final boolean isEqual = seed1.equals(seed2);

        // Then
        assertTrue(isEqual);
        assertEquals(seed1.hashCode(), seed2.hashCode());
    }

    @Test
    public void shouldSerialiseAndDeserialiseIntegersAndLongs() throws SerialisationException {
        // Given
        final Long source = 1L;
        final Integer destination = 2;
        final boolean directed = true;
        final EdgeId seed = new EdgeSeed(source, destination, directed);

        // When
        final byte[] bytes = JSONSerialiser.serialise(seed);
        final EdgeId seedDeserialised = JSONSerialiser.deserialise(bytes, EdgeId.class);

        // Then
        assertEquals(seed, seedDeserialised);
        assertTrue(seedDeserialised.getSource() instanceof Long);
        assertTrue(seedDeserialised.getDestination() instanceof Integer);
    }

    @Test
    public void shouldSerialiseAndDeserialiseCustomVertexObjects() throws SerialisationException {
        // Given
        final CustomVertex source = new CustomVertex();
        source.setType("sourceType");
        source.setValue("sourceValue");
        final CustomVertex destination = new CustomVertex();
        destination.setType("destinationType");
        destination.setValue("destinationValue");
        final boolean directed = true;
        final EdgeId seed = new EdgeSeed(source, destination, directed);

        // When
        final byte[] bytes = JSONSerialiser.serialise(seed);
        final EdgeId seedDeserialised = JSONSerialiser.deserialise(bytes, EdgeId.class);

        // Then
        assertTrue(seedDeserialised.getSource() instanceof CustomVertex);
        assertTrue(seedDeserialised.getDestination() instanceof CustomVertex);
        assertEquals("sourceType", ((CustomVertex) seedDeserialised.getSource()).getType());
        assertEquals("sourceValue", ((CustomVertex) seedDeserialised.getSource()).getValue());
        assertEquals("destinationType", ((CustomVertex) seedDeserialised.getDestination()).getType());
        assertEquals("destinationValue", ((CustomVertex) seedDeserialised.getDestination()).getValue());
    }

    @Test
    public void shouldSwapVerticesIfSourceIsGreaterThanDestination_toString() {
        // Given
        final EdgeSeed edgeSeed = new EdgeSeed(new Vertex("2"), new Vertex("1"), false);

        // Then
        assertThat(edgeSeed.getSource(), equalTo(new Vertex("1")));
        assertThat(edgeSeed.getDestination(), equalTo(new Vertex("2")));
    }

    @Test
    public void shouldNotSwapVerticesIfSourceIsLessThanDestination_toString() {
        // Given
        final EdgeSeed edgeSeed = new EdgeSeed(new Vertex("1"), new Vertex("2"), false);

        // Then
        assertThat(edgeSeed.getSource(), equalTo(new Vertex("1")));
        assertThat(edgeSeed.getDestination(), equalTo(new Vertex("2")));
    }

    @Test
    public void shouldSwapVerticesIfSourceIsGreaterThanDestination_comparable() {
        // Given
        final EdgeSeed edgeSeed = new EdgeSeed(2, 1, false);

        // Then
        assertThat(edgeSeed.getSource(), equalTo(1));
        assertThat(edgeSeed.getDestination(), equalTo(2));
    }

    @Test
    public void shouldNotSwapVerticesIfSourceIsLessThanDestination_comparable() {
        // Given
        final EdgeSeed edgeSeed = new EdgeSeed(1, 2, false);

        // Then
        assertThat(edgeSeed.getSource(), equalTo(1));
        assertThat(edgeSeed.getDestination(), equalTo(2));
    }

    @Test
    public void shouldFailToConsistentlySwapVerticesWithNoToStringImplementation() {
        // Given
        final List<EdgeSeed> edgeSeeds = new ArrayList<>();
        final List<Vertex2> sources = new ArrayList<>();
        final List<Vertex2> destinations = new ArrayList<>();

        // Create a load of edgeSeeds with Vertex2 objects as source and destination.
        // Vertex2 has no toString method and does not implement Comparable, so
        // this should result in EdgeSeeds being created with different sources and
        // destinations.
        for (int i = 0; i < 1000; i++) {
            final Vertex2 source = new Vertex2("1");
            final Vertex2 destination = new Vertex2("2");

            sources.add(source);
            destinations.add(destination);
        }

        for (int i = 0; i < 1000; i++) {
            final EdgeSeed edgeSeed = new EdgeSeed(sources.get(i), destinations.get(i), false);
            edgeSeeds.add(edgeSeed);
        }

        // Then
        assertThat(edgeSeeds.stream().map(EdgeSeed::getSource).distinct().count(), greaterThan(1L));
        assertThat(edgeSeeds.stream().map(EdgeSeed::getDestination).distinct().count(), greaterThan(1L));
    }

    @Test
    public void shouldNotFailToConsistentlySwapVerticesWithStringImplementation() {
        // Opposite to shouldFailToConsistentlySwapVerticesWithNoToStringImplementation(),
        // showing that EdgeSeeds which implement toString, equals and hashCode are
        // consistently created with source and destination the correct way round

        // Given
        final List<EdgeSeed> edgeSeeds = new ArrayList<>();
        final List<Vertex> sources = new ArrayList<>();
        final List<Vertex> destinations = new ArrayList<>();

        for (int i = 0; i < 1000; i++) {
            final Vertex source = new Vertex("1");
            final Vertex destination = new Vertex("2");

            sources.add(source);
            destinations.add(destination);
        }

        for (int i = 0; i < 1000; i++) {
            final EdgeSeed edgeSeed = new EdgeSeed(sources.get(i), destinations.get(i), false);
            edgeSeeds.add(edgeSeed);
        }

        // Then
        assertThat(edgeSeeds.stream().map(EdgeSeed::getSource).distinct().count(), equalTo(1L));
        assertThat(edgeSeeds.stream().map(EdgeSeed::getDestination).distinct().count(), equalTo(1L));
    }

    @Test
    public void shouldSetIdentifiers() {
        // Given
        final EdgeSeed edgeSeed1 = new EdgeSeed(1, 2, false);
        final EdgeSeed edgeSeed2 = new EdgeSeed(4, 3, false);

        // When
        edgeSeed1.setIdentifiers(4, 3, DirectedType.UNDIRECTED);

        // Then
        assertEquals(3, edgeSeed1.getSource());
        assertThat(edgeSeed1, equalTo(edgeSeed2));
    }

    @Test
    public void shouldFallbackToToStringComparisonIfSourceAndDestinationHaveDifferentTypes() {
        // Given
        final EdgeSeed edgeSeed1 = new EdgeSeed(1, "2", false);
        final EdgeSeed edgeSeed2 = new EdgeSeed("2", 1, false);

        // Then
        assertThat(edgeSeed1, equalTo(edgeSeed2));
    }

    @Test
    public void shouldDeserialiseFromJsonUsingDirectedTrueField() {
        // Given
        final String json = "{\"class\": \"uk.gov.gchq.gaffer.operation.data.EdgeSeed\", \"directed\": true}";

        // When
        final EdgeSeed deserialisedEdgeSeed = fromJson(json.getBytes());

        // Then
        assertEquals(DirectedType.DIRECTED, deserialisedEdgeSeed.getDirectedType());
    }

    @Test
    public void shouldDeserialiseFromJsonUsingDirectedFalseField() {
        // Given
        final String json = "{\"class\": \"uk.gov.gchq.gaffer.operation.data.EdgeSeed\", \"directed\": false}";

        // When
        final EdgeSeed deserialisedEdgeSeed = fromJson(json.getBytes());

        // Then
        assertEquals(DirectedType.UNDIRECTED, deserialisedEdgeSeed.getDirectedType());
    }

    @Test
    public void shouldDeserialiseFromJsonWhenDirectedTypeIsDirected() {
        // Given
        final String json = "{\"class\": \"uk.gov.gchq.gaffer.operation.data.EdgeSeed\", \"directedType\": \"DIRECTED\"}";

        // When
        final EdgeSeed deserialisedEdgeSeed = fromJson(json.getBytes());

        // Then
        assertEquals(DirectedType.DIRECTED, deserialisedEdgeSeed.getDirectedType());
    }

    @Test
    public void shouldDeserialiseFromJsonWhenDirectedTypeIsUndirected() {
        // Given
        final String json = "{\"class\": \"uk.gov.gchq.gaffer.operation.data.EdgeSeed\", \"directedType\": \"UNDIRECTED\"}";

        // When
        final EdgeSeed deserialisedEdgeSeed = fromJson(json.getBytes());

        // Then
        assertEquals(DirectedType.UNDIRECTED, deserialisedEdgeSeed.getDirectedType());
    }

    @Test
    public void shouldDeserialiseFromJsonWhenDirectedTypeIsEither() {
        // Given
        final String json = "{\"class\": \"uk.gov.gchq.gaffer.operation.data.EdgeSeed\", \"directedType\": \"EITHER\"}";

        // When
        final EdgeSeed deserialisedEdgeSeed = fromJson(json.getBytes());

        // Then
        assertEquals(DirectedType.EITHER, deserialisedEdgeSeed.getDirectedType());
    }


    @Test
    public void shouldThrowExceptionWhenDeserialiseFromJsonUsingDirectedAndDirectedType() {
        // Given
        final String json = "{\"class\": \"uk.gov.gchq.gaffer.operation.data.EdgeSeed\", \"directed\": true, \"directedType\": \"DIRECTED\"}";

        // When / Then
        try {
            fromJson(json.getBytes());
            fail("Exception expected");
        } catch (final Exception e) {
            assertTrue(e.getMessage().contains("not both"));
        }
    }

    @Override
    protected EdgeSeed getTestObject() {
        return new EdgeSeed();
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
            return new HashCodeBuilder(53, 41)
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
