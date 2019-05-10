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

import org.junit.Test;

import uk.gov.gchq.gaffer.JSONSerialisationTest;
import uk.gov.gchq.gaffer.data.element.id.EdgeId;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class EntitySeedTest extends JSONSerialisationTest<EntitySeed> {
    @Test
    public void shouldBeRelatedToEdgeIdWhenSourceEqualsVertex() {
        // Given
        final String source = "source";
        final String destination = "destination";
        final EntityId seed = new EntitySeed(source);
        final EdgeId relatedSeed = mock(EdgeId.class);

        given(relatedSeed.getSource()).willReturn(source);
        given(relatedSeed.getDestination()).willReturn(destination);

        // When
        final boolean isRelated = seed.isRelated((ElementId) relatedSeed).isMatch();

        // Then
        assertTrue(isRelated);
    }

    @Test
    public void shouldBeRelatedToEdgeIdWhenDestinationEqualsVertex() {
        // Given
        final String source = "source";
        final String destination = "destination";
        final EntityId seed = new EntitySeed(destination);
        final EdgeId relatedSeed = mock(EdgeId.class);

        given(relatedSeed.getSource()).willReturn(source);
        given(relatedSeed.getDestination()).willReturn(destination);

        // When
        final boolean isRelated = seed.isRelated((ElementId) relatedSeed).isMatch();

        // Then
        assertTrue(isRelated);
    }

    @Test
    public void shouldBeRelatedToEdgeIdWhenSourceAndVertexAreNull() {
        // Given
        final String source = null;
        final String destination = "destination";
        final EntityId seed = new EntitySeed(destination);
        final EdgeId relatedSeed = mock(EdgeId.class);

        given(relatedSeed.getSource()).willReturn(source);
        given(relatedSeed.getDestination()).willReturn(destination);

        // When
        final boolean isRelated = seed.isRelated((ElementId) relatedSeed).isMatch();

        // Then
        assertTrue(isRelated);
    }

    @Test
    public void shouldBeRelatedToEdgeIdWhenDestinationAndVertexAreNull() {
        // Given
        final String source = "source";
        final String destination = null;
        final EntityId seed = new EntitySeed(destination);
        final EdgeId relatedSeed = mock(EdgeId.class);

        given(relatedSeed.getSource()).willReturn(source);
        given(relatedSeed.getDestination()).willReturn(destination);

        // When
        final boolean isRelated = seed.isRelated((ElementId) relatedSeed).isMatch();

        // Then
        assertTrue(isRelated);
    }

    @Test
    public void shouldNotBeRelatedToEdgeIdWhenVertexNotEqualToSourceOrDestination() {
        // Given
        final String source = "source";
        final String destination = "destination";
        final boolean directed = true;
        final EntityId seed = new EntitySeed("other vertex");
        final EdgeId relatedSeed = mock(EdgeId.class);

        given(relatedSeed.getSource()).willReturn(source);
        given(relatedSeed.getDestination()).willReturn(destination);
        given(relatedSeed.isDirected()).willReturn(directed);

        // When
        final boolean isRelated = seed.isRelated((ElementId) relatedSeed).isMatch();

        // Then
        assertFalse(isRelated);
    }

    @Test
    public void shouldBeRelatedToEntityId() {
        // Given
        final EntityId seed1 = new EntitySeed("vertex");
        final EntityId seed2 = new EntitySeed("vertex");

        // When
        final boolean isRelated = seed1.isRelated(seed2).isMatch();

        // Then
        assertTrue(isRelated);
    }

    @Test
    public void shouldBeEqualWhenVerticesEqual() {
        // Given
        final String vertex = "vertex";
        final EntityId seed1 = new EntitySeed(vertex);
        final EntityId seed2 = new EntitySeed(vertex);

        // When
        final boolean isEqual = seed1.equals(seed2);

        // Then
        assertTrue(isEqual);
        assertEquals(seed1.hashCode(), seed2.hashCode());
    }

    @Test
    public void shouldNotBeEqualWhenVerticesEqual() {
        // Given
        final EntityId seed1 = new EntitySeed("vertex");
        final EntityId seed2 = new EntitySeed("other vertex");

        // When
        final boolean isEqual = seed1.equals(seed2);

        // Then
        assertFalse(isEqual);
        assertNotEquals(seed1.hashCode(), seed2.hashCode());
    }

    @Test
    public void shouldSerialiseAndDeserialiseIntegersAndLongs() throws SerialisationException {
        // Given
        final Long vertex1 = 1L;
        final Integer vertex2 = 2;
        final EntityId seed1 = new EntitySeed(vertex1);
        final EntityId seed2 = new EntitySeed(vertex2);

        // When
        final byte[] bytes1 = JSONSerialiser.serialise(seed1);
        final byte[] bytes2 = JSONSerialiser.serialise(seed2);
        final EntityId seed1Deserialised = JSONSerialiser.deserialise(bytes1, EntityId.class);
        final EntityId seed2Deserialised = JSONSerialiser.deserialise(bytes2, EntityId.class);


        // Then
        assertEquals(seed1, seed1Deserialised);
        assertEquals(seed2, seed2Deserialised);
        assertTrue(seed1Deserialised.getVertex() instanceof Long);
        assertTrue(seed2Deserialised.getVertex() instanceof Integer);
    }

    @Test
    public void shouldSerialiseAndDeserialiseCustomVertexObjects() throws SerialisationException {
        // Given
        final CustomVertex vertex = new CustomVertex();
        vertex.setType("type");
        vertex.setValue("value");
        final EntityId seed = new EntitySeed(vertex);

        // When
        final byte[] bytes = JSONSerialiser.serialise(seed);
        final EntityId seedDeserialised = JSONSerialiser.deserialise(bytes, EntityId.class);

        // Then
        assertTrue(seedDeserialised.getVertex() instanceof CustomVertex);
        assertEquals("type", ((CustomVertex) seedDeserialised.getVertex()).getType());
        assertEquals("value", ((CustomVertex) seedDeserialised.getVertex()).getValue());
    }

    @Override
    protected EntitySeed getTestObject() {
        return new EntitySeed();
    }
}
