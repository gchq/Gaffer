/*
 * Copyright 2017 Crown Copyright
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

package uk.gov.gchq.gaffer.store.serialiser.lengthvalue;

import org.junit.Before;
import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.id.EdgeId;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.operation.data.generator.EdgeIdExtractor;
import uk.gov.gchq.gaffer.operation.data.generator.EntityIdExtractor;
import uk.gov.gchq.gaffer.serialisation.implementation.StringSerialiser;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.serialiser.lengthvalue.EdgeIdSerialiser;
import uk.gov.gchq.gaffer.store.serialiser.lengthvalue.ElementIdSerialiser;
import uk.gov.gchq.gaffer.store.serialiser.lengthvalue.EntityIdSerialiser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ElementIdSerialiserTest {

    private Schema schema;
    private ElementIdSerialiser elementIdSerialiser;
    private static final String TEST_VERTEX = "testVertex";

    @Before
    public void setUp() {
        schema = new Schema.Builder()
                .vertexSerialiser(new StringSerialiser())
                .build();
        elementIdSerialiser = new ElementIdSerialiser(schema);
    }

    @Test
    public void testNullSerialiser() {
        // Given
        schema = new Schema.Builder()
                .build();

        // When / Then
        try {
            elementIdSerialiser = new ElementIdSerialiser(schema);
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Vertex serialiser is required"));
        }
    }

    @Test
    public void testCanSeraliseEdgeId() throws SerialisationException {
        // Given
        final EdgeIdSerialiser edgeIdSerialiser = new EdgeIdSerialiser(schema);
        final EdgeIdExtractor extractor = new EdgeIdExtractor();
        final Edge edge = new Edge(TestGroups.EDGE, "source", "destination", true);
        final EdgeId edgeId = extractor._apply(edge);

        // When
        final byte[] serialisedEdgeId = elementIdSerialiser.serialise(edgeId);
        final ElementId deserialisedElementId = edgeIdSerialiser.deserialise(serialisedEdgeId);

        // Then
        assertEquals(edgeId, deserialisedElementId);
    }

    @Test
    public void testCanSeraliseEntityId() throws SerialisationException {
        // Given
        final EntityIdSerialiser entityIdSerialiser = new EntityIdSerialiser(schema);
        final EntityIdExtractor extractor = new EntityIdExtractor();
        final Edge edge = new Edge(TestGroups.EDGE, "source", "destination", true);
        final EntityId entityId = extractor._apply(edge);

        // When
        final byte[] serialisedEntityId = elementIdSerialiser.serialise(entityId);
        final ElementId deserialisedElementId = entityIdSerialiser.deserialise(serialisedEntityId);

        // Then
        assertEquals(entityId, deserialisedElementId);
    }

    @Test
    public void testCanSeraliseEntityElementId() throws SerialisationException {
        // Given
        final EntityIdSerialiser entityIdSerialiser = new EntityIdSerialiser(schema);
        final EntityIdExtractor extractor = new EntityIdExtractor();
        final Edge edge = new Edge(TestGroups.EDGE, "source", "destination", true);
        final ElementId entityElementId = extractor._apply(edge);

        // When
        final byte[] serialisedEntityId = elementIdSerialiser.serialise(entityElementId);
        final ElementId deserialisedElementId = entityIdSerialiser.deserialise(serialisedEntityId);

        // Then
        assertEquals(entityElementId, deserialisedElementId);
    }

    @Test
    public void testCanSeraliseEdgeElementId() throws SerialisationException {
        // Given
        final EdgeIdSerialiser edgeIdSerialiser = new EdgeIdSerialiser(schema);
        final EdgeIdExtractor extractor = new EdgeIdExtractor();
        final Edge edge = new Edge(TestGroups.EDGE, "source", "destination", true);
        final ElementId edgeElementId = extractor._apply(edge);

        // When
        final byte[] serialisedEdgeId = elementIdSerialiser.serialise(edgeElementId);
        final ElementId deserialisedElementId = edgeIdSerialiser.deserialise(serialisedEdgeId);

        // Then
        assertEquals(edgeElementId, deserialisedElementId);
    }

    @Test
    public void testCanSerialiseVertex() throws SerialisationException {
        //Given
        final EntityIdSerialiser entityIdSerialiser = new EntityIdSerialiser(schema);

        // When
        final byte[] serialisedVertex = elementIdSerialiser.serialiseVertex(TEST_VERTEX);
        final EntityId deserialisedEntityId = entityIdSerialiser.deserialise(serialisedVertex);

        // Then
        assertEquals(TEST_VERTEX, deserialisedEntityId.getVertex());
    }

    @Test
    public void testCantSerialiseIntegerClass() throws SerialisationException {
        assertFalse(elementIdSerialiser.canHandle(Integer.class));
    }

    @Test
    public void testCanSerialiseElementIdClass() throws SerialisationException {
        assertTrue(elementIdSerialiser.canHandle(ElementId.class));
    }

    @Test
    public void testDeserialiseEmpty() throws SerialisationException {
        assertEquals(null, elementIdSerialiser.deserialiseEmpty());
    }

    @Test
    public void testPreserveObjectOrdering() throws SerialisationException {
        assertEquals(false, elementIdSerialiser.preservesObjectOrdering());
    }
}
