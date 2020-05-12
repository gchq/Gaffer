/*
 * Copyright 2017-2020 Crown Copyright
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

package uk.gov.gchq.gaffer.store.serialiser;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.data.element.id.EdgeId;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.serialisation.implementation.StringSerialiser;
import uk.gov.gchq.gaffer.store.schema.Schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class EdgeIdSerialiserTest {

    private Schema schema;
    private EdgeIdSerialiser serialiser;

    @BeforeEach
    public void setUp() {
        schema = new Schema.Builder()
                .vertexSerialiser(new StringSerialiser())
                .build();
        serialiser = new EdgeIdSerialiser(schema);
    }

    @Test
    public void testNullSerialiser() {
        // Given
        schema = new Schema.Builder().build();

        // When / Then
        final Exception exception = assertThrows(IllegalArgumentException.class, () -> new EdgeIdSerialiser(schema));
        assertEquals("Vertex serialiser is required",exception.getMessage());
    }

    @Test
    public void testCanSerialiseEdgeId() throws SerialisationException {
        // Given
        final EdgeId edgeId = new EdgeSeed("source", "destination", true);

        // When
        final byte[] serialisedEdgeId = serialiser.serialise(edgeId);
        final EdgeId deserialisedEdgeId = serialiser.deserialise(serialisedEdgeId);

        // Then
        assertEquals(edgeId, deserialisedEdgeId);
    }

    @Test
    public void testCantSerialiseIntegerClass() {
        assertFalse(serialiser.canHandle(Integer.class));
    }

    @Test
    public void testCanSerialiseEdgeIdClass() {
        assertTrue(serialiser.canHandle(EdgeId.class));
    }

    @Test
    public void testDeserialiseEmpty() throws SerialisationException {
        assertNull(serialiser.deserialiseEmpty());
    }

    @Test
    public void testPreserveObjectOrdering()  {
        assertTrue(serialiser.preservesObjectOrdering());
    }

    @Test
    public void testIsConsistent() {
        assertTrue(serialiser.isConsistent());
    }
}
