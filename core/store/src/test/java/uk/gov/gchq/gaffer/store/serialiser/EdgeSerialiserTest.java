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

package uk.gov.gchq.gaffer.store.serialiser;

import org.junit.Before;
import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.implementation.StringSerialiser;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class EdgeSerialiserTest {

    private Schema schema;
    private EdgeSerialiser edgeSerialiser;

    @Before
    public void setUp() {

        final SchemaEdgeDefinition edgeDef = new SchemaEdgeDefinition.Builder()
                .build();

        schema = new Schema.Builder()
                .vertexSerialiser(new StringSerialiser())
                .edge(TestGroups.EDGE, edgeDef)
                .build();
        edgeSerialiser = new EdgeSerialiser(schema);
    }

    @Test
    public void testNullSerialiser() {
        // Given
        schema = new Schema.Builder()
                .build();

        // When / Then
        try {
            edgeSerialiser = new EdgeSerialiser(schema);
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Vertex serialiser is required"));
        }
    }

    @Test
    public void testCanSeraliseEdge() throws SerialisationException {
        // Given
        final Edge edge = new Edge(TestGroups.EDGE, "source", "destination", true);

        // When
        final byte[] serialisedEdge = edgeSerialiser.serialise(edge);
        final Edge deserialisedEdge = edgeSerialiser.deserialise(serialisedEdge);

        // Then
        assertEquals(edge, deserialisedEdge);
    }

    @Test
    public void testCantSerialiseIntegerClass() throws SerialisationException {
        assertFalse(edgeSerialiser.canHandle(Integer.class));
    }

    @Test
    public void testCanSerialiseEdgeClass() throws SerialisationException {
        assertTrue(edgeSerialiser.canHandle(Edge.class));
    }

    @Test
    public void testDeserialiseEmpty() throws SerialisationException {
        assertEquals(null, edgeSerialiser.deserialiseEmpty());
    }

    @Test
    public void testPreserveObjectOrdering() throws SerialisationException {
        assertEquals(false, edgeSerialiser.preservesObjectOrdering());
    }
}
