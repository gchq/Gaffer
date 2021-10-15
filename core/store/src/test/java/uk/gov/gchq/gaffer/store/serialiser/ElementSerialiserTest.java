/*
 * Copyright 2017-2021 Crown Copyright
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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.implementation.StringSerialiser;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ElementSerialiserTest {

    private static Schema schema;
    private static ElementSerialiser serialiser;
    private static final String TEST_VERTEX = "testVertex";

    @BeforeAll
    public static void setUp() {
        final SchemaEdgeDefinition edgeDef = new SchemaEdgeDefinition.Builder()
                .build();
        final SchemaEntityDefinition entityDef = new SchemaEntityDefinition.Builder()
                .build();

        schema = new Schema.Builder()
                .entity(TestGroups.ENTITY, entityDef)
                .edge(TestGroups.EDGE, edgeDef)
                .vertexSerialiser(new StringSerialiser())
                .build();
        serialiser = new ElementSerialiser(schema);
    }

    @Test
    public void testCanSerialiseEdgeElement() throws SerialisationException {
        // Given
        final Edge edge = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source("source")
                .dest("destination")
                .directed(true)
                .build();

        // When
        final byte[] serialisedEdge = serialiser.serialise(edge);
        final Element deserialisedElement = serialiser.deserialise(serialisedEdge);

        // Then
        assertEquals(edge, deserialisedElement);
    }

    @Test
    public void testCanSerialiseEntityElement() throws SerialisationException {
        // Given
        final Entity entity = new Entity(TestGroups.ENTITY, TEST_VERTEX);

        // When
        final byte[] serialisedEntity = serialiser.serialise(entity);
        final Element deserialisedEntity = serialiser.deserialise(serialisedEntity);

        // Then
        assertEquals(entity, deserialisedEntity);
    }

    @Test
    public void testGetGroup() throws SerialisationException {
        // Given
        final Edge edge = new Edge.Builder().group(TestGroups.ENTITY)
                .source("source")
                .dest("destination")
                .directed(true)
                .build();

        // When
        final byte[] serialisedEdge = serialiser.serialise(edge);

        // Then
        assertEquals(TestGroups.ENTITY, serialiser.getGroup(serialisedEdge));
    }

    @Test
    public void testCantSerialiseIntegerClass() throws SerialisationException {
        assertFalse(serialiser.canHandle(Integer.class));
    }

    @Test
    public void testCanSerialiseElementClass() throws SerialisationException {
        assertTrue(serialiser.canHandle(Element.class));
    }

    @Test
    public void testDeserialiseEmpty() throws SerialisationException {
        assertThat(serialiser.deserialiseEmpty()).isNull();
    }

    @Test
    public void testPreserveObjectOrdering() throws SerialisationException {
        assertEquals(false, serialiser.preservesObjectOrdering());
    }
}
