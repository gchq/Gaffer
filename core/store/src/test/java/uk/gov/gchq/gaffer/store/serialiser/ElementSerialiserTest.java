/*
 * Copyright 2017-2018 Crown Copyright
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
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.implementation.StringSerialiser;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ElementSerialiserTest {

    private Schema schema;
    private ElementSerialiser elementSerialiser;
    private static final String TEST_VERTEX = "testVertex";

    @Before
    public void setUp() {
        final SchemaEdgeDefinition edgeDef = new SchemaEdgeDefinition.Builder()
                .build();
        final SchemaEntityDefinition entityDef = new SchemaEntityDefinition.Builder()
                .build();

        schema = new Schema.Builder()
                .entity(TestGroups.ENTITY, entityDef)
                .edge(TestGroups.EDGE, edgeDef)
                .vertexSerialiser(new StringSerialiser())
                .build();
        elementSerialiser = new ElementSerialiser(schema);
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
        final byte[] serialisedEdge = elementSerialiser.serialise(edge);
        final Element deserialisedElement = elementSerialiser.deserialise(serialisedEdge);

        // Then
        assertEquals(edge, deserialisedElement);
    }

    @Test
    public void testCanSerialiseEntityElement() throws SerialisationException {
        // Given
        final Entity entity = new Entity(TestGroups.ENTITY, TEST_VERTEX);

        // When
        final byte[] serialisedEntity = elementSerialiser.serialise(entity);
        final Element deserialisedEntity = elementSerialiser.deserialise(serialisedEntity);

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
        final byte[] serialisedEdge = elementSerialiser.serialise(edge);

        // Then
        assertEquals(TestGroups.ENTITY, elementSerialiser.getGroup(serialisedEdge));
    }

    @Test
    public void testCantSerialiseIntegerClass() throws SerialisationException {
        assertFalse(elementSerialiser.canHandle(Integer.class));
    }

    @Test
    public void testCanSerialiseElementClass() throws SerialisationException {
        assertTrue(elementSerialiser.canHandle(Element.class));
    }

    @Test
    public void testDeserialiseEmpty() throws SerialisationException {
        assertEquals(null, elementSerialiser.deserialiseEmpty());
    }

    @Test
    public void testPreserveObjectOrdering() throws SerialisationException {
        assertEquals(false, elementSerialiser.preservesObjectOrdering());
    }
}
