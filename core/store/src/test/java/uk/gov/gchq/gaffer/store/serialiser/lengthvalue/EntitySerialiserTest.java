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
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.implementation.StringSerialiser;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.store.serialiser.lengthvalue.EntitySerialiser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class EntitySerialiserTest {

    private Schema schema;
    private EntitySerialiser entitySerialiser;

    @Before
    public void setUp() {
        final SchemaEntityDefinition entityDef = new SchemaEntityDefinition.Builder()
                .build();

        schema = new Schema.Builder()
                .vertexSerialiser(new StringSerialiser())
                .entity(TestGroups.ENTITY, entityDef)
                .build();

        entitySerialiser = new EntitySerialiser(schema);
    }

    @Test
    public void testNullSerialiser() {
        // Given
        schema = new Schema.Builder()
                .build();

        // When / Then
        try {
            entitySerialiser = new EntitySerialiser(schema);
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Vertex serialiser is required"));
        }
    }

    @Test
    public void testCanSeraliseEntity() throws SerialisationException {
        // Given
        final Entity entity = new Entity(TestGroups.ENTITY, "testVertex");

        // When
        final byte[] serialisedEntity = entitySerialiser.serialise(entity);
        final Entity deserialisedEntity = entitySerialiser.deserialise(serialisedEntity);

        // Then
        assertEquals(entity, deserialisedEntity);
    }

    @Test
    public void testCantSerialiseIntegerClass() throws SerialisationException {
        assertFalse(entitySerialiser.canHandle(Integer.class));
    }

    @Test
    public void testCanSerialiseElementClass() throws SerialisationException {
        assertTrue(entitySerialiser.canHandle(Entity.class));
    }

    @Test
    public void testDeserialiseEmpty() throws SerialisationException {
        assertEquals(null, entitySerialiser.deserialiseEmpty());
    }

    @Test
    public void testPreserveObjectOrdering() throws SerialisationException {
        assertEquals(false, entitySerialiser.preservesObjectOrdering());
    }
}
