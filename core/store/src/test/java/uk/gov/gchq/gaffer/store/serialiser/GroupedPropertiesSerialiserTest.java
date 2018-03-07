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
import uk.gov.gchq.gaffer.data.element.GroupedProperties;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.implementation.StringSerialiser;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class GroupedPropertiesSerialiserTest {

    private Schema schema;
    private GroupedPropertiesSerialiser serialiser;

    @Before
    public void setUp() {
        final SchemaEdgeDefinition edgeDef = new SchemaEdgeDefinition.Builder()
                .build();

        schema = new Schema.Builder()
                .vertexSerialiser(new StringSerialiser())
                .edge(TestGroups.EDGE, edgeDef)
                .build();

        serialiser = new GroupedPropertiesSerialiser(schema);
    }

    @Test
    public void testCanSerialiseGroupedProperties() throws SerialisationException {
        // Given
        final GroupedProperties groupedProperties = new GroupedProperties(TestGroups.EDGE);

        // When
        final byte[] serialisedGroupedProperties = serialiser.serialise(groupedProperties);
        final GroupedProperties deserialisedGroupProperties = serialiser.deserialise(serialisedGroupedProperties);

        // Then
        assertEquals(groupedProperties, deserialisedGroupProperties);
    }

    @Test
    public void testGetGroup() throws SerialisationException {
        // Given
        final GroupedProperties groupedProperties = new GroupedProperties(TestGroups.EDGE);

        // When
        final byte[] serialisedEdge = serialiser.serialise(groupedProperties);

        // Then
        assertEquals(TestGroups.EDGE, serialiser.getGroup(serialisedEdge));
    }

    @Test
    public void testCantSerialiseIntegerClass() throws SerialisationException {
        assertFalse(serialiser.canHandle(Integer.class));
    }

    @Test
    public void testCanSerialiseGroupedPropertiesClass() throws SerialisationException {
        assertTrue(serialiser.canHandle(GroupedProperties.class));
    }

    @Test
    public void testDeserialiseEmpty() throws SerialisationException {
        assertEquals(null, serialiser.deserialiseEmpty());
    }

    @Test
    public void testPreserveObjectOrdering() throws SerialisationException {
        assertEquals(false, serialiser.preservesObjectOrdering());
    }
}
