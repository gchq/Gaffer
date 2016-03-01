/*
 * Copyright 2016 Crown Copyright
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

package gaffer.store.schema;

import gaffer.commonutil.StreamUtil;
import gaffer.commonutil.TestGroups;
import gaffer.commonutil.TestPropertyNames;
import gaffer.data.element.IdentifierType;
import gaffer.serialisation.Serialisation;
import gaffer.serialisation.implementation.JavaSerialiser;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.NotSerializableException;
import java.util.Map;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;

public class StoreSchemaTest {

    @Test
    public void shouldLoadAllPartsOfSchemaFromJsonCorrectly() throws IOException {
        // When
        StoreSchema storeSchema = loadStoreSchema();

        assertEquals(5, storeSchema.getPositions().size());

        // Then - check entity definition
        assertEquals(1, storeSchema.getEntities().size());
        final StoreElementDefinition entityDef = storeSchema.getElement(TestGroups.ENTITY);
        assertNotNull(entityDef);

        assertEquals(1, entityDef.getPropertyDefinitions().size());
        final StorePropertyDefinition property1 = entityDef.getPropertyDefinitions().iterator().next();
        assertSame(property1, entityDef.getProperty(TestPropertyNames.F1));
        assertEquals(JavaSerialiser.class, property1.getSerialiser().getClass());
        assertEquals("PROPERTY_1", property1.getPosition());

        // Then - check edge definition
        assertEquals(1, storeSchema.getEdges().size());
        final StoreElementDefinition edgeDef = storeSchema.getEdge(TestGroups.EDGE);
        assertNotNull(edgeDef);

        assertEquals(2, edgeDef.getPropertyDefinitions().size());
        final StorePropertyDefinition property2 = (StorePropertyDefinition) edgeDef.getPropertyDefinitions().toArray()[0];
        assertSame(property2, edgeDef.getProperty(TestPropertyNames.F2));
        assertEquals(JavaSerialiser.class, property2.getSerialiser().getClass());
        assertEquals("PROPERTY_1", property2.getPosition());

        final StorePropertyDefinition property3 = (StorePropertyDefinition) edgeDef.getPropertyDefinitions().toArray()[1];
        assertSame(property3, edgeDef.getProperty(TestPropertyNames.DATE));
        assertEquals(JavaSerialiser.class, property3.getSerialiser().getClass());
        assertEquals("PROPERTY_2", property3.getPosition());
    }

    @Test
    public void testCorrectSerialiserRetrievableFromConfig() throws NotSerializableException {
        StoreSchema store = new StoreSchema.Builder()
                .edge(TestGroups.EDGE, new StoreElementDefinition.Builder()
                        .property(TestPropertyNames.F1, new StorePropertyDefinition.Builder()
                                .serialiser(new JavaSerialiser())
                                .build())
                        .build())
                .build();

        assertEquals(JavaSerialiser.class, store.getElement(TestGroups.EDGE)
                .getProperty(TestPropertyNames.F1).getSerialiser().getClass());
    }

    @Test
    public void testStoreConfigUsableWithSchemaInitialisationAndProgramaticListOfElements() {
        final StoreElementDefinition entityDef = new StoreElementDefinition.Builder()
                .property(TestPropertyNames.F1, new StorePropertyDefinition.Builder()
                        .position("0")
                        .serialiser(new JavaSerialiser())
                        .build())
                .build();

        final StoreElementDefinition edgeDef = new StoreElementDefinition.Builder()
                .property(TestPropertyNames.F2, new StorePropertyDefinition.Builder()
                        .position("0")
                        .serialiser(new JavaSerialiser())
                        .build())
                .build();

        final StoreSchema storeSchema = new StoreSchema.Builder()
                .entity(TestGroups.ENTITY, entityDef)
                .edge(TestGroups.EDGE, edgeDef)
                .build();

        assertSame(entityDef, storeSchema.getEntity(TestGroups.ENTITY));
        assertSame(edgeDef, storeSchema.getEdge(TestGroups.EDGE));
    }

    @Test
    public void shouldConvertToJsonCorrectly() throws IOException {
        // Given
        final StoreSchema storeSchema = loadStoreSchema();

        // When
        final byte[] json = storeSchema.toJson(true);

        // Then
        assertEquals("{\n" +
                "  \"edges\" : {\n" +
                "    \"BasicEdge\" : {\n" +
                "      \"properties\" : {\n" +
                "        \"property2\" : {\n" +
                "          \"position\" : \"PROPERTY_1\"\n" +
                "        },\n" +
                "        \"dateProperty\" : {\n" +
                "          \"position\" : \"PROPERTY_2\"\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "  \"entities\" : {\n" +
                "    \"BasicEntity\" : {\n" +
                "      \"properties\" : {\n" +
                "        \"property1\" : {\n" +
                "          \"position\" : \"PROPERTY_1\"\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "  \"positions\" : {\n" +
                "    \"GROUP\" : \"GROUP\",\n" +
                "    \"VERTEX\" : \"VERTEX\",\n" +
                "    \"SOURCE\" : \"SOURCE\",\n" +
                "    \"DESTINATION\" : \"DESTINATION\",\n" +
                "    \"DIRECTED\" : \"DIRECTED\"\n" +
                "  }\n" +
                "}", new String(json));
    }

    private StoreSchema loadStoreSchema() throws IOException {
        return StoreSchema.fromJson(StreamUtil.storeSchema(getClass()));
    }

    @Test
    public void testStoreSchemaConstructedFromInputStream() throws IOException {
        final InputStream resourceAsStream = this.getClass().getResourceAsStream(StreamUtil.STORE_SCHEMA);
        assertNotNull(resourceAsStream);
        final StoreSchema deserialisedStoreSchema = StoreSchema.fromJson(resourceAsStream);
        assertNotNull(deserialisedStoreSchema);

        final Map<String, StoreElementDefinition> edges = deserialisedStoreSchema.getEdges();

        assertEquals(1, edges.size());
        final StoreElementDefinition edgeGroup = edges.get(TestGroups.EDGE);
        assertEquals(2, edgeGroup.getProperties().size());

        final Map<String, StoreElementDefinition> entities = deserialisedStoreSchema.getEntities();

        assertEquals(1, entities.size());
        final StoreElementDefinition entityGroup = entities.get(TestGroups.ENTITY);
        assertEquals(1, entityGroup.getProperties().size());
    }

    @Test
    public void shouldDeserialiseAndReserialiseIntoTheSameJson() throws IOException {
        //Given
        final StoreSchema storeSchema1 = loadStoreSchema();
        final byte[] json1 = storeSchema1.toJson(false);
        final StoreSchema storeSchema2 = StoreSchema.fromJson(json1);

        // When
        final byte[] json2 = storeSchema2.toJson(false);

        // Then
        Assert.assertEquals(new String(json1), new String(json2));
    }

    @Test
    public void shouldDeserialiseAndReserialiseIntoTheSamePrettyJson() throws IOException {
        //Given
        final StoreSchema storeSchema1 = loadStoreSchema();
        final byte[] json1 = storeSchema1.toJson(true);
        final StoreSchema storeSchema2 = StoreSchema.fromJson(json1);

        // When
        final byte[] json2 = storeSchema2.toJson(true);

        // Then
        Assert.assertEquals(new String(json1), new String(json2));
    }

    @Test
    public void shouldBuildStoreSchema() {
        // Given
        final IdentifierType idType1 = IdentifierType.SOURCE;
        final IdentifierType idType2 = IdentifierType.DESTINATION;

        final String position1 = "1";
        final String position2 = "2";

        final StoreElementDefinition edgeDef1 = mock(StoreElementDefinition.class);
        final StoreElementDefinition edgeDef2 = mock(StoreElementDefinition.class);
        final StoreElementDefinition entityDef1 = mock(StoreElementDefinition.class);
        final StoreElementDefinition entityDef2 = mock(StoreElementDefinition.class);
        final Serialisation vertexSerialiser = mock(Serialisation.class);

        // When
        final StoreSchema schema = new StoreSchema.Builder()
                .edge(TestGroups.EDGE, edgeDef1)
                .entity(TestGroups.ENTITY, entityDef1)
                .entity(TestGroups.ENTITY_2, entityDef2)
                .edge(TestGroups.EDGE_2, edgeDef2)
                .vertexSerialiser(vertexSerialiser)
                .position(idType1.name(), position1)
                .position(idType2.name(), position2)
                .build();

        // Then
        assertEquals(2, schema.getEdges().size());
        assertSame(edgeDef1, schema.getEdge(TestGroups.EDGE));
        assertSame(edgeDef2, schema.getEdge(TestGroups.EDGE_2));

        assertEquals(2, schema.getEntities().size());
        assertSame(entityDef1, schema.getEntity(TestGroups.ENTITY));
        assertSame(entityDef2, schema.getEntity(TestGroups.ENTITY_2));

        assertEquals(2, schema.getPositions().size());
        assertSame(position1, schema.getPosition(idType1.name()));
        assertSame(position2, schema.getPosition(idType2.name()));

        assertSame(vertexSerialiser, schema.getVertexSerialiser());
    }
}
