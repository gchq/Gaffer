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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import gaffer.commonutil.StreamUtil;
import gaffer.commonutil.TestGroups;
import gaffer.commonutil.TestPropertyNames;
import gaffer.commonutil.TestTypes;
import gaffer.data.element.ElementComponentKey;
import gaffer.data.element.IdentifierType;
import gaffer.data.element.function.ElementAggregator;
import gaffer.data.element.function.ElementFilter;
import gaffer.data.elementdefinition.exception.SchemaException;
import gaffer.exception.SerialisationException;
import gaffer.function.AggregateFunction;
import gaffer.function.ExampleAggregateFunction;
import gaffer.function.ExampleFilterFunction;
import gaffer.function.FilterFunction;
import gaffer.function.IsA;
import gaffer.function.context.ConsumerFunctionContext;
import gaffer.function.context.PassThroughFunctionContext;
import gaffer.serialisation.Serialisation;
import gaffer.serialisation.implementation.JavaSerialiser;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;
import java.io.InputStream;
import java.io.NotSerializableException;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class SchemaTest {
    private Schema schema;

    @Before
    public void setup() throws IOException {
        schema = Schema.fromJson(StreamUtil.schemas(getClass()));
    }

    @Test
    public void shouldDeserialiseAndReserialiseIntoTheSameJson() throws SerialisationException {
        //Given
        final byte[] json1 = schema.toJson(false);
        final Schema schema2 = Schema.fromJson(Schema.class, json1);

        // When
        final byte[] json2 = schema2.toJson(false);

        // Then
        assertEquals(new String(json1), new String(json2));
    }

    @Test
    public void shouldDeserialiseAndReserialiseIntoTheSamePrettyJson() throws SerialisationException {
        //Given
        final byte[] json1 = schema.toJson(true);
        final Schema schema2 = Schema.fromJson(json1);

        // When
        final byte[] json2 = schema2.toJson(true);

        // Then
        assertEquals(new String(json1), new String(json2));
    }

    @Test
    public void testLoadingSchemaFromJson() {
        // Edge definitions
        SchemaElementDefinition edgeDefinition = schema.getEdge(TestGroups.EDGE);
        assertNotNull(edgeDefinition);

        final Map<String, String> propertyMap = edgeDefinition.getPropertyMap();
        assertEquals(2, propertyMap.size());
        assertEquals("prop.string", propertyMap.get(TestPropertyNames.PROP_2));
        assertEquals("prop.date", propertyMap.get(TestPropertyNames.DATE));

        // Check validator
        ElementFilter validator = edgeDefinition.getValidator();
        final List<ConsumerFunctionContext<ElementComponentKey, FilterFunction>> valContexts = validator.getFunctions();
        int index = 0;

        ConsumerFunctionContext<ElementComponentKey, FilterFunction> valContext = valContexts.get(index++);
        assertTrue(valContext.getFunction() instanceof IsA);
        assertEquals(1, valContext.getSelection().size());
        assertEquals(IdentifierType.SOURCE, valContext.getSelection().get(0).getIdentifierType());

        valContext = valContexts.get(index++);
        assertTrue(valContext.getFunction() instanceof IsA);
        assertEquals(1, valContext.getSelection().size());
        assertEquals(IdentifierType.DESTINATION, valContext.getSelection().get(0).getIdentifierType());

        valContext = valContexts.get(index++);
        assertTrue(valContext.getFunction() instanceof IsA);
        assertEquals(1, valContext.getSelection().size());
        assertEquals(IdentifierType.DIRECTED, valContext.getSelection().get(0).getIdentifierType());

        valContext = valContexts.get(index++);
        assertTrue(valContext.getFunction() instanceof ExampleFilterFunction);
        assertEquals(1, valContext.getSelection().size());
        assertEquals(IdentifierType.DIRECTED, valContext.getSelection().get(0).getIdentifierType());

        valContext = valContexts.get(index++);
        assertTrue(valContext.getFunction() instanceof IsA);
        assertEquals(1, valContext.getSelection().size());
        assertEquals(TestPropertyNames.PROP_2, valContext.getSelection().get(0).getPropertyName());

        valContext = valContexts.get(index++);
        assertTrue(valContext.getFunction() instanceof ExampleFilterFunction);
        assertEquals(1, valContext.getSelection().size());
        assertEquals(TestPropertyNames.PROP_2, valContext.getSelection().get(0).getPropertyName());

        valContext = valContexts.get(index++);
        assertTrue(valContext.getFunction() instanceof IsA);
        assertEquals(1, valContext.getSelection().size());
        assertEquals(TestPropertyNames.DATE, valContext.getSelection().get(0).getPropertyName());

        assertEquals(index, valContexts.size());

        TypeDefinition type = edgeDefinition.getPropertyTypeDef(TestPropertyNames.DATE);
        assertEquals(Date.class, type.getClazz());
        assertEquals(JavaSerialiser.class, type.getSerialiser().getClass());
        assertEquals("PROPERTY_2", type.getPosition());
        assertTrue(type.getAggregateFunction() instanceof ExampleAggregateFunction);


        // Entity definitions
        SchemaElementDefinition entityDefinition = schema.getEntity(TestGroups.ENTITY);
        assertNotNull(entityDefinition);
        assertTrue(entityDefinition.containsProperty(TestPropertyNames.PROP_1));
        type = entityDefinition.getPropertyTypeDef(TestPropertyNames.PROP_1);
        assertEquals(String.class, type.getClazz());
        assertEquals(JavaSerialiser.class, type.getSerialiser().getClass());
        assertEquals("PROPERTY_1", type.getPosition());
        assertTrue(type.getAggregateFunction() instanceof ExampleAggregateFunction);

        ElementAggregator aggregator = edgeDefinition.getAggregator();
        List<PassThroughFunctionContext<ElementComponentKey, AggregateFunction>> aggContexts = aggregator.getFunctions();
        assertEquals(2, aggContexts.size());

        PassThroughFunctionContext<ElementComponentKey, AggregateFunction> aggContext = aggContexts.get(0);
        assertTrue(aggContext.getFunction() instanceof ExampleAggregateFunction);
        assertEquals(1, aggContext.getSelection().size());
        assertEquals(TestPropertyNames.PROP_2, aggContext.getSelection().get(0).getPropertyName());


        aggContext = aggContexts.get(1);
        assertTrue(aggContext.getFunction() instanceof ExampleAggregateFunction);
        assertEquals(1, aggContext.getSelection().size());
        assertEquals(TestPropertyNames.DATE, aggContext.getSelection().get(0).getPropertyName());


    }

    @Test
    public void createProgramaticSchema() {
        schema = createSchema();
    }

    private Schema createSchema() {
        return new Schema.Builder()
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .property(TestPropertyNames.PROP_1, TestTypes.PROP_STRING, String.class)
                        .property(TestPropertyNames.PROP_2, TestTypes.PROP_INTEGER, Integer.class)
                        .validator(new ElementFilter.Builder()
                                .select(TestPropertyNames.PROP_1)
                                .execute(new ExampleFilterFunction())
                                .build())
                        .build())
                .type(TestTypes.PROP_STRING, String.class)
                .type(TestTypes.PROP_INTEGER, Integer.class)
                .build();
    }

    @Test
    public void writeProgramaticSchemaAsJson() throws IOException, SchemaException {
        schema = createSchema();
        assertEquals(String.format("{%n" +
                "  \"edges\" : {%n" +
                "    \"BasicEdge\" : {%n" +
                "      \"properties\" : {%n" +
                "        \"property1\" : \"prop.string\",%n" +
                "        \"property2\" : \"prop.integer\"%n" +
                "      },%n" +
                "      \"validateFunctions\" : [ {%n" +
                "        \"function\" : {%n" +
                "          \"class\" : \"gaffer.function.ExampleFilterFunction\"%n" +
                "        },%n" +
                "        \"selection\" : [ {%n" +
                "          \"key\" : \"property1\"%n" +
                "        } ]%n" +
                "      } ]%n" +
                "    }%n" +
                "  },%n" +
                "  \"types\" : {%n" +
                "    \"prop.integer\" : {%n" +
                "      \"class\" : \"java.lang.Integer\"%n" +
                "    },%n" +
                "    \"prop.string\" : {%n" +
                "      \"class\" : \"java.lang.String\"%n" +
                "    }%n" +
                "  }%n" +
                "}"), new String(schema.toJson(true)));
    }

    @Test
    public void testCorrectSerialiserRetrievableFromConfig() throws NotSerializableException {
        Schema store = new Schema.Builder()
                .type(TestTypes.PROP_STRING, new TypeDefinition.Builder()
                        .clazz(String.class)
                        .serialiser(new JavaSerialiser())
                        .build())
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .property(TestPropertyNames.PROP_1, TestTypes.PROP_STRING)
                        .build())
                .build();

        assertEquals(JavaSerialiser.class,
                store.getElement(TestGroups.EDGE)
                        .getPropertyTypeDef(TestPropertyNames.PROP_1)
                        .getSerialiser()
                        .getClass());
    }

    @Test
    public void testStoreConfigUsableWithSchemaInitialisationAndProgramaticListOfElements() {
        final SchemaEntityDefinition entityDef = new SchemaEntityDefinition.Builder()
                .property(TestPropertyNames.PROP_1, TestTypes.PROP_STRING)
                .build();

        final SchemaEdgeDefinition edgeDef = new SchemaEdgeDefinition.Builder()
                .property(TestPropertyNames.PROP_2, TestTypes.PROP_STRING)
                .build();

        final Schema schema = new Schema.Builder()
                .type(TestTypes.PROP_STRING, String.class)
                .type(TestTypes.PROP_STRING, Integer.class)
                .entity(TestGroups.ENTITY, entityDef)
                .edge(TestGroups.EDGE, edgeDef)
                .build();

        assertSame(entityDef, schema.getEntity(TestGroups.ENTITY));
        assertSame(edgeDef, schema.getEdge(TestGroups.EDGE));
    }

    @Test
    public void testSchemaConstructedFromInputStream() throws IOException {
        final InputStream resourceAsStream = this.getClass().getResourceAsStream(StreamUtil.DATA_SCHEMA);
        assertNotNull(resourceAsStream);
        final Schema deserialisedSchema = Schema.fromJson(resourceAsStream);
        assertNotNull(deserialisedSchema);

        final Map<String, SchemaEdgeDefinition> edges = deserialisedSchema.getEdges();

        assertEquals(1, edges.size());
        final SchemaElementDefinition edgeGroup = edges.get(TestGroups.EDGE);
        assertEquals(2, edgeGroup.getProperties().size());

        final Map<String, SchemaEntityDefinition> entities = deserialisedSchema.getEntities();

        assertEquals(1, entities.size());
        final SchemaElementDefinition entityGroup = entities.get(TestGroups.ENTITY);
        assertEquals(1, entityGroup.getProperties().size());
    }

    @Test
    public void shouldBuildSchema() {
        // Given
        final Serialisation vertexSerialiser = mock(Serialisation.class);

        // When
        final Schema schema = new Schema.Builder()
                .edge(TestGroups.EDGE)
                .entity(TestGroups.ENTITY)
                .entity(TestGroups.ENTITY_2)
                .edge(TestGroups.EDGE_2)
                .vertexSerialiser(vertexSerialiser)
                .position(IdentifierType.VERTEX.name(), "position1")
                .position(IdentifierType.SOURCE.name(), "position2")
                .type(TestTypes.PROP_STRING, String.class)
                .build();

        // Then
        assertEquals(2, schema.getEdges().size());
        assertNotNull(schema.getEdge(TestGroups.EDGE));
        assertNotNull(schema.getEdge(TestGroups.EDGE_2));

        assertEquals(2, schema.getEntities().size());
        assertNotNull(schema.getEntity(TestGroups.ENTITY));
        assertNotNull(schema.getEntity(TestGroups.ENTITY_2));

        assertEquals("position1", schema.getPosition(IdentifierType.VERTEX.name()));
        assertEquals("position2", schema.getPosition(IdentifierType.SOURCE.name()));
        assertEquals(String.class, schema.getType(TestTypes.PROP_STRING).getClazz());
        assertSame(vertexSerialiser, schema.getVertexSerialiser());
    }

    @Test
    public void shouldMergeDifferentSchemas() {
        // Given
        final String type1 = "type1";
        final String type2 = "type2";
        final Serialisation vertexSerialiser = mock(Serialisation.class);
        final Schema schema1 = new Schema.Builder()
                .edge(TestGroups.EDGE)
                .entity(TestGroups.ENTITY)
                .vertexSerialiser(vertexSerialiser)
                .position(IdentifierType.VERTEX.name(), "position1")
                .type(type1, Integer.class)
                .build();

        final Schema schema2 = new Schema.Builder()
                .edge(TestGroups.EDGE)
                .entity(TestGroups.ENTITY_2)
                .edge(TestGroups.EDGE_2)
                .position(IdentifierType.SOURCE.name(), "position2")
                .type(type2, String.class)
                .build();

        // When
        schema1.merge(schema2);

        // Then
        assertEquals(2, schema1.getEdges().size());
        assertNotNull(schema1.getEdge(TestGroups.EDGE));
        assertNotNull(schema1.getEdge(TestGroups.EDGE_2));

        assertEquals(2, schema1.getEntities().size());
        assertNotNull(schema1.getEntity(TestGroups.ENTITY));
        assertNotNull(schema1.getEntity(TestGroups.ENTITY_2));

        assertEquals("position1", schema1.getPosition(IdentifierType.VERTEX.name()));
        assertEquals("position2", schema1.getPosition(IdentifierType.SOURCE.name()));
        assertEquals(Integer.class, schema1.getType(type1).getClazz());
        assertEquals(String.class, schema1.getType(type2).getClazz());
        assertSame(vertexSerialiser, schema1.getVertexSerialiser());
    }

    @Test
    public void shouldMergeDifferentSchemasOppositeWayAround() {
        // Given
        final String type1 = "type1";
        final String type2 = "type2";
        final Serialisation vertexSerialiser = mock(Serialisation.class);
        final Schema schema1 = new Schema.Builder()
                .edge(TestGroups.EDGE)
                .entity(TestGroups.ENTITY)
                .vertexSerialiser(vertexSerialiser)
                .position(IdentifierType.VERTEX.name(), "position1")
                .type(type1, Integer.class)
                .build();

        final Schema schema2 = new Schema.Builder()
                .edge(TestGroups.EDGE)
                .entity(TestGroups.ENTITY_2)
                .edge(TestGroups.EDGE_2)
                .position(IdentifierType.SOURCE.name(), "position2")
                .type(type2, String.class)
                .build();

        // When
        schema2.merge(schema1);

        // Then
        assertEquals(2, schema2.getEdges().size());
        assertNotNull(schema2.getEdge(TestGroups.EDGE));
        assertNotNull(schema2.getEdge(TestGroups.EDGE_2));

        assertEquals(2, schema2.getEntities().size());
        assertNotNull(schema2.getEntity(TestGroups.ENTITY));
        assertNotNull(schema2.getEntity(TestGroups.ENTITY_2));

        assertEquals("position1", schema2.getPosition(IdentifierType.VERTEX.name()));
        assertEquals("position2", schema2.getPosition(IdentifierType.SOURCE.name()));
        assertEquals(Integer.class, schema2.getType(type1).getClazz());
        assertEquals(String.class, schema2.getType(type2).getClazz());
        assertSame(vertexSerialiser, schema2.getVertexSerialiser());
    }


    @Test
    public void shouldBeAbleToMergeSchemaWithItselfAndNotDuplicateObjects() {
        // Given
        final String position1 = "position1";
        final String position2 = "position2";
        final Serialisation vertexSerialiser = mock(Serialisation.class);
        final Schema schema = new Schema.Builder()
                .edge(TestGroups.EDGE)
                .entity(TestGroups.ENTITY)
                .entity(TestGroups.ENTITY_2)
                .edge(TestGroups.EDGE_2)
                .vertexSerialiser(vertexSerialiser)
                .position(IdentifierType.VERTEX.name(), position1)
                .position(IdentifierType.SOURCE.name(), position2)
                .type(TestTypes.PROP_STRING, String.class)
                .build();

        // When
        schema.merge(schema);

        // Then
        assertEquals(2, schema.getEdges().size());
        assertNotNull(schema.getEdge(TestGroups.EDGE));
        assertNotNull(schema.getEdge(TestGroups.EDGE_2));

        assertEquals(2, schema.getEntities().size());
        assertNotNull(schema.getEntity(TestGroups.ENTITY));
        assertNotNull(schema.getEntity(TestGroups.ENTITY_2));

        assertEquals(position1, schema.getPosition(IdentifierType.VERTEX.name()));
        assertEquals(position2, schema.getPosition(IdentifierType.SOURCE.name()));
        assertEquals(String.class, schema.getType(TestTypes.PROP_STRING).getClazz());
        assertSame(vertexSerialiser, schema.getVertexSerialiser());
    }

    @Test
    public void shouldThrowExceptionWhenMergeSchemasWithConflictingVertexSerialiser() {
        // Given
        final Serialisation vertexSerialiser1 = mock(Serialisation.class);
        final Serialisation vertexSerialiser2 = mock(SerialisationImpl.class);
        final Schema schema1 = new Schema.Builder()
                .vertexSerialiser(vertexSerialiser1)
                .build();
        final Schema schema2 = new Schema.Builder()
                .vertexSerialiser(vertexSerialiser2)
                .build();

        // When / Then
        try {
            schema1.merge(schema2);
            fail("Exception expected");
        } catch (final SchemaException e) {
            assertTrue(e.getMessage().contains("vertex serialiser"));
        }
    }

    @Test
    public void shouldThrowExceptionWhenMergeSchemasWithConflictingPosition() {
        // Given
        final String position1 = "position1";
        final String position2 = "position2";
        final Schema schema1 = new Schema.Builder()
                .position(IdentifierType.SOURCE.name(), position1)
                .build();
        final Schema schema2 = new Schema.Builder()
                .position(IdentifierType.SOURCE.name(), position2)
                .build();

        // When / Then
        try {
            schema1.merge(schema2);
            fail("Exception expected");
        } catch (final SchemaException e) {
            assertTrue(e.getMessage().contains("position"));
        }
    }

    private class SerialisationImpl implements Serialisation {
        private static final long serialVersionUID = 5055359689222968046L;

        @Override
        public boolean canHandle(final Class clazz) {
            return false;
        }

        @Override
        public byte[] serialise(final Object object) throws SerialisationException {
            return new byte[0];
        }

        @Override
        public Object deserialise(final byte[] bytes) throws SerialisationException {
            return null;
        }
    }
}