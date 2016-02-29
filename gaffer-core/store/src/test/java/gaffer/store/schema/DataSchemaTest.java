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
import static org.mockito.Mockito.mock;

import gaffer.commonutil.PathUtil;
import gaffer.commonutil.TestGroups;
import gaffer.commonutil.TestPropertyNames;
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
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class DataSchemaTest {
    private DataSchema dataSchema;

    @Before
    public void setup() throws IOException {
        dataSchema = DataSchema.fromJson(PathUtil.dataSchema(getClass()));

        final DataSchema dataSchemaTypes = DataSchema.fromJson(PathUtil.schemaTypes(getClass()));
        dataSchema.merge(dataSchemaTypes);
    }

    @Test
    public void shouldDeserialiseAndReserialiseIntoTheSameJson() throws SerialisationException {
        //Given
        final byte[] json1 = dataSchema.toJson(false);
        final DataSchema dataSchema2 = DataSchema.fromJson(DataSchema.class, json1);

        // When
        final byte[] json2 = dataSchema2.toJson(false);

        // Then
        assertEquals(new String(json1), new String(json2));
    }

    @Test
    public void shouldDeserialiseAndReserialiseIntoTheSamePrettyJson() throws SerialisationException {
        //Given
        final byte[] json1 = dataSchema.toJson(true);
        final DataSchema dataSchema2 = DataSchema.fromJson(json1);

        // When
        final byte[] json2 = dataSchema2.toJson(true);

        // Then
        assertEquals(new String(json1), new String(json2));
    }

    @Test
    public void testLoadingSchemaFromJson() {
        // Edge definitions
        DataElementDefinition edgeDefinition = dataSchema.getEdge(TestGroups.EDGE);
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
        DataElementDefinition entityDefinition = dataSchema.getEntity(TestGroups.ENTITY);
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
        dataSchema = createSchema();
    }

    private DataSchema createSchema() {
        return new DataSchema.Builder()
                .edge(TestGroups.EDGE, new DataEdgeDefinition.Builder()
                        .property(TestPropertyNames.PROP_1, "property.string", String.class)
                        .property(TestPropertyNames.PROP_2, "property.integer", Integer.class)
                        .validator(new ElementFilter.Builder()
                                .select(TestPropertyNames.PROP_1)
                                .execute(new ExampleFilterFunction())
                                .build())
                        .build())
                .type("property.string", String.class)
                .type("property.integer", Integer.class)
                .build();
    }

    @Test
    public void writeProgramaticSchemaAsJson() throws IOException, SchemaException {
        dataSchema = createSchema();
        assertEquals("{\n" +
                "  \"edges\" : {\n" +
                "    \"BasicEdge\" : {\n" +
                "      \"properties\" : {\n" +
                "        \"property1\" : \"property.string\",\n" +
                "        \"property2\" : \"property.integer\"\n" +
                "      },\n" +
                "      \"validator\" : {\n" +
                "        \"functions\" : [ {\n" +
                "          \"function\" : {\n" +
                "            \"class\" : \"gaffer.function.ExampleFilterFunction\"\n" +
                "          },\n" +
                "          \"selection\" : [ {\n" +
                "            \"key\" : \"property1\"\n" +
                "          } ]\n" +
                "        } ]\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "  \"types\" : {\n" +
                "    \"property.string\" : {\n" +
                "      \"class\" : \"java.lang.String\"\n" +
                "    },\n" +
                "    \"property.integer\" : {\n" +
                "      \"class\" : \"java.lang.Integer\"\n" +
                "    }\n" +
                "  }\n" +
                "}", new String(dataSchema.toJson(true)));
    }

    @Test
    public void testCorrectSerialiserRetrievableFromConfig() throws NotSerializableException {
        DataSchema store = new DataSchema.Builder()
                .type("property.string", new TypeDefinition.Builder()
                        .clazz(String.class)
                        .serialiser(new JavaSerialiser())
                        .build())
                .edge(TestGroups.EDGE, new DataEdgeDefinition.Builder()
                        .property(TestPropertyNames.PROP_1, "property.string")
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
        final DataEntityDefinition entityDef = new DataEntityDefinition.Builder()
                .property(TestPropertyNames.PROP_1, "property.1.string")
                .build();

        final DataEdgeDefinition edgeDef = new DataEdgeDefinition.Builder()
                .property(TestPropertyNames.PROP_2, "property.2.string")
                .build();

        final DataSchema dataSchema = new DataSchema.Builder()
                .type("property.1.string", String.class)
                .type("property.2.string", Integer.class)
                .entity(TestGroups.ENTITY, entityDef)
                .edge(TestGroups.EDGE, edgeDef)
                .build();

        assertSame(entityDef, dataSchema.getEntity(TestGroups.ENTITY));
        assertSame(edgeDef, dataSchema.getEdge(TestGroups.EDGE));
    }

    @Test
    public void testAbleToLoadProgramaticallyCreatedSchema() throws IOException {
        dataSchema = createSchema();
        Path path = PathUtil.path(getClass(), "/testFile");
        ByteChannel channel = Files.newByteChannel(path, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
        channel.write(ByteBuffer.wrap(dataSchema.toJson(true)));

        dataSchema = DataSchema.fromJson(path);
    }

    @Test
    public void testDataSchemaConstructedFromInputStream() throws IOException {
        final InputStream resourceAsStream = this.getClass().getResourceAsStream(PathUtil.DATA_SCHEMA);
        assertNotNull(resourceAsStream);
        final DataSchema deserialisedDataSchema = DataSchema.fromJson(resourceAsStream);
        assertNotNull(deserialisedDataSchema);

        final Map<String, DataEdgeDefinition> edges = deserialisedDataSchema.getEdges();

        assertEquals(1, edges.size());
        final DataElementDefinition edgeGroup = edges.get(TestGroups.EDGE);
        assertEquals(2, edgeGroup.getProperties().size());

        final Map<String, DataEntityDefinition> entities = deserialisedDataSchema.getEntities();

        assertEquals(1, entities.size());
        final DataElementDefinition entityGroup = entities.get(TestGroups.ENTITY);
        assertEquals(1, entityGroup.getProperties().size());
    }

    @Test
    public void shouldBuildDataSchema() {
        // Given
        final Serialisation vertexSerialiser = mock(Serialisation.class);

        // When
        final DataSchema schema = new DataSchema.Builder()
                .edge(TestGroups.EDGE)
                .entity(TestGroups.ENTITY)
                .entity(TestGroups.ENTITY_2)
                .edge(TestGroups.EDGE_2)
                .vertexSerialiser(vertexSerialiser)
                .position(IdentifierType.VERTEX.name(), "position1")
                .position(IdentifierType.SOURCE.name(), "position2")
                .build();

        // Then
        assertEquals(2, schema.getEdges().size());
        assertNotNull(schema.getEdge(TestGroups.EDGE));
        assertNotNull(schema.getEdge(TestGroups.EDGE_2));

        assertEquals(2, schema.getEntities().size());
        assertNotNull(schema.getEntity(TestGroups.ENTITY));
        assertNotNull(schema.getEntity(TestGroups.ENTITY_2));
    }
}