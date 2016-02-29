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

package gaffer.data.elementdefinition.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import gaffer.commonutil.StreamUtil;
import gaffer.commonutil.TestGroups;
import gaffer.commonutil.TestPropertyNames;
import gaffer.data.element.ElementComponentKey;
import gaffer.data.element.IdentifierType;
import gaffer.data.element.function.ElementAggregator;
import gaffer.data.element.function.ElementFilter;
import gaffer.data.elementdefinition.schema.exception.SchemaException;
import gaffer.exception.SerialisationException;
import gaffer.function.AggregateFunction;
import gaffer.function.ExampleAggregatorFunction;
import gaffer.function.ExampleFilterFunction;
import gaffer.function.FilterFunction;
import gaffer.function.IsA;
import gaffer.function.context.ConsumerFunctionContext;
import gaffer.function.context.PassThroughFunctionContext;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;

public class DataSchemaTest {
    private DataSchema dataSchema;

    @Before
    public void setup() throws IOException {
        dataSchema = DataSchema.fromJson(StreamUtil.dataSchema(getClass()));
    }

    @Test
    public void shouldDeserialiseAndReserialiseIntoTheSameJson() throws SerialisationException {
        //Given
        dataSchema.addTypesFromStream(StreamUtil.schemaTypes(getClass()));
        final byte[] json1 = dataSchema.toJson(false);
        final DataSchema dataSchema2 = DataSchema.fromJson(json1, DataSchema.class);

        // When
        final byte[] json2 = dataSchema2.toJson(false);

        // Then
        assertEquals(new String(json1), new String(json2));
    }

    @Test
    public void shouldDeserialiseAndReserialiseIntoTheSamePrettyJson() throws SerialisationException {
        //Given
        dataSchema.addTypesFromStream(StreamUtil.schemaTypes(getClass()));
        final byte[] json1 = dataSchema.toJson(true);
        final DataSchema dataSchema2 = DataSchema.fromJson(json1);

        // When
        final byte[] json2 = dataSchema2.toJson(true);

        // Then
        assertEquals(new String(json1), new String(json2));
    }

    @Test
    public void testLoadingSchemaFromJson() {
        // Add the types from a separate file
        dataSchema.addTypesFromStream(StreamUtil.schemaTypes(getClass()));

        // Edge definitions
        DataElementDefinition edgeDefinition = dataSchema.getEdge(TestGroups.EDGE);
        assertNotNull(edgeDefinition);

        final Map<String, String> propertyMap = edgeDefinition.getPropertyMap();
        assertEquals(2, propertyMap.size());
        assertEquals("simpleProperty", propertyMap.get(TestPropertyNames.F2));
        assertEquals("simpleDate", propertyMap.get(TestPropertyNames.DATE));

        // Check aggregator
        assertNull(edgeDefinition.getOriginalAggregator());
        ElementAggregator aggregator = edgeDefinition.getAggregator();
        List<PassThroughFunctionContext<ElementComponentKey, AggregateFunction>> aggContexts = aggregator.getFunctions();
        assertEquals(2, aggContexts.size());

        PassThroughFunctionContext<ElementComponentKey, AggregateFunction> aggContext = aggContexts.get(0);
        assertTrue(aggContext.getFunction() instanceof ExampleAggregatorFunction);
        assertEquals(1, aggContext.getSelection().size());
        assertEquals(TestPropertyNames.F2, aggContext.getSelection().get(0).getPropertyName());

        aggContext = aggContexts.get(1);
        assertTrue(aggContext.getFunction() instanceof ExampleAggregatorFunction);
        assertEquals(1, aggContext.getSelection().size());
        assertEquals(TestPropertyNames.DATE, aggContext.getSelection().get(0).getPropertyName());

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
        assertEquals(TestPropertyNames.F2, valContext.getSelection().get(0).getPropertyName());

        valContext = valContexts.get(index++);
        assertTrue(valContext.getFunction() instanceof ExampleFilterFunction);
        assertEquals(1, valContext.getSelection().size());
        assertEquals(TestPropertyNames.F2, valContext.getSelection().get(0).getPropertyName());

        valContext = valContexts.get(index++);
        assertTrue(valContext.getFunction() instanceof IsA);
        assertEquals(1, valContext.getSelection().size());
        assertEquals(TestPropertyNames.DATE, valContext.getSelection().get(0).getPropertyName());

        assertEquals(index, valContexts.size());

        // Entity definitions
        DataElementDefinition entityDefinition = dataSchema.getEntity(TestGroups.ENTITY);
        assertNotNull(entityDefinition);
        assertTrue(entityDefinition.containsProperty(TestPropertyNames.F1));
        aggregator = entityDefinition.getAggregator();
        aggContexts = aggregator.getFunctions();
        assertEquals(1, aggContexts.size());
        assertTrue(aggContexts.get(0).getFunction() instanceof ExampleAggregatorFunction);
        assertEquals(1, aggContexts.get(0).getSelection().size());
        assertEquals(TestPropertyNames.F1, aggContexts.get(0).getSelection().get(0).getPropertyName());
    }

    @Test
    public void createProgramaticSchema() {
        dataSchema = createSchema();
    }

    private DataSchema createSchema() {
        return new DataSchema.Builder()
                .edge(TestGroups.EDGE, new DataEdgeDefinition.Builder()
                        .property(TestPropertyNames.F1, String.class)
                        .property(TestPropertyNames.F2, Integer.class)
                        .validator(new ElementFilter.Builder()
                                .select(TestPropertyNames.F1)
                                .execute(new ExampleFilterFunction())
                                .build())
                        .aggregator(new ElementAggregator.Builder()
                                .select(TestPropertyNames.F1)
                                .execute(new ExampleAggregatorFunction())
                                .build())
                        .build())
                .build();
    }

    @Test
    public void writeProgramaticSchemaAsJson() throws IOException, SchemaException {
        dataSchema = createSchema();
        assertEquals("{\n" +
                "  \"edges\" : {\n" +
                "    \"BasicEdge\" : {\n" +
                "      \"properties\" : {\n" +
                "        \"property1\" : \"java.lang.String\",\n" +
                "        \"property2\" : \"java.lang.Integer\"\n" +
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
                "      },\n" +
                "      \"aggregator\" : {\n" +
                "        \"functions\" : [ {\n" +
                "          \"function\" : {\n" +
                "            \"class\" : \"gaffer.function.ExampleAggregatorFunction\"\n" +
                "          },\n" +
                "          \"selection\" : [ {\n" +
                "            \"key\" : \"property1\"\n" +
                "          } ]\n" +
                "        } ]\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}", new String(dataSchema.toJson(true)));
    }

    @Test
    public void testAbleToLoadProgramaticallyCreatedSchema() throws IOException {
        dataSchema = createSchema();
        Path path = Paths.get(getClass().getResource("/testFile").getPath());
        ByteChannel channel = Files.newByteChannel(path, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
        channel.write(ByteBuffer.wrap(dataSchema.toJson(true)));

        dataSchema = DataSchema.fromJson(path);
    }

    @Test
    public void shouldBuildDataSchema() {
        // Given
        final DataEdgeDefinition edgeDef1 = mock(DataEdgeDefinition.class);
        final DataEdgeDefinition edgeDef2 = mock(DataEdgeDefinition.class);
        final DataEntityDefinition entityDef1 = mock(DataEntityDefinition.class);
        final DataEntityDefinition entityDef2 = mock(DataEntityDefinition.class);

        // When
        final DataSchema schema = new DataSchema.Builder()
                .edge(TestGroups.EDGE, edgeDef1)
                .entity(TestGroups.ENTITY, entityDef1)
                .entity(TestGroups.ENTITY_2, entityDef2)
                .edge(TestGroups.EDGE_2, edgeDef2)
                .build();

        // Then
        assertEquals(2, schema.getEdges().size());
        assertSame(edgeDef1, schema.getEdge(TestGroups.EDGE));
        assertSame(edgeDef2, schema.getEdge(TestGroups.EDGE_2));

        assertEquals(2, schema.getEntities().size());
        assertSame(entityDef1, schema.getEntity(TestGroups.ENTITY));
        assertSame(entityDef2, schema.getEntity(TestGroups.ENTITY_2));
    }
}