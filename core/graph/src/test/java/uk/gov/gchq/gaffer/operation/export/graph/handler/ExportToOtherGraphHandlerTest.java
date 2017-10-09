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

package uk.gov.gchq.gaffer.operation.export.graph.handler;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;
import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.integration.store.TestStore;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.export.graph.ExportToOtherGraph;
import uk.gov.gchq.gaffer.operation.export.graph.OtherGraphExporter;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.library.FileGraphLibrary;
import uk.gov.gchq.gaffer.store.library.GraphLibrary;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.user.User;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class ExportToOtherGraphHandlerTest {

    private static final String GRAPH_ID = "graphId";
    private static final String STORE_PROPS_ID = "storePropsId";
    private static final String SCHEMA_ID = "schemaId";
    private static final String ID = "gaffer.store.id";
    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);
    private final Store store = mock(Store.class);
    private final Schema schema = new Schema.Builder().id(SCHEMA_ID).build();
    private GraphLibrary graphLibrary;
    private StoreProperties storeProperties;

    @Before
    public void before() throws IOException {
        storeProperties = StoreProperties.loadStoreProperties(StreamUtil.storeProps(getClass()));
        storeProperties.setId(STORE_PROPS_ID);

        final File graphLibraryFolder = testFolder.newFolder("graphLibrary");
        graphLibrary = new FileGraphLibrary(graphLibraryFolder.getPath());
    }

    @Test
    public void shouldGetExporterClass() {
        final ExportToOtherGraphHandler handler = new ExportToOtherGraphHandler();

        assertEquals(OtherGraphExporter.class, handler.getExporterClass());
    }

    @Test
    public void shouldThrowExceptionWhenExportingToSameGraph() {
        // Given
        given(store.getGraphId()).willReturn(GRAPH_ID);
        graphLibrary.add(GRAPH_ID, schema, storeProperties);
        final ExportToOtherGraph export = new ExportToOtherGraph.Builder()
                .graphId(GRAPH_ID)
                .build();

        // When / Then
        try {
            createGraph(export);
            fail("Exception expected");
        } catch (final IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("same graph"));
        }
    }

    private Graph createGraph(final ExportToOtherGraph export) {
        return GraphDelegate.createGraph(store, export.getGraphId(),
                export.getSchema(), export.getStoreProperties(), export.getParentSchemaIds(),
                export.getParentStorePropertiesId());
    }

    @Test
    public void shouldCreateExporter() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, OperationException {
        // Given
        given(store.getGraphId()).willReturn(GRAPH_ID);
        graphLibrary.add(GRAPH_ID + 1, schema, storeProperties);
        given(store.getGraphLibrary()).willReturn(graphLibrary);
        final Context context = mock(Context.class);
        final User user = new User();
        given(context.getUser()).willReturn(user);
        final ExportToOtherGraph export = new ExportToOtherGraph.Builder()
                .graphId(GRAPH_ID + 1)
                .build();
        final ExportToOtherGraphHandler handler = new ExportToOtherGraphHandler();

        // When
        OtherGraphExporter exporter = handler.createExporter(export, context, store);

        // Then
        assertNotNull(exporter);

        TestStore.mockStore = mock(Store.class);
        final Iterable elements = mock(Iterable.class);
        exporter.add("key", elements);
        final ArgumentCaptor<OperationChain> opChainCaptor = ArgumentCaptor.forClass(OperationChain.class);
        verify(TestStore.mockStore).execute(opChainCaptor.capture(), Mockito.any(Context.class));
        final List<Operation> ops = opChainCaptor.getValue().getOperations();
        assertEquals(1, ops.size());
        assertSame(elements, ((AddElements) ops.get(0)).getInput());

        try {
            exporter.get("key");
            fail("exception expected");
        } catch (final UnsupportedOperationException e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void shouldCreateNewGraphWithStoreGraphLibrary() {
        // Given
        given(store.getGraphId()).willReturn(GRAPH_ID);
        graphLibrary.add(GRAPH_ID + 1, schema, storeProperties);
        given(store.getGraphLibrary()).willReturn(graphLibrary);
        final ExportToOtherGraph export = new ExportToOtherGraph.Builder()
                .graphId(GRAPH_ID + 1)
                .build();

        // When
        Graph graph = createGraph(export);

        // Then
        assertEquals(GRAPH_ID + 1, graph.getGraphId());
        assertEquals(schema, graph.getSchema());
        assertEquals(storeProperties, graph.getStoreProperties());
    }

    @Test
    public void shouldCreateNewGraphWithStoresStoreProperties() {
        // Given
        Schema schema1 = new Schema.Builder().id(SCHEMA_ID + 1).build();
        given(store.getProperties()).willReturn(storeProperties);
        given(store.getGraphId()).willReturn(GRAPH_ID);

        final ExportToOtherGraph export = new ExportToOtherGraph.Builder()
                .graphId(GRAPH_ID + 1)
                .schema(schema1)
                .build();

        // When
        Graph graph = createGraph(export);

        // Then
        assertEquals(GRAPH_ID + 1, graph.getGraphId());
        assertEquals(schema1, graph.getSchema());
        assertEquals(store.getProperties(), graph.getStoreProperties());
    }

    @Test
    public void shouldCreateNewGraphWithStoresSchema() {
        // Given
        final StoreProperties storeProperties1 = StoreProperties.loadStoreProperties(StreamUtil.storeProps(getClass()));
        storeProperties1.setId(STORE_PROPS_ID + 1);

        given(store.getSchema()).willReturn(schema);
        given(store.getGraphId()).willReturn(GRAPH_ID);

        final ExportToOtherGraph export = new ExportToOtherGraph.Builder()
                .graphId(GRAPH_ID + 1)
                .storeProperties(storeProperties1)
                .build();

        // When
        Graph graph = createGraph(export);

        // Then
        assertEquals(GRAPH_ID + 1, graph.getGraphId());
        assertEquals(schema, graph.getSchema());
        assertEquals(storeProperties1, graph.getStoreProperties());
    }

    @Test
    public void shouldCreateNewGraphWithParentSchemaId() {
        // Given
        given(store.getGraphId()).willReturn(GRAPH_ID);

        Schema schema1 = new Schema.Builder().id(SCHEMA_ID + 1).build();

        graphLibrary.addOrUpdate(GRAPH_ID + 1, schema, storeProperties);
        graphLibrary.addSchema(schema1);
        given(store.getGraphLibrary()).willReturn(graphLibrary);

        final ExportToOtherGraph export = new ExportToOtherGraph.Builder()
                .graphId(GRAPH_ID + 2)
                .parentSchemaIds(SCHEMA_ID + 1)
                .storeProperties(storeProperties)
                .build();

        // When
        Graph graph = createGraph(export);

        // Then
        assertEquals(GRAPH_ID + 2, graph.getGraphId());
        assertEquals(schema1, graph.getSchema());
        assertEquals(storeProperties, graph.getStoreProperties());
    }

    @Test
    public void shouldCreateNewGraphWithMergedParentSchemaIdAndProvidedSchema() {
        // Given
        given(store.getGraphId()).willReturn(GRAPH_ID);

        Schema schema1 = new Schema.Builder()
                .id(SCHEMA_ID + 1)
                .entity("entity", new SchemaEntityDefinition.Builder()
                        .vertex("vertex")
                        .build())
                .type("vertex", String.class)
                .build();
        Schema schema2 = new Schema.Builder()
                .id(SCHEMA_ID + 2)
                .edge("edge", new SchemaEdgeDefinition.Builder()
                        .source("vertex")
                        .destination("vertex")
                        .build())
                .type("vertex", String.class)
                .build();

        graphLibrary.addOrUpdate(GRAPH_ID + 1, schema, storeProperties);
        graphLibrary.addSchema(schema1);
        graphLibrary.addSchema(schema2);
        given(store.getGraphLibrary()).willReturn(graphLibrary);

        final ExportToOtherGraph export = new ExportToOtherGraph.Builder()
                .graphId(GRAPH_ID + 2)
                .parentSchemaIds(SCHEMA_ID + 1, SCHEMA_ID + 2)
                .schema(schema)
                .storeProperties(storeProperties)
                .build();

        // When
        Graph graph = createGraph(export);

        // Then
        assertEquals(GRAPH_ID + 2, graph.getGraphId());
        JsonAssert.assertEquals(new Schema.Builder()
                        .id(SCHEMA_ID)
                        .entity("entity", new SchemaEntityDefinition.Builder()
                                .vertex("vertex")
                                .build())
                        .edge("edge", new SchemaEdgeDefinition.Builder()
                                .source("vertex")
                                .destination("vertex")
                                .build())
                        .type("vertex", String.class)
                        .build().toJson(false),
                graph.getSchema().toJson(false));
        assertEquals(storeProperties, graph.getStoreProperties());
    }

    @Test
    public void shouldCreateNewGraphWithParentStorePropertiesId() {
        // Given
        given(store.getGraphId()).willReturn(GRAPH_ID);

        StoreProperties storeProperties1 = StoreProperties.loadStoreProperties(StreamUtil.storeProps(getClass()));
        storeProperties1.setId(STORE_PROPS_ID + 1);

        graphLibrary.addOrUpdate(GRAPH_ID + 1, schema, storeProperties);
        graphLibrary.addProperties(storeProperties1);
        given(store.getGraphLibrary()).willReturn(graphLibrary);

        final ExportToOtherGraph export = new ExportToOtherGraph.Builder()
                .graphId(GRAPH_ID + 2)
                .schema(schema)
                .parentStorePropertiesId(STORE_PROPS_ID + 1)
                .build();

        // When
        Graph graph = createGraph(export);

        // Then
        assertEquals(GRAPH_ID + 2, graph.getGraphId());
        assertEquals(schema, graph.getSchema());
        assertEquals(storeProperties1, graph.getStoreProperties());
    }

    @Test
    public void shouldCreateNewGraphWithMergedParentStorePropertiesIdAndProvidedStoreProperties() {
        // Given
        given(store.getGraphId()).willReturn(GRAPH_ID);

        StoreProperties storeProperties1 = StoreProperties.loadStoreProperties(StreamUtil.storeProps(getClass()));
        storeProperties1.setId(STORE_PROPS_ID + 1);

        graphLibrary.addOrUpdate(GRAPH_ID + 1, schema, storeProperties);
        graphLibrary.addProperties(storeProperties1);
        given(store.getGraphLibrary()).willReturn(graphLibrary);

        final ExportToOtherGraph export = new ExportToOtherGraph.Builder()
                .graphId(GRAPH_ID + 2)
                .schema(schema)
                .parentStorePropertiesId(STORE_PROPS_ID + 1)
                .storeProperties(storeProperties)
                .build();

        // When
        Graph graph = createGraph(export);

        // Then
        assertEquals(GRAPH_ID + 2, graph.getGraphId());
        assertEquals(schema, graph.getSchema());
        storeProperties1.getProperties().remove(ID);
        storeProperties1.getProperties().putAll(storeProperties.getProperties());
        assertEquals(storeProperties1, graph.getStoreProperties());
        assertEquals(storeProperties1.getId(), graph.getStoreProperties().getId());
    }

    @Test
    public void shouldValidateGraphIdMustBeDifferent() {
        // Given
        given(store.getGraphId()).willReturn(GRAPH_ID);
        final ExportToOtherGraph export = new ExportToOtherGraph.Builder()
                .graphId(GRAPH_ID)
                .build();

        // When / Then
        try {
            validate(export);
            fail("Exception expected");
        } catch (final IllegalArgumentException e) {
            assertEquals("Validation errors: \n" +
                    "Cannot export to the same graph: graphId", e.getMessage());
        }
    }

    private void validate(final ExportToOtherGraph export) {
        GraphDelegate.validate(store, export.getGraphId(), export.getSchema(), export.getStoreProperties(),
                export.getParentSchemaIds(), export.getParentStorePropertiesId());
    }


    @Test
    public void shouldValidatePropsIdCannotBeUsedWithoutGraphLibrary() {
        // Given
        given(store.getGraphId()).willReturn(GRAPH_ID);
        given(store.getGraphLibrary()).willReturn(null);
        final ExportToOtherGraph export = new ExportToOtherGraph.Builder()
                .graphId(GRAPH_ID + 1)
                .parentStorePropertiesId(STORE_PROPS_ID + 1)
                .build();

        // When / Then
        try {
            validate(export);
            fail("Exception expected");
        } catch (final IllegalArgumentException e) {
            assertEquals("Validation errors: \n" +
                    "parentStorePropertiesId cannot be used without a GraphLibrary", e.getMessage());
        }
    }

    @Test
    public void shouldValidateSchemaIdCannotBeUsedWithoutGraphLibrary() {
        // Given
        given(store.getGraphLibrary()).willReturn(null);
        final ExportToOtherGraph export = new ExportToOtherGraph.Builder()
                .graphId(GRAPH_ID + 1)
                .parentSchemaIds("schemaId")
                .build();

        // When / Then
        try {
            validate(export);
            fail("Exception expected");
        } catch (final IllegalArgumentException e) {
            assertEquals("Validation errors: \n" +
                    "parentSchemaIds cannot be used without a GraphLibrary", e.getMessage());
        }
    }

    @Test
    public void shouldValidateSchemaIdCannotBeUsedWhenGraphIdAlreadyExists() {
        // Given
        GraphLibrary mockLibrary = mock(GraphLibrary.class);
        given(store.getGraphLibrary()).willReturn(mockLibrary);
        given(mockLibrary.exists(GRAPH_ID + 1)).willReturn(true);
        final ExportToOtherGraph export = new ExportToOtherGraph.Builder()
                .graphId(GRAPH_ID + 1)
                .parentSchemaIds("schemaId")
                .build();

        // When / Then
        try {
            validate(export);
            fail("Exception expected");
        } catch (final IllegalArgumentException e) {
            assertEquals("Validation errors: \n" +
                    "GraphId graphId1 already exists so you cannot use a different schema. Do not set the parentSchemaIds field.", e.getMessage());
        }
    }

    @Test
    public void shouldValidateSchemaCannotBeUsedWhenGraphIdAlreadyExists() {
        // Given
        GraphLibrary mockLibrary = mock(GraphLibrary.class);
        given(store.getGraphLibrary()).willReturn(mockLibrary);
        given(mockLibrary.exists(GRAPH_ID + 1)).willReturn(true);
        final ExportToOtherGraph export = new ExportToOtherGraph.Builder()
                .graphId(GRAPH_ID + 1)
                .schema(new Schema())
                .build();

        // When / Then
        try {
            validate(export);
            fail("Exception expected");
        } catch (final IllegalArgumentException e) {
            assertEquals("Validation errors: \n" +
                    "GraphId graphId1 already exists so you cannot provide a different schema. Do not set the schema field.", e.getMessage());
        }
    }

    @Test
    public void shouldValidatePropsIdCannotBeUsedWhenGraphIdAlreadyExists() {
        // Given
        GraphLibrary mockLibrary = mock(GraphLibrary.class);
        given(store.getGraphLibrary()).willReturn(mockLibrary);
        given(mockLibrary.exists(GRAPH_ID + 1)).willReturn(true);
        final ExportToOtherGraph export = new ExportToOtherGraph.Builder()
                .graphId(GRAPH_ID + 1)
                .parentStorePropertiesId("props1")
                .build();

        // When / Then
        try {
            validate(export);
            fail("Exception expected");
        } catch (final IllegalArgumentException e) {
            assertEquals("Validation errors: \n" +
                    "GraphId graphId1 already exists so you cannot use different store properties. Do not set the parentStorePropertiesId field.", e.getMessage());
        }
    }

    @Test
    public void shouldValidatePropsCannotBeUsedWhenGraphIdAlreadyExists() {
        // Given
        GraphLibrary mockLibrary = mock(GraphLibrary.class);
        given(store.getGraphLibrary()).willReturn(mockLibrary);
        given(mockLibrary.exists(GRAPH_ID + 1)).willReturn(true);
        final ExportToOtherGraph export = new ExportToOtherGraph.Builder()
                .graphId(GRAPH_ID + 1)
                .storeProperties(new StoreProperties())
                .build();

        // When / Then
        try {
            validate(export);
            fail("Exception expected");
        } catch (final IllegalArgumentException e) {
            assertEquals("Validation errors: \n" +
                    "GraphId graphId1 already exists so you cannot provide different store properties. Do not set the storeProperties field.", e.getMessage());
        }
    }

    @Test
    public void shouldValidateSchemaIdCannotBeFound() {
        // Given
        GraphLibrary mockLibrary = mock(GraphLibrary.class);
        given(store.getGraphLibrary()).willReturn(mockLibrary);
        given(mockLibrary.exists(GRAPH_ID + 1)).willReturn(false);
        final ExportToOtherGraph export = new ExportToOtherGraph.Builder()
                .graphId(GRAPH_ID + 1)
                .parentSchemaIds("schemaId")
                .build();

        // When / Then
        try {
            validate(export);
            fail("Exception expected");
        } catch (final IllegalArgumentException e) {
            assertEquals("Validation errors: \n" +
                    "Schema could not be found in the graphLibrary with id: [schemaId]", e.getMessage());
        }
    }

    @Test
    public void shouldValidatePropsIdCannotBeFound() {
        // Given
        GraphLibrary mockLibrary = mock(GraphLibrary.class);
        given(store.getGraphLibrary()).willReturn(mockLibrary);
        given(mockLibrary.exists(GRAPH_ID + 1)).willReturn(false);
        final ExportToOtherGraph export = new ExportToOtherGraph.Builder()
                .graphId(GRAPH_ID + 1)
                .parentStorePropertiesId("propsId")
                .build();

        // When / Then
        try {
            validate(export);
            fail("Exception expected");
        } catch (final IllegalArgumentException e) {
            assertEquals("Validation errors: \n" +
                    "Store properties could not be found in the graphLibrary with id: propsId", e.getMessage());
        }
    }

    @Test
    public void shouldPassValidation() {
        // Given
        GraphLibrary mockLibrary = mock(GraphLibrary.class);
        given(store.getGraphLibrary()).willReturn(mockLibrary);
        given(mockLibrary.exists(GRAPH_ID + 1)).willReturn(false);
        final ExportToOtherGraph export = new ExportToOtherGraph.Builder()
                .graphId(GRAPH_ID + 1)
                .build();

        // When
        validate(export);

        // Then - no exceptions
    }
}
