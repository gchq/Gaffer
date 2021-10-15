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

package uk.gov.gchq.gaffer.operation.export.graph.handler;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import uk.gov.gchq.gaffer.cache.impl.HashMapCacheService;
import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.integration.store.TestStore;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.export.graph.ExportToOtherGraph;
import uk.gov.gchq.gaffer.operation.export.graph.GraphForExportDelegate;
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
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static uk.gov.gchq.gaffer.operation.export.graph.handler.GraphDelegate.CANNOT_EXPORT_TO_THE_SAME_GRAPH_S;
import static uk.gov.gchq.gaffer.operation.export.graph.handler.GraphDelegate.GRAPH_ID_S_CANNOT_BE_CREATED_WITHOUT_DEFINED_KNOWN_S;
import static uk.gov.gchq.gaffer.operation.export.graph.handler.GraphDelegate.GRAPH_S_ALREADY_EXISTS_SO_YOU_CANNOT_USE_A_DIFFERENT_S_DO_NOT_SET_THE_S_FIELD;
import static uk.gov.gchq.gaffer.operation.export.graph.handler.GraphDelegate.PARENT_STORE_PROPERTIES_ID;
import static uk.gov.gchq.gaffer.operation.export.graph.handler.GraphDelegate.SCHEMA_COULD_NOT_BE_FOUND_IN_THE_GRAPH_LIBRARY_WITH_ID_S;
import static uk.gov.gchq.gaffer.operation.export.graph.handler.GraphDelegate.SCHEMA_STRING;
import static uk.gov.gchq.gaffer.operation.export.graph.handler.GraphDelegate.STORE_PROPERTIES_COULD_NOT_BE_FOUND_IN_THE_GRAPH_LIBRARY_WITH_ID_S;
import static uk.gov.gchq.gaffer.operation.export.graph.handler.GraphDelegate.STORE_PROPERTIES_STRING;
import static uk.gov.gchq.gaffer.operation.export.graph.handler.GraphDelegate.S_CANNOT_BE_USED_WITHOUT_A_GRAPH_LIBRARY;
import static uk.gov.gchq.gaffer.store.TestTypes.DIRECTED_EITHER;

public class ExportToOtherGraphHandlerTest {

    private static final String GRAPH_ID = "graphId";
    private static final String STORE_PROPS_ID = "storePropsId";
    public static final String STORE_PROPS_ID_1 = STORE_PROPS_ID + 1;
    private static final String SCHEMA_ID = "schemaId";
    private static final String EXCEPTION_EXPECTED = "Exception expected";
    public static final String SCHEMA_ID_2 = SCHEMA_ID + 2;
    public static final String SCHEMA_ID_1 = SCHEMA_ID + 1;
    private final Store store = mock(Store.class);
    private final Schema schema = new Schema.Builder().build();
    private GraphLibrary graphLibrary;
    private StoreProperties storeProperties;

    @TempDir
    Path tempDir;

    @BeforeEach
    public void before() throws IOException {
        storeProperties = StoreProperties.loadStoreProperties(StreamUtil.storeProps(getClass()));
        storeProperties.set(HashMapCacheService.STATIC_CACHE, String.valueOf(false));

        final File graphLibraryFolder = tempDir.resolve("graphLibrary").toFile();
        graphLibrary = new FileGraphLibrary(graphLibraryFolder.getPath());

        given(store.getGraphLibrary()).willReturn(graphLibrary);
        given(store.getGraphId()).willReturn(GRAPH_ID);
    }

    @Test
    public void shouldGetExporterClass() {
        final ExportToOtherGraphHandler handler = new ExportToOtherGraphHandler();

        assertEquals(OtherGraphExporter.class, handler.getExporterClass());
    }

    @Test
    public void shouldThrowExceptionWhenExportingToSameGraph() {
        // Given
        graphLibrary.add(GRAPH_ID, SCHEMA_ID, schema, STORE_PROPS_ID, storeProperties);
        final ExportToOtherGraph export = new ExportToOtherGraph.Builder()
                .graphId(GRAPH_ID)
                .build();

        // When / Then
        assertThatIllegalArgumentException()
                .isThrownBy(() -> createGraph(export))
                .withMessage(getErrorMessage(CANNOT_EXPORT_TO_THE_SAME_GRAPH_S, GRAPH_ID));
    }

    @Test
    public void shouldCreateExporter() throws OperationException {
        // Given
        graphLibrary.add(GRAPH_ID + 1, SCHEMA_ID, schema, STORE_PROPS_ID, storeProperties);
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
        assertThat(ops).hasSize(1);
        assertSame(elements, ((AddElements) ops.get(0)).getInput());

        assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> exporter.get("key")).extracting("message").isNotNull();
    }

    @Test
    public void shouldCreateNewGraphWithStoreGraphLibrary() {
        // Given
        graphLibrary.add(GRAPH_ID + 1, schema, storeProperties);
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
        given(store.getProperties()).willReturn(storeProperties);
        given(store.getGraphLibrary()).willReturn(null);

        Schema schema1 = new Schema.Builder().build();

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
        given(store.getSchema()).willReturn(schema);
        given(store.getGraphLibrary()).willReturn(null);

        final StoreProperties storeProperties1 = StoreProperties.loadStoreProperties(StreamUtil.storeProps(getClass()));

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
        Schema schema1 = new Schema.Builder().build();

        graphLibrary.addOrUpdate(GRAPH_ID + 1, SCHEMA_ID, schema, STORE_PROPS_ID, storeProperties);
        graphLibrary.addSchema(SCHEMA_ID_1, schema1);

        final ExportToOtherGraph export = new ExportToOtherGraph.Builder()
                .graphId(GRAPH_ID + 2)
                .parentSchemaIds(SCHEMA_ID_1)
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
        Schema schema1 = new Schema.Builder()
                .entity("entity", new SchemaEntityDefinition.Builder()
                        .vertex("vertex")
                        .build())
                .type("vertex", String.class)
                .build();
        Schema schema2 = new Schema.Builder()
                .edge("edge", new SchemaEdgeDefinition.Builder()
                        .source("vertex")
                        .destination("vertex")
                        .directed(DIRECTED_EITHER)
                        .build())
                .type("vertex", String.class)
                .type(DIRECTED_EITHER, Boolean.class)
                .build();

        graphLibrary.addOrUpdate(GRAPH_ID + 1, schema, storeProperties);
        graphLibrary.addSchema(SCHEMA_ID_1, schema1);
        graphLibrary.addSchema(SCHEMA_ID_2, schema2);

        final ExportToOtherGraph export = new ExportToOtherGraph.Builder()
                .graphId(GRAPH_ID + 2)
                .parentSchemaIds(SCHEMA_ID_1, SCHEMA_ID_2)
                .schema(schema)
                .storeProperties(storeProperties)
                .build();

        // When
        Graph graph = createGraph(export);

        // Then
        assertEquals(GRAPH_ID + 2, graph.getGraphId());
        JsonAssert.assertEquals(new Schema.Builder()
                        .entity("entity", new SchemaEntityDefinition.Builder()
                                .vertex("vertex")
                                .build())
                        .edge("edge", new SchemaEdgeDefinition.Builder()
                                .source("vertex")
                                .destination("vertex")
                                .directed(DIRECTED_EITHER)
                                .build())
                        .type("vertex", String.class)
                        .type(DIRECTED_EITHER, Boolean.class)
                        .build().toJson(false),
                graph.getSchema().toJson(false));
        assertEquals(storeProperties, graph.getStoreProperties());
    }

    @Test
    public void shouldCreateNewGraphWithParentStorePropertiesId() {
        // Given
        StoreProperties storeProperties1 = StoreProperties.loadStoreProperties(StreamUtil.storeProps(getClass()));

        graphLibrary.addOrUpdate(GRAPH_ID + 1, SCHEMA_ID, schema, STORE_PROPS_ID, storeProperties);
        graphLibrary.addProperties(STORE_PROPS_ID_1, storeProperties1);

        final ExportToOtherGraph export = new ExportToOtherGraph.Builder()
                .graphId(GRAPH_ID + 2)
                .schema(schema)
                .parentStorePropertiesId(STORE_PROPS_ID_1)
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

        StoreProperties storeProperties1 = StoreProperties.loadStoreProperties(StreamUtil.storeProps(getClass()));

        graphLibrary.addOrUpdate(GRAPH_ID + 1, SCHEMA_ID, schema, STORE_PROPS_ID, storeProperties);
        graphLibrary.addProperties(STORE_PROPS_ID_1, storeProperties1);

        final ExportToOtherGraph export = new ExportToOtherGraph.Builder()
                .graphId(GRAPH_ID + 2)
                .schema(schema)
                .parentStorePropertiesId(STORE_PROPS_ID_1)
                .storeProperties(storeProperties)
                .build();

        // When
        Graph graph = createGraph(export);

        // Then
        assertEquals(GRAPH_ID + 2, graph.getGraphId());
        assertEquals(schema, graph.getSchema());
        storeProperties1.merge(storeProperties);
        assertEquals(storeProperties1, graph.getStoreProperties());
    }

    @Test
    public void shouldValidateGraphIdMustBeDifferent() {
        // Given
        final ExportToOtherGraph export = new ExportToOtherGraph.Builder()
                .graphId(GRAPH_ID)
                .schema(schema)
                .storeProperties(storeProperties)
                .build();

        // When / Then
        assertThatIllegalArgumentException()
                .isThrownBy(() -> createGraph(export))
                .withMessage(getErrorMessage(CANNOT_EXPORT_TO_THE_SAME_GRAPH_S, "graphId"));
    }

    @Test
    public void shouldValidateParentPropsIdCannotBeUsedWithoutGraphLibrary() {
        // Given
        given(store.getGraphLibrary()).willReturn(null);
        final ExportToOtherGraph export = new ExportToOtherGraph.Builder()
                .graphId(GRAPH_ID + 1)
                .parentStorePropertiesId(STORE_PROPS_ID_1)
                .build();

        // When / Then
        assertThatIllegalArgumentException()
                .isThrownBy(() -> createGraph(export))
                .withMessage(getErrorMessage(S_CANNOT_BE_USED_WITHOUT_A_GRAPH_LIBRARY, "parentStorePropertiesId"));
    }

    @Test
    public void shouldValidateParentSchemaIdCannotBeUsedWithoutGraphLibrary() {
        // Given
        given(store.getGraphLibrary()).willReturn(null);
        final ExportToOtherGraph export = new ExportToOtherGraph.Builder()
                .graphId(GRAPH_ID + 1)
                .parentSchemaIds(SCHEMA_ID)
                .parentStorePropertiesId(SCHEMA_ID)
                .build();

        // When / Then
        final String expectedMessage = "Validation errors: \n"
                + String.format(S_CANNOT_BE_USED_WITHOUT_A_GRAPH_LIBRARY, "parentSchemaIds")
                + '\n'
                + String.format(S_CANNOT_BE_USED_WITHOUT_A_GRAPH_LIBRARY, PARENT_STORE_PROPERTIES_ID);

        assertThatIllegalArgumentException()
                .isThrownBy(() -> createGraph(export))
                .withMessageContaining(expectedMessage);
    }

    private String getErrorMessage(final String format, final String... s) {
        return "Validation errors: \n" +
                String.format(format, s);
    }

    @Test
    public void shouldValidateParentSchemaIdCannotBeUsedWhenGraphIdAlreadyExists() {
        // Given
        graphLibrary.add(GRAPH_ID + 1, SCHEMA_ID_1, new Schema.Builder().edge("edge", new SchemaEdgeDefinition()).build(), STORE_PROPS_ID, new StoreProperties());
        graphLibrary.addSchema(SCHEMA_ID, new Schema.Builder().build());
        final ExportToOtherGraph export = new ExportToOtherGraph.Builder()
                .graphId(GRAPH_ID + 1)
                .parentSchemaIds(SCHEMA_ID)
                .build();

        // When / Then
        final String expectedMessage = "Validation errors: \n" +
                String.format(GRAPH_S_ALREADY_EXISTS_SO_YOU_CANNOT_USE_A_DIFFERENT_S_DO_NOT_SET_THE_S_FIELD, "graphId1", SCHEMA_STRING, "parentSchemaIds");

        assertThatIllegalArgumentException()
                .isThrownBy(() -> createGraph(export))
                .withMessage(expectedMessage);
    }

    @Test
    public void shouldValidateParentSchemaIdCanBeUsedWhenGraphIdAlreadyExistsAndIsSame() {
        // Given
        graphLibrary.add(GRAPH_ID + 1, SCHEMA_ID, new Schema.Builder().build(), STORE_PROPS_ID, new StoreProperties());
        graphLibrary.addSchema(SCHEMA_ID, new Schema.Builder().build());
        final ExportToOtherGraph export = new ExportToOtherGraph.Builder()
                .graphId(GRAPH_ID + 1)
                .parentSchemaIds(SCHEMA_ID)
                .build();

        // When / Then`
        validate(export);
    }

    @Test
    public void shouldValidateSchemaCannotBeUsedWhenGraphIdAlreadyExists() {
        // Given
        graphLibrary.add(GRAPH_ID + 1, SCHEMA_ID, new Schema.Builder().edge("testEdge", new SchemaEdgeDefinition()).build(), STORE_PROPS_ID, new StoreProperties());
        final ExportToOtherGraph export = new ExportToOtherGraph.Builder()
                .graphId(GRAPH_ID + 1)
                .schema(new Schema())
                .build();

        // When / Then
        final String expectedMessage = "Validation errors: \n" +
                String.format(GRAPH_S_ALREADY_EXISTS_SO_YOU_CANNOT_USE_A_DIFFERENT_S_DO_NOT_SET_THE_S_FIELD, "graphId1", SCHEMA_STRING, SCHEMA_STRING);

        assertThatIllegalArgumentException()
                .isThrownBy(() -> createGraph(export))
                .withMessage(expectedMessage);
    }

    @Test
    public void shouldValidateSchemaUsedWhenGraphIdAlreadyExistsAndIsSame() {
        // Given
        Schema schema1 = new Schema.Builder().edge("testEdge", new SchemaEdgeDefinition()).build();
        graphLibrary.add(GRAPH_ID + 1, SCHEMA_ID + 1, schema1, STORE_PROPS_ID, new StoreProperties());
        final ExportToOtherGraph export = new ExportToOtherGraph.Builder()
                .graphId(GRAPH_ID + 1)
                .schema(schema1)
                .build();

        // When / Then
        validate(export);
    }

    @Test
    public void shouldValidateParentPropsIdCannotBeUsedWhenGraphIdAlreadyExists() {
        // Given
        StoreProperties storeProperties1 = new StoreProperties();
        storeProperties1.set("testKey", "testValue");
        graphLibrary.add(GRAPH_ID + 1, SCHEMA_ID, new Schema.Builder().build(), STORE_PROPS_ID, storeProperties1);
        final ExportToOtherGraph export = new ExportToOtherGraph.Builder()
                .graphId(GRAPH_ID + 1)
                .parentStorePropertiesId(STORE_PROPS_ID + 1)
                .build();

        // When / Then
        final String expectedMessage = "Validation errors: \n" +
                String.format(GRAPH_S_ALREADY_EXISTS_SO_YOU_CANNOT_USE_A_DIFFERENT_S_DO_NOT_SET_THE_S_FIELD, "graphId1", "StoreProperties", "parentStorePropertiesId");

        assertThatIllegalArgumentException()
                .isThrownBy(() -> createGraph(export))
                .withMessage(expectedMessage);
    }


    @Test
    public void shouldValidateParentPropsIdCanBeUsedWhenGraphIdAlreadyExistsAndIsSame() {
        // Given
        graphLibrary.add(GRAPH_ID + 1, SCHEMA_ID, new Schema(), STORE_PROPS_ID_1, new StoreProperties());
        final ExportToOtherGraph export = new ExportToOtherGraph.Builder()
                .graphId(GRAPH_ID + 1)
                .parentStorePropertiesId(STORE_PROPS_ID_1)
                .build();

        // When / Then
        validate(export);
    }

    @Test
    public void shouldValidatePropsCannotBeUsedWhenGraphIdAlreadyExists() {
        // Given
        StoreProperties storeProperties1 = new StoreProperties();
        storeProperties1.set("testKey", "testValue");
        graphLibrary.add(GRAPH_ID + 1, SCHEMA_ID, new Schema.Builder().build(), STORE_PROPS_ID, storeProperties1);
        final ExportToOtherGraph export = new ExportToOtherGraph.Builder()
                .graphId(GRAPH_ID + 1)
                .storeProperties(new StoreProperties())
                .build();

        // When / Then
        final String expectedMessage = "Validation errors: \n" +
                String.format(GRAPH_S_ALREADY_EXISTS_SO_YOU_CANNOT_USE_A_DIFFERENT_S_DO_NOT_SET_THE_S_FIELD, "graphId1", STORE_PROPERTIES_STRING, STORE_PROPERTIES_STRING);

        assertThatIllegalArgumentException()
                .isThrownBy(() -> createGraph(export))
                .withMessage(expectedMessage);
    }

    @Test
    public void shouldValidatePropsCanBeUsedWhenGraphIdAlreadyExistsAndIsSame() {
        // Given
        StoreProperties storeProperties1 = new StoreProperties();
        storeProperties1.set("testKey", "testValue");
        graphLibrary.add(GRAPH_ID + 1, SCHEMA_ID, new Schema.Builder().build(), STORE_PROPS_ID, storeProperties1);
        final ExportToOtherGraph export = new ExportToOtherGraph.Builder()
                .graphId(GRAPH_ID + 1)
                .storeProperties(storeProperties1)
                .build();

        // When / Then
        validate(export);
    }

    @Test
    public void shouldThrowExceptionSchemaIdCannotBeFound() {
        // Given
        final ExportToOtherGraph export = new ExportToOtherGraph.Builder()
                .graphId(GRAPH_ID + 1)
                .parentSchemaIds(SCHEMA_ID)
                .storeProperties(new StoreProperties())
                .build();

        // When / Then
        final String expectedMessage = getErrorMessage(SCHEMA_COULD_NOT_BE_FOUND_IN_THE_GRAPH_LIBRARY_WITH_ID_S, "[schemaId]");

        assertThatIllegalArgumentException()
                .isThrownBy(() -> createGraph(export))
                .withMessage(expectedMessage);
    }

    @Test
    public void shouldThrowExceptionPropsIdCannotBeFound() {
        // Given
        final ExportToOtherGraph export = new ExportToOtherGraph.Builder()
                .graphId(GRAPH_ID + 1)
                .schema(new Schema())
                .parentStorePropertiesId(STORE_PROPS_ID)
                .build();

        // When / Then
        final String expectedMessage = getErrorMessage(STORE_PROPERTIES_COULD_NOT_BE_FOUND_IN_THE_GRAPH_LIBRARY_WITH_ID_S, STORE_PROPS_ID);

        assertThatIllegalArgumentException()
                .isThrownBy(() -> createGraph(export))
                .withMessage(expectedMessage);
    }

    @Test
    public void shouldThrowExceptionPropertiesCannotBeUsedIfNotDefinedOrFound() {
        // Given
        final ExportToOtherGraph export = new ExportToOtherGraph.Builder()
                .graphId(GRAPH_ID + 1)
                .schema(new Schema())
                .build();

        // When / Then
        final String expectedMessage = "Validation errors: \n" +
                String.format(GRAPH_ID_S_CANNOT_BE_CREATED_WITHOUT_DEFINED_KNOWN_S, GRAPH_ID + 1, "StoreProperties");

        assertThatIllegalArgumentException()
                .isThrownBy(() -> createGraph(export))
                .withMessage(expectedMessage);
    }

    @Test
    public void shouldThrowExceptionSchemaCannotBeUsedIfNotDefinedOrFound() {
        // Given
        final ExportToOtherGraph export = new ExportToOtherGraph.Builder()
                .graphId(GRAPH_ID + 1)
                .storeProperties(new StoreProperties())
                .build();

        // When / Then
        final String expectedMessage = "Validation errors: \n" +
                String.format(GRAPH_ID_S_CANNOT_BE_CREATED_WITHOUT_DEFINED_KNOWN_S, GRAPH_ID + 1, SCHEMA_STRING);

        assertThatIllegalArgumentException()
                .isThrownBy(() -> createGraph(export))
                .withMessage(expectedMessage);
    }

    @Test
    public void shouldValidateWithSchemaAndStorePropertiesSpecified() {
        // Given
        final ExportToOtherGraph export = new ExportToOtherGraph.Builder()
                .graphId(GRAPH_ID + 1)
                .schema(new Schema())
                .storeProperties(new StoreProperties())
                .build();

        // When
        validate(export);

        // Then - no exceptions
    }

    private Graph createGraph(final ExportToOtherGraph export) {
        return new GraphForExportDelegate.Builder()
                .store(store)
                .graphId(export.getGraphId())
                .schema(export.getSchema())
                .storeProperties(export.getStoreProperties())
                .parentSchemaIds(export.getParentSchemaIds())
                .parentStorePropertiesId(export.getParentStorePropertiesId())
                .build();
    }

    private void validate(final ExportToOtherGraph export) {
        new GraphForExportDelegate().validateGraph(store, export.getGraphId(), export.getSchema(), export.getStoreProperties(),
                export.getParentSchemaIds(), export.getParentStorePropertiesId());
    }
}
