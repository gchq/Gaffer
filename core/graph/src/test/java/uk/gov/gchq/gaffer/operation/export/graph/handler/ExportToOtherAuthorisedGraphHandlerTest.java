/*
 * Copyright 2017-2020 Crown Copyright
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

import com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.export.graph.AuthorisedGraphForExportDelegate;
import uk.gov.gchq.gaffer.operation.export.graph.ExportToOtherAuthorisedGraph;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.library.FileGraphLibrary;
import uk.gov.gchq.gaffer.store.library.GraphLibrary;
import uk.gov.gchq.gaffer.store.operation.declaration.OperationDeclaration;
import uk.gov.gchq.gaffer.store.operation.declaration.OperationDeclarations;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class ExportToOtherAuthorisedGraphHandlerTest {

    private static final String GRAPH_ID = "graphId";
    private static final String STORE_PROPS_ID = "storePropsId";
    private static final String SCHEMA_ID = "schemaId";
    public static final String SCHEMA_ID_1 = SCHEMA_ID + 1;

    @TempDir
    File testFolder;

    private final Store store = mock(Store.class);
    private final User user = new User.Builder().opAuths("auth1", "auth2").build();
    private GraphLibrary graphLibrary;
    private Schema schema = new Schema.Builder().build();
    private StoreProperties storeProperties;
    private Map<String, List<String>> idAuths = new HashMap<>();

    @BeforeEach
    public void setUp() throws IOException {
        storeProperties = StoreProperties.loadStoreProperties(StreamUtil.storeProps(getClass()));
        given(store.getGraphId()).willReturn(GRAPH_ID);

        final File graphLibraryFolder = new File(testFolder, "graphLibraryTest/");
        graphLibrary = new FileGraphLibrary(graphLibraryFolder.getPath());
    }

    @Test
    public void shouldThrowExceptionWhenExportingToSameGraph() {
        // Given
        given(store.getGraphLibrary()).willReturn(graphLibrary);
        final ExportToOtherAuthorisedGraph export = new ExportToOtherAuthorisedGraph.Builder()
                .graphId(GRAPH_ID)
                .build();

        // When / Then
        final Exception exception = assertThrows(IllegalArgumentException.class, () -> createGraph(export));

        final String expected = "Validation errors: \n" +
                "User is not authorised to export using graphId: graphId\n" +
                "GraphLibrary cannot be found with graphId: graphId\n" +
                "Cannot export to the same Graph: graphId";
        assertEquals(expected, exception.getMessage());
    }

    @Test
    public void shouldThrowExceptionWithNullGraphLibrary() {
        // Given
        final ExportToOtherAuthorisedGraph export = new ExportToOtherAuthorisedGraph.Builder()
                .graphId(GRAPH_ID)
                .build();

        // When / Then
        final Exception exception = assertThrows(IllegalArgumentException.class, () -> createGraph(export));
        final String expected = "Validation errors: \n" +
                "Schema and parentSchemaIds can't both be null\n" +
                "StoreProperties and parentStorePropertiesId can't both be null\n" +
                "Store GraphLibrary is null";
        assertEquals(expected, exception.getMessage());
    }

    @Test
    public void shouldCreateGraphWithGraphIdInLibraryAndAuths() {
        // Given
        graphLibrary.addOrUpdate(GRAPH_ID + 1, schema, storeProperties);
        given(store.getGraphLibrary()).willReturn(graphLibrary);
        List<String> graphIdAuths = new ArrayList<>();
        graphIdAuths.add("auth1");
        idAuths.put(GRAPH_ID + 1, graphIdAuths);
        final ExportToOtherAuthorisedGraph export = new ExportToOtherAuthorisedGraph.Builder()
                .graphId(GRAPH_ID + 1)
                .build();

        // When
        final Graph graph = createGraph(export);

        // Then
        assertEquals(GRAPH_ID + 1, graph.getGraphId());
        assertEquals(schema, graph.getSchema());
        assertEquals(storeProperties, graph.getStoreProperties());
    }

    @Test
    public void shouldCreateGraphWithParentSchemaIdAndStorePropertiesIdAndAuths() {
        // Given
        Schema schema1 = new Schema.Builder().build();
        graphLibrary.addOrUpdate(GRAPH_ID + 1, SCHEMA_ID, schema, STORE_PROPS_ID, storeProperties);
        graphLibrary.addSchema(SCHEMA_ID_1, schema1);
        given(store.getGraphLibrary()).willReturn(graphLibrary);
        List<String> opAuths = Lists.newArrayList("auth1");
        idAuths.put(GRAPH_ID + 2, opAuths);
        idAuths.put(SCHEMA_ID_1, opAuths);
        idAuths.put(STORE_PROPS_ID, opAuths);
        final ExportToOtherAuthorisedGraph export = new ExportToOtherAuthorisedGraph.Builder()
                .graphId(GRAPH_ID + 2)
                .parentSchemaIds(SCHEMA_ID_1)
                .parentStorePropertiesId(STORE_PROPS_ID)
                .build();

        // When
        final Graph graph = createGraph(export);

        // Then
        assertEquals(GRAPH_ID + 2, graph.getGraphId());
        assertEquals(schema1, graph.getSchema());
        assertEquals(storeProperties, graph.getStoreProperties());
    }

    @Test
    public void shouldThrowExceptionWithParentSchemaIdAndStorePropertiesIdAndNoGraphAuths() {
        // Given
        Schema schema1 = new Schema.Builder().build();
        graphLibrary.addOrUpdate(GRAPH_ID + 1, SCHEMA_ID, schema, STORE_PROPS_ID, storeProperties);
        graphLibrary.addSchema(SCHEMA_ID_1, schema1);
        given(store.getGraphLibrary()).willReturn(graphLibrary);
        List<String> opAuths = Lists.newArrayList("auth1");
        idAuths.put(SCHEMA_ID_1, opAuths);
        idAuths.put(STORE_PROPS_ID, opAuths);
        final ExportToOtherAuthorisedGraph export = new ExportToOtherAuthorisedGraph.Builder()
                .graphId(GRAPH_ID + 2)
                .parentSchemaIds(SCHEMA_ID_1)
                .parentStorePropertiesId(STORE_PROPS_ID)
                .build();

        // When / Then
        final Exception exception = assertThrows(IllegalArgumentException.class, () -> createGraph(export));

        final String expected = "Validation errors: \n" +
                "User is not authorised to export using graphId: graphId2";
        assertEquals(expected, exception.getMessage());
    }

    @Test
    public void shouldThrowExceptionWithParentSchemaIdAndStorePropertiesIdAndNoSchemaAuths() {
        // Given
        Schema schema1 = new Schema.Builder().build();
        graphLibrary.addOrUpdate(GRAPH_ID + 1, SCHEMA_ID, schema, STORE_PROPS_ID, storeProperties);
        graphLibrary.addSchema(SCHEMA_ID_1, schema1);
        given(store.getGraphLibrary()).willReturn(graphLibrary);
        List<String> opAuths = Lists.newArrayList("auth1");
        idAuths.put(GRAPH_ID + 2, opAuths);
        idAuths.put(STORE_PROPS_ID, opAuths);
        final ExportToOtherAuthorisedGraph export = new ExportToOtherAuthorisedGraph.Builder()
                .graphId(GRAPH_ID + 2)
                .parentSchemaIds(SCHEMA_ID_1)
                .parentStorePropertiesId(STORE_PROPS_ID)
                .build();

        // When / Then
        final Exception exception = assertThrows(IllegalArgumentException.class, () -> createGraph(export));
        final String expected = "Validation errors: \n" +
                "User is not authorised to export using schemaId: schemaId1";
        assertEquals(expected, exception.getMessage());
    }

    @Test
    public void shouldCreateGraphWithParentSchemaIdAndStorePropertiesIdAndNoStorePropsAuths() {
        // Given
        Schema schema1 = new Schema.Builder().build();
        graphLibrary.addOrUpdate(GRAPH_ID + 1, SCHEMA_ID, schema, STORE_PROPS_ID, storeProperties);
        graphLibrary.addSchema(SCHEMA_ID_1, schema1);
        given(store.getGraphLibrary()).willReturn(graphLibrary);
        List<String> opAuths = Lists.newArrayList("auth1");
        idAuths.put(GRAPH_ID + 2, opAuths);
        idAuths.put(SCHEMA_ID_1, opAuths);
        final ExportToOtherAuthorisedGraph export = new ExportToOtherAuthorisedGraph.Builder()
                .graphId(GRAPH_ID + 2)
                .parentSchemaIds(SCHEMA_ID_1)
                .parentStorePropertiesId(STORE_PROPS_ID)
                .build();

        // When / Then
        final Exception exception = assertThrows(IllegalArgumentException.class, () -> createGraph(export));

        final String expected = "Validation errors: \n" +
                "User is not authorised to export using storePropertiesId: storePropsId";
        assertEquals(expected, exception.getMessage());
    }

    @Test
    public void shouldThrowExceptionWithParentSchemaIdWithNoParentStorePropertiesIdAndAuths() {
        // Given
        Schema schema1 = new Schema.Builder().build();
        graphLibrary.addOrUpdate(GRAPH_ID + 1, SCHEMA_ID, schema, STORE_PROPS_ID, storeProperties);
        graphLibrary.addSchema(SCHEMA_ID_1, schema1);
        given(store.getGraphLibrary()).willReturn(graphLibrary);
        List<String> opAuths = Lists.newArrayList("auth1");
        idAuths.put(GRAPH_ID + 2, opAuths);
        idAuths.put(SCHEMA_ID_1, opAuths);
        final ExportToOtherAuthorisedGraph export = new ExportToOtherAuthorisedGraph.Builder()
                .graphId(GRAPH_ID + 2)
                .parentSchemaIds(SCHEMA_ID_1)
                .build();

        // When / Then
        final Exception exception = assertThrows(IllegalArgumentException.class, () -> createGraph(export));

        final String expected = "Validation errors: \n" +
                "parentStorePropertiesId must be specified with parentSchemaIds";
        assertEquals(expected, exception.getMessage());
    }

    @Test
    public void shouldThrowExceptionWithParentStorePropertiesIdWithNoParentSchemaIdAndAuths() {
        // Given
        Schema schema1 = new Schema.Builder().build();
        graphLibrary.addOrUpdate(GRAPH_ID + 1, schema, storeProperties);
        graphLibrary.addSchema(SCHEMA_ID_1, schema1);
        given(store.getGraphLibrary()).willReturn(graphLibrary);
        List<String> opAuths = Lists.newArrayList("auth1");
        idAuths.put(GRAPH_ID + 2, opAuths);
        idAuths.put(SCHEMA_ID_1, opAuths);
        idAuths.put(STORE_PROPS_ID, opAuths);
        final ExportToOtherAuthorisedGraph export = new ExportToOtherAuthorisedGraph.Builder()
                .graphId(GRAPH_ID + 2)
                .parentStorePropertiesId(STORE_PROPS_ID)
                .build();

        // When / Then
        final Exception exception = assertThrows(IllegalArgumentException.class, () -> createGraph(export));

        final String expected = "Validation errors: \n" +
                "StoreProperties could not be found in the graphLibrary with id: storePropsId\n" +
                "parentSchemaIds must be specified with parentStorePropertiesId";
        assertEquals(expected, exception.getMessage());
    }


    @Test
    public void shouldThrowExceptionWhenGraphIdCannotBeFound() {
        // Given
        given(store.getGraphLibrary()).willReturn(graphLibrary);
        List<String> graphIdAuths = new ArrayList<>();
        graphIdAuths.add("auth1");
        idAuths.put(GRAPH_ID + 1, graphIdAuths);
        final ExportToOtherAuthorisedGraph export = new ExportToOtherAuthorisedGraph.Builder()
                .graphId(GRAPH_ID + 1)
                .build();

        // When / Then
        final Exception exception = assertThrows(IllegalArgumentException.class, () -> createGraph(export));

        final String expected = "Validation errors: \n" +
                "GraphLibrary cannot be found with graphId: graphId1";
        assertEquals(expected, exception.getMessage());
    }

    @Test
    public void shouldGetHandlerFromJson() {
        // Given
        OperationDeclarations opDeclarations = OperationDeclarations.fromPaths("src/test/resources/ExportToOtherAuthorisedGraphOperationDeclarations.json");
        OperationDeclaration opDeclaration = opDeclarations.getOperations().get(0);
        OperationHandler handler = opDeclaration.getHandler();

        // When / Then
        final String expected = "uk.gov.gchq.gaffer.operation.export.graph.handler.ExportToOtherAuthorisedGraphHandler";
        assertEquals(expected, handler.getClass().getName());
    }

    private Graph createGraph(final ExportToOtherAuthorisedGraph export) {
        return new AuthorisedGraphForExportDelegate.Builder()
                .store(store)
                .graphId(export.getGraphId())
                .parentSchemaIds(export.getParentSchemaIds())
                .parentStorePropertiesId(export.getParentStorePropertiesId())
                .idAuths(idAuths)
                .user(user)
                .build();
    }
}
