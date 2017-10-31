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

import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.export.graph.ExportToOtherAuthorisedGraph;
import uk.gov.gchq.gaffer.store.Context;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class ExportToOtherAuthorisedGraphHandlerTest {

    private static final String GRAPH_ID = "graphId";
    private static final String STORE_PROPS_ID = "storePropsId";
    private static final String SCHEMA_ID = "schemaId";
    public static final String SCHEMA_ID_1 = SCHEMA_ID + 1;
    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);
    private final Store store = mock(Store.class);
    private final User user = new User.Builder().opAuths("auth1", "auth2").build();
    private final Context context = new Context(user);
    private GraphLibrary graphLibrary;
    private Schema schema = new Schema.Builder().build();
    private StoreProperties storeProperties;
    private Map<String, List<String>> idAuths = new HashMap<>();

    @Before
    public void setUp() throws IOException {
        storeProperties = StoreProperties.loadStoreProperties(StreamUtil.storeProps(getClass()));
        given(store.getGraphId()).willReturn(GRAPH_ID);

        final File graphLibraryFolder = testFolder.newFolder("graphLibraryTest");
        graphLibrary = new FileGraphLibrary(graphLibraryFolder.getPath());

    }

    @Test
    public void shouldThrowExceptionWhenExportingToSameGraph() {
        // Given
        given(store.getGraphLibrary()).willReturn(graphLibrary);
        List<String> graphIdAuths = new ArrayList<>();
        graphIdAuths.add("auth1");
        idAuths.put(GRAPH_ID, graphIdAuths);
        final ExportToOtherAuthorisedGraph export = new ExportToOtherAuthorisedGraph.Builder()
                .graphId(GRAPH_ID)
                .build();
        final ExportToOtherAuthorisedGraphHandler handler = new ExportToOtherAuthorisedGraphHandler();
        handler.setIdAuths(idAuths);

        // When / Then
        try {
            handler.createGraph(export, context, store);
            fail("Exception expected");
        } catch (final IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("same Graph"));
        }
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
        final ExportToOtherAuthorisedGraphHandler handler = new ExportToOtherAuthorisedGraphHandler();
        handler.setIdAuths(idAuths);

        // When
        Graph graph = handler.createGraph(export, context, store);

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
        final ExportToOtherAuthorisedGraphHandler handler = new ExportToOtherAuthorisedGraphHandler();
        handler.setIdAuths(idAuths);

        // When
        Graph graph = handler.createGraph(export, context, store);

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
        final ExportToOtherAuthorisedGraphHandler handler = new ExportToOtherAuthorisedGraphHandler();
        handler.setIdAuths(idAuths);

        // When / Then
        try {
            handler.createGraph(export, context, store);
            fail("Exception expected");
        } catch (final IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("User is not authorised to export using graphId"));
        }
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
        final ExportToOtherAuthorisedGraphHandler handler = new ExportToOtherAuthorisedGraphHandler();
        handler.setIdAuths(idAuths);

        // When / Then
        try {
            handler.createGraph(export, context, store);
            fail("Exception expected");
        } catch (final IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("User is not authorised to export using schemaId"));
        }
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
        final ExportToOtherAuthorisedGraphHandler handler = new ExportToOtherAuthorisedGraphHandler();
        handler.setIdAuths(idAuths);

        // When / Then
        try {
            handler.createGraph(export, context, store);
            fail("Exception expected");
        } catch (final IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("User is not authorised to export using storePropertiesId"));
        }
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
        final ExportToOtherAuthorisedGraphHandler handler = new ExportToOtherAuthorisedGraphHandler();
        handler.setIdAuths(idAuths);

        // When / Then
        try {
            handler.createGraph(export, context, store);
            fail("Exception expected");
        } catch (final IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("parentStorePropertiesId must be specified with parentSchemaId"));
        }
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
        final ExportToOtherAuthorisedGraphHandler handler = new ExportToOtherAuthorisedGraphHandler();
        handler.setIdAuths(idAuths);

        // When / Then
        try {
            handler.createGraph(export, context, store);
            fail("Exception expected");
        } catch (final IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("parentSchemaId must be specified with parentStorePropertiesId"));
        }
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
        final ExportToOtherAuthorisedGraphHandler handler = new ExportToOtherAuthorisedGraphHandler();
        handler.setIdAuths(idAuths);

        // When / Then
        try {
            handler.createGraph(export, context, store);
            fail("Exception expected");
        } catch (final IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("GraphLibrary cannot be found with graphId"));
        }
    }

    @Test
    public void shouldGetHandlerFromJson() throws OperationException {
        // Given
        OperationDeclarations opDeclarations = OperationDeclarations.fromPaths("src/test/resources/ExportToOtherAuthorisedGraphOperationDeclarations.json");
        OperationDeclaration opDeclaration = opDeclarations.getOperations().get(0);
        OperationHandler handler = opDeclaration.getHandler();

        // When / Then
        assertTrue(handler.getClass().getName().contains("AuthorisedGraph"));
    }
}
