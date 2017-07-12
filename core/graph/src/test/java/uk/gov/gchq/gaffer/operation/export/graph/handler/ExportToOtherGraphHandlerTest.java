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

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.library.FileGraphLibrary;
import uk.gov.gchq.gaffer.graph.library.GraphLibrary;
import uk.gov.gchq.gaffer.operation.export.graph.ExportToOtherGraph;
import uk.gov.gchq.gaffer.operation.export.graph.OtherGraphExporter;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class ExportToOtherGraphHandlerTest {

    private static final String GRAPH_ID = "graphId";
    private static final String STORE_PROPS_ID = "storePropsId";
    private static final String SCHEMA_ID = "schemaId";
    private static final String TEST_FILE_PATH = "src/test/resources/graphLibrary";
    private static final String ID = "gaffer.store.id";
    private final GraphLibrary graphLibrary = new FileGraphLibrary(TEST_FILE_PATH);
    private final User user = new User();
    private final String jobId = "jobId";
    private final Context context = new Context(new User(), "jobId");
    private final Store store = mock(Store.class);
    private final Schema schema = new Schema.Builder().id(SCHEMA_ID).build();
    private StoreProperties storeProperties;

    @Before
    @After
    public void cleanUp() throws IOException {
        if (new File(TEST_FILE_PATH).exists()) {
            FileUtils.forceDelete(new File(TEST_FILE_PATH));
        }

        storeProperties = new StoreProperties(Paths.get("src/test/resources/store.properties"));
        storeProperties.setId(STORE_PROPS_ID);

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
        final ExportToOtherGraph export = new ExportToOtherGraph.Builder<>()
                .graphId(GRAPH_ID)
                .graphLibrary(graphLibrary)
                .build();
        final ExportToOtherGraphHandler handler = new ExportToOtherGraphHandler();

        // When / Then
        try {
            handler.createGraph(export, store);
            fail("Exception expected");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("same graph"));
        }
    }

    @Test
    public void shouldCreateNewGraphWithFullGraphLibrary() {
        // Given
        given(store.getGraphId()).willReturn(GRAPH_ID);
        graphLibrary.add(GRAPH_ID + 1, schema, storeProperties);
        final ExportToOtherGraph export = new ExportToOtherGraph.Builder<>()
                .graphId(GRAPH_ID + 1)
                .graphLibrary(graphLibrary)
                .build();
        final ExportToOtherGraphHandler handler = new ExportToOtherGraphHandler();

        // When
        Graph graph = handler.createGraph(export, store);

        // Then
        assertEquals(GRAPH_ID + 1, graph.getGraphId());
        assertEquals(schema, graph.getSchema());
        assertEquals(storeProperties, graph.getStore().getProperties());
    }

    @Test
    public void shouldCreateNewGraphWithNoGraphLibrary() {
        // Given
        given(store.getGraphId()).willReturn(GRAPH_ID);
        final ExportToOtherGraph export = new ExportToOtherGraph.Builder<>()
                .graphId(GRAPH_ID + 1)
                .storeProperties(storeProperties)
                .schema(schema)
                .build();
        final ExportToOtherGraphHandler handler = new ExportToOtherGraphHandler();

        // When
        Graph graph = handler.createGraph(export, store);

        // Then
        assertEquals(GRAPH_ID + 1, graph.getGraphId());
        assertEquals(schema, graph.getSchema());
        assertEquals(storeProperties, graph.getStore().getProperties());
    }

    @Test
    public void shouldCreateNewGraphWithStoresSchemaWhenNoneSupplied() {
        // Given
        Schema schema1 = new Schema.Builder().id(SCHEMA_ID + 1).build();
        given(store.getGraphId()).willReturn(GRAPH_ID);
        given(store.getSchema()).willReturn(schema1);

        final ExportToOtherGraphHandler handler = new ExportToOtherGraphHandler();
        final ExportToOtherGraph export = new ExportToOtherGraph.Builder<>()
                .graphId(GRAPH_ID + 1)
                .graphLibrary(graphLibrary)
                .storeProperties(storeProperties)
                .build();

        // When
        Graph graph = handler.createGraph(export, store);

        // Then
        assertEquals(GRAPH_ID + 1, graph.getGraphId());
        assertEquals(schema1, graph.getSchema());
        assertEquals(storeProperties, graph.getStore().getProperties());
    }

    @Test
    public void shouldCreateNewGraphWithStoresStorePropertiesWhenNoneSupplied() {
        // Given
        given(store.getProperties()).willReturn(storeProperties);
        given(store.getGraphId()).willReturn(GRAPH_ID);

        final ExportToOtherGraphHandler handler = new ExportToOtherGraphHandler();
        final ExportToOtherGraph export = new ExportToOtherGraph.Builder<>()
                .graphId(GRAPH_ID + 1)
                .graphLibrary(graphLibrary)
                .schema(schema)
                .build();

        // When
        Graph graph = handler.createGraph(export, store);

        // Then
        assertEquals(GRAPH_ID + 1, graph.getGraphId());
        assertEquals(schema, graph.getSchema());
        assertEquals(storeProperties, graph.getStore().getProperties());
    }

    @Test
    public void shouldCreateNewGraphWithParentSchemaId() {
        // Given
        given(store.getGraphId()).willReturn(GRAPH_ID);

        Schema schema1 = new Schema.Builder().id(SCHEMA_ID + 1).build();

        graphLibrary.addOrUpdate(GRAPH_ID + 1, schema, storeProperties);
        graphLibrary.addSchema(SCHEMA_ID + 1, schema1);

        final ExportToOtherGraphHandler handler = new ExportToOtherGraphHandler();
        final ExportToOtherGraph export = new ExportToOtherGraph.Builder<>()
                .graphId(GRAPH_ID + 2)
                .graphLibrary(graphLibrary)
                .parentSchemaId(SCHEMA_ID + 1)
                .storeProperties(storeProperties)
                .build();

        // When
        Graph graph = handler.createGraph(export, store);

        // Then
        assertEquals(GRAPH_ID + 2, graph.getGraphId());
        assertEquals(schema1, graph.getSchema());
        assertEquals(storeProperties, graph.getStore().getProperties());
    }

    @Test
    public void shouldCreateNewGraphWithParentStorePropertiesId() {
        // Given
        given(store.getGraphId()).willReturn(GRAPH_ID);

        StoreProperties storeProperties1 = new StoreProperties(Paths.get("src/test/resources/store.properties"));
        storeProperties1.setId(STORE_PROPS_ID + 1);

        graphLibrary.add(GRAPH_ID + 1, schema, storeProperties);
        graphLibrary.addProperties(STORE_PROPS_ID + 1, storeProperties1);

        final ExportToOtherGraphHandler handler = new ExportToOtherGraphHandler();
        final ExportToOtherGraph export = new ExportToOtherGraph.Builder<>()
                .graphId(GRAPH_ID + 2)
                .graphLibrary(graphLibrary)
                .schema(schema)
                .parentStorePropertiesId(STORE_PROPS_ID + 1)
                .build();

        // When
        Graph graph = handler.createGraph(export, store);

        // Then
        assertEquals(GRAPH_ID + 2, graph.getGraphId());
        assertEquals(schema, graph.getSchema());
        assertEquals(storeProperties1, graph.getStore().getProperties());
    }

    @Test
    public void shouldCreateNewGraphWithParentSchemaIdAndExportSchemaToMerge() {
        // Given
        given(store.getGraphId()).willReturn(GRAPH_ID);

        Schema schema1 = new Schema.Builder().id(SCHEMA_ID + 1).build();

        graphLibrary.addOrUpdate(GRAPH_ID + 1, schema, storeProperties);
        graphLibrary.addSchema(SCHEMA_ID + 1, schema1);

        final ExportToOtherGraphHandler handler = new ExportToOtherGraphHandler();
        final ExportToOtherGraph export = new ExportToOtherGraph.Builder<>()
                .graphId(GRAPH_ID + 2)
                .graphLibrary(graphLibrary)
                .parentSchemaId(SCHEMA_ID + 1)
                .schema(schema)
                .storeProperties(storeProperties)
                .build();

        // When
        Graph graph = handler.createGraph(export, store);

        // Then
        assertEquals(GRAPH_ID + 2, graph.getGraphId());
        assertEquals(new Schema.Builder().merge(schema).merge(schema1).id(null).build(), graph.getSchema());
        assertEquals(storeProperties, graph.getStore().getProperties());
    }

    @Test
    public void shouldCreateNewGraphWithParentStorePropertiesIdAndExportStorePropertiesToMerge() {
        // Given
        given(store.getGraphId()).willReturn(GRAPH_ID);

        StoreProperties storeProperties1 = new StoreProperties(Paths.get("src/test/resources/store.properties"));
        storeProperties1.setId(STORE_PROPS_ID + 1);

        graphLibrary.addOrUpdate(GRAPH_ID + 1, schema, storeProperties);
        graphLibrary.addProperties(STORE_PROPS_ID + 1, storeProperties1);

        final ExportToOtherGraphHandler handler = new ExportToOtherGraphHandler();
        final ExportToOtherGraph export = new ExportToOtherGraph.Builder<>()
                .graphId(GRAPH_ID + 2)
                .graphLibrary(graphLibrary)
                .schema(schema)
                .storeProperties(storeProperties)
                .parentStorePropertiesId(STORE_PROPS_ID + 1)
                .build();

        // When
        Graph graph = handler.createGraph(export, store);

        // Then
        assertEquals(GRAPH_ID + 2, graph.getGraphId());
        assertEquals(schema, graph.getSchema());
        storeProperties1.getProperties().remove(ID);
        storeProperties1.getProperties().putAll(storeProperties.getProperties());
        assertEquals(storeProperties, graph.getStore().getProperties());
    }
}
