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
import uk.gov.gchq.gaffer.operation.export.graph.ExportToOtherAuthorisedGraph;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.library.FileGraphLibrary;
import uk.gov.gchq.gaffer.store.library.GraphLibrary;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class ExportToOtherAuthorisedGraphTest {

    private static final String GRAPH_ID = "graphId";
    private static final String STORE_PROPS_ID = "storePropsId";
    private static final String SCHEMA_ID = "schemaId";
    private static final String TEST_FILE_PATH = "src/test/resources/exportToOtherPredefinedGraphLibrary";
    private static final String ID = "gaffer.store.id";
    private final GraphLibrary graphLibrary = new FileGraphLibrary(TEST_FILE_PATH);
    private final Store store = mock(Store.class);
    private final User user = new User.Builder().opAuths("auth1", "auth2").build();
    private final Context context = new Context(user);
    private Schema schema;
    private StoreProperties storeProperties;
    private Map<String, List<String>> idAuths = new HashMap<>();

    @Before
    @After
    public void cleanUp() throws IOException {
        if (new File(TEST_FILE_PATH).exists()) {
            FileUtils.forceDelete(new File(TEST_FILE_PATH));
        }
    }

    @Test
    public void shouldThrowExceptionWhenExportingToSameGraph() {
        // Given
        given(store.getGraphId()).willReturn(GRAPH_ID);
        final ExportToOtherAuthorisedGraph export = new ExportToOtherAuthorisedGraph.Builder<>()
                .graphId(GRAPH_ID)
                .build();
        final ExportToOtherAuthorisedGraphHandler handler = new ExportToOtherAuthorisedGraphHandler();

        // When / Then
        try {
            handler.createGraph(export, context, store);
            fail("Exception expected");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("same graph"));
        }
    }

    @Test
    public void shouldCreateGraphWithGraphIdInLibraryAndAuths() {
        // Given
        schema = new Schema.Builder().id(SCHEMA_ID).build();
        storeProperties = new StoreProperties(Paths.get("src/test/resources/store.properties"));
        storeProperties.setId(STORE_PROPS_ID);
        graphLibrary.addOrUpdate(GRAPH_ID + 1, schema, storeProperties);
        given(store.getGraphId()).willReturn(GRAPH_ID);
        given(store.getGraphLibrary()).willReturn(graphLibrary);
        List<String> graphID1OpAuths = new ArrayList<>();
        graphID1OpAuths.add("auth1");
        idAuths.put(GRAPH_ID + 1, graphID1OpAuths);
        final ExportToOtherAuthorisedGraph export = new ExportToOtherAuthorisedGraph.Builder<>()
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
}
