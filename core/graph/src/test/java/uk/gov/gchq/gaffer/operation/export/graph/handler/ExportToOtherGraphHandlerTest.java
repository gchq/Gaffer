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
import uk.gov.gchq.gaffer.graph.library.FileGraphLibrary;
import uk.gov.gchq.gaffer.graph.library.GraphLibrary;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.export.graph.ExportToOtherGraph;
import uk.gov.gchq.gaffer.operation.export.graph.OtherGraphExporter;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.mock;

public class ExportToOtherGraphHandlerTest {

    private static final String GRAPH_ID = "graphId";
    private static final String STORE_PROPS_ID = "storePropsId";
    private static final String SCHEMA_ID = "schemaId";
    private static final String TEST_FILE_PATH = "src/test/resources/graphLibrary";
    private final GraphLibrary graphLibrary = new FileGraphLibrary(TEST_FILE_PATH);
    private final Context context = new Context();
    private final Store store = mock(Store.class);

    @Before
    @After
    public void cleanUp() throws IOException {
        if (new File(TEST_FILE_PATH).exists()) {
            FileUtils.forceDelete(new File(TEST_FILE_PATH));
        }
    }

    @Test
    public void testExporter() throws OperationException {
        graphLibrary.add(GRAPH_ID, new Schema.Builder().id(SCHEMA_ID).build(), new StoreProperties(STORE_PROPS_ID));
        final List<?> elements = Arrays.asList(1, 2, 3);
        final ExportToOtherGraph export = new ExportToOtherGraph.Builder<>()
                .graphId(GRAPH_ID)
                .input(elements)
                .graphLibrary(graphLibrary)
                .build();

        final OtherGraphExporter exporter = mock(OtherGraphExporter.class);
        context.addExporter(exporter);

        final ExportToOtherGraphHandler handler = new ExportToOtherGraphHandler();

        handler.setGraphId(GRAPH_ID);
        handler.setGraphLibrary(graphLibrary);

        final Object handlerResult = handler.doOperation(export, context, store);
    }
}
