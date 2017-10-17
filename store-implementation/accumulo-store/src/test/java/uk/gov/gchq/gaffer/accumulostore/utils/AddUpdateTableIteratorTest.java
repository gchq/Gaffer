package uk.gov.gchq.gaffer.accumulostore.utils;/*
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

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.library.FileGraphLibrary;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class AddUpdateTableIteratorTest {

    private static final String GRAPH_ID = "graphId";
    private static final String SCHEMA_DIR = "src/test/resources/schema";
    private static final String SCHEMA_2_DIR = "src/test/resources/schema2";
    private static final String STORE_PROPS_PATH = "src/test/resources/store.properties";
    private static final String STORE_PROPS_2_PATH = "src/test/resources/store2.properties";
    private static final String FILE_GRAPH_LIBRARY_TEST_PATH = "target/graphLibrary";

    @Before
    @After
    public void cleanUp() throws IOException {
        if (new File(FILE_GRAPH_LIBRARY_TEST_PATH).exists()) {
            FileUtils.forceDelete(new File(FILE_GRAPH_LIBRARY_TEST_PATH));
        }
    }

    @Test
    public void shouldRunMainWithFileGraphLibrary() throws Exception {
        // Given
        final String[] args = {GRAPH_ID, SCHEMA_DIR, STORE_PROPS_PATH, "update", FILE_GRAPH_LIBRARY_TEST_PATH};

        // When
        AddUpdateTableIterator.main(args);


        // Then
        final Pair<Schema, StoreProperties> pair = new FileGraphLibrary(FILE_GRAPH_LIBRARY_TEST_PATH).get(GRAPH_ID);
        assertNotNull("Graph for " + GRAPH_ID + " was not found", pair);
        assertNotNull("Schema not found", pair.getFirst());
        assertNotNull("Store properties not found", pair.getSecond());
        JsonAssert.assertEquals(Schema.fromJson(Paths.get(SCHEMA_DIR)).toJson(false), pair.getFirst().toJson(false));
        assertEquals(AccumuloProperties.loadStoreProperties(STORE_PROPS_PATH).getProperties(), pair.getSecond().getProperties());
    }

    @Test
    public void shouldOverrideExistingGraphInGraphLibrary() throws Exception {
        // Given
        shouldRunMainWithFileGraphLibrary(); // load version graph version 1 into the library.
        final String[] args = {GRAPH_ID, SCHEMA_2_DIR, STORE_PROPS_2_PATH, "update", FILE_GRAPH_LIBRARY_TEST_PATH};

        // When
        AddUpdateTableIterator.main(args);

        // Then
        final Pair<Schema, StoreProperties> pair = new FileGraphLibrary(FILE_GRAPH_LIBRARY_TEST_PATH).get(GRAPH_ID);
        assertNotNull("Graph for " + GRAPH_ID + " was not found", pair);
        assertNotNull("Schema not found", pair.getFirst());
        assertNotNull("Store properties not found", pair.getSecond());
        JsonAssert.assertEquals(Schema.fromJson(Paths.get(SCHEMA_2_DIR)).toJson(false), pair.getFirst().toJson(false));
        assertEquals(AccumuloProperties.loadStoreProperties(STORE_PROPS_2_PATH).getProperties(), pair.getSecond().getProperties());
    }

    @Test
    public void shouldRunMainWithNoGraphLibrary() throws Exception {
        // Given
        final String[] args = {GRAPH_ID, SCHEMA_DIR, STORE_PROPS_PATH, "update"};

        // When
        AddUpdateTableIterator.main(args);

        // Then - no exceptions
        final Pair<Schema, StoreProperties> pair = new FileGraphLibrary(FILE_GRAPH_LIBRARY_TEST_PATH).get(GRAPH_ID);
        assertNull("Graph should not have been stored", pair);
    }
}
