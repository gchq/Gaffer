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

package uk.gov.gchq.gaffer.graph.library;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.graph.exception.OverwritingException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;
import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FileGraphLibraryTest {

    FileGraphLibrary fileGraphLibrary;
    private static final String TEST_FILE_PATH = "src/test/resources/graphLibrary";
    private static final String GRAPH_ID = "fileGraphLibraryTestGraphId";
    private static final String SCHEMA_ID = "fileGraphLibraryTestSchemaId";
    private static final String PROPERTIES_ID = "fileGraphLibraryTestPropertiesId";
    private final StoreProperties storeProperties = new StoreProperties(PROPERTIES_ID);
    private final Schema schema = new Schema.Builder().id(SCHEMA_ID).build();

    @Before
    public void setUp() {
        deleteTestFiles();
    }

    @After
    public void tearDown() {
        deleteTestFiles();
    }

    @Test
    public void shouldThrowExceptionWithInvalidPath() {
        try {
            fileGraphLibrary = new FileGraphLibrary("inv@lidP@th");
            fail("Exception expected");
        } catch (final IllegalArgumentException e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void shouldGetIdsInFileGraphLibrary() {
        fileGraphLibrary = new FileGraphLibrary(TEST_FILE_PATH);

        fileGraphLibrary.add(GRAPH_ID, schema, storeProperties);

        assertEquals(new Pair<>(SCHEMA_ID, PROPERTIES_ID), fileGraphLibrary.getIds(GRAPH_ID));
    }

    @Test
    public void shouldThrowExceptionWhenGraphIdWithDifferentSchemaExists() {
        fileGraphLibrary = new FileGraphLibrary(TEST_FILE_PATH);
        final StoreProperties storeProperties = new StoreProperties();
        final Schema schema1 = new Schema.Builder()
                .id(SCHEMA_ID + "1")
                .build();

        try {
            fileGraphLibrary.add(GRAPH_ID, schema, storeProperties);
            fileGraphLibrary.add(GRAPH_ID, schema1, storeProperties);
            fail("Exception expected");
        } catch (OverwritingException e) {
            assertNotNull(e.getMessage());
            assertTrue(e.getMessage().contains("already exists with a different schema"));
        }
    }

    @Test
    public void shouldThrowExceptionWhenGraphIdWithDifferentPropertiesExists() {
        fileGraphLibrary = new FileGraphLibrary(TEST_FILE_PATH);
        final StoreProperties storeProperties1 = new StoreProperties();
        storeProperties1.setId(PROPERTIES_ID + "1");

        try {
            fileGraphLibrary.add(GRAPH_ID, schema, storeProperties);
            fileGraphLibrary.add(GRAPH_ID, schema, storeProperties1);
            fail("Exception expected");
        } catch (OverwritingException e) {
            assertNotNull(e.getMessage());
            assertTrue(e.getMessage().contains("already exists with a different store properties"));
        }
    }

    private static void deleteTestFiles() {
        final File dir = new File(TEST_FILE_PATH);
        if (dir.exists()) {
            final File[] children = dir.listFiles();
            for (int i = 0; i < children.length; i++) {
                children[i].delete();
            }
            dir.delete();
        }
    }
}
