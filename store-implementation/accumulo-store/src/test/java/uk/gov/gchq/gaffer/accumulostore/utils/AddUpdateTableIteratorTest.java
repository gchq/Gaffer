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

package uk.gov.gchq.gaffer.accumulostore.utils;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.library.FileGraphLibrary;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Paths;
import java.security.Permission;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class AddUpdateTableIteratorTest {

    private static final String GRAPH_ID = "graphId";
    private static final String SCHEMA_DIR = "src/test/resources/schema";
    private static final String SCHEMA_2_DIR = "src/test/resources/schema2";
    private static final String STORE_PROPS_PATH = "src/test/resources/store.properties";
    private static final String STORE_PROPS_2_PATH = "src/test/resources/store2.properties";
    private static final String EMPTY_STORE_PROPS_PATH = "src/test/resources/empty-store.properties";
    private static final String FILE_GRAPH_LIBRARY_TEST_PATH = "target/graphLibrary";

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    @After
    public void setUpAndTearDown() throws IOException {
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

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowKeyErrorWhenInvalidModifyKeyGiven() throws Exception {
        // Given
        final String[] args = {GRAPH_ID, SCHEMA_DIR, STORE_PROPS_PATH, "invalid key", FILE_GRAPH_LIBRARY_TEST_PATH};

        // When
        AddUpdateTableIterator.main(args);

        // Then
        final String expected = "Supplied add or update key (invalid key) was not valid, it must either be add,remove or update.";
        thrown.expectMessage(expected);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldReturnStoreClassNameNotFoundWhenStorePropsIsEmpty() throws Exception {
        // Given
        final String[] args = {GRAPH_ID, SCHEMA_DIR, EMPTY_STORE_PROPS_PATH, "update", FILE_GRAPH_LIBRARY_TEST_PATH};

        // When
        AddUpdateTableIterator.main(args);

        // Then
        final String expected = "The Store class name was not found in the store properties for key: gaffer.store.class";
        thrown.expectMessage(expected);
    }

    @Test(expected = RuntimeException.class)
    public void shouldReturnInvalidFilePathErrorWhenPathDoesNotExist() throws Exception {
        // Given
        final String[] args = {GRAPH_ID, SCHEMA_DIR, "invalid/file/path", "update", FILE_GRAPH_LIBRARY_TEST_PATH};

        // When
        AddUpdateTableIterator.main(args);

        // Then
        final String expected = "Failed to load store properties file : invalid/file/path";
        thrown.expectMessage(expected);
    }

    @Test(expected = SecurityException.class)
    public void whenExecutedWithLessThan3ArgsPrintlnWrongNoOfArgsError() throws Exception {
        // Given
        // Override System.exit when not enough args given
        System.setSecurityManager(new OverrideSysExitSecurityManager());
        ByteArrayOutputStream errContent = new ByteArrayOutputStream();
        System.setErr(new PrintStream(errContent));

        final String[] threeArgs = {"1", "2", "3"};

        // When
        AddUpdateTableIterator.main(threeArgs);

        // Then
        final String expected = "Wrong number of arguments. \n" +
                "Usage: <graphId> <comma separated schema paths> " +
                "<store properties path> <add,remove or update> <file graph library path>\n";
        assertEquals(expected, errContent.toString());
    }

    private class OverrideSysExitSecurityManager extends SecurityManager {
        @Override
        public void checkExit(int status) {
            throw new SecurityException();
        }

        @Override
        public void checkPermission(Permission perm) {
            // Allow
        }
    }
}
