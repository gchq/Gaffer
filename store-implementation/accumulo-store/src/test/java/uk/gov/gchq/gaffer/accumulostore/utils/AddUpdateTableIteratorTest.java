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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.MiniAccumuloClusterManager;
import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.library.FileGraphLibrary;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class AddUpdateTableIteratorTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(AddUpdateTableIteratorTest.class);
    private static final String GRAPH_ID = "graphId";
    private static final String SCHEMA_DIR = "src/test/resources/schema";
    private static final String SCHEMA_2_DIR = "src/test/resources/schema2";
    private static final String STORE_PROPS_PATH = "src/test/resources/store.properties";
    private static final String STORE_PROPS_2_PATH = "src/test/resources/store2.properties";
    private static final String STORE_PROPS_PATH_UPDATED = "src/test/resources/current_store.properties";
    private static final String STORE_PROPS_2_PATH_UPDATED = "src/test/resources/current_store2.properties";
    private static final String EMPTY_STORE_PROPS_PATH = "src/test/resources/empty-store.properties";
    private static final String FILE_GRAPH_LIBRARY_TEST_PATH = "target/graphLibrary";

    private static MiniAccumuloClusterManager miniAccumuloClusterManager1;
    private static MiniAccumuloClusterManager miniAccumuloClusterManager2;
    private static final AccumuloProperties PROPERTIES_1 = AccumuloProperties.loadStoreProperties(STORE_PROPS_PATH);
    private static final AccumuloProperties PROPERTIES_2 = AccumuloProperties.loadStoreProperties(STORE_PROPS_2_PATH);

    @BeforeAll
    public static void setUpStore(@TempDir Path tempDir) {
        miniAccumuloClusterManager1 = new MiniAccumuloClusterManager(PROPERTIES_1, tempDir.toAbsolutePath().toString());
        miniAccumuloClusterManager2 = new MiniAccumuloClusterManager(PROPERTIES_2, tempDir.toAbsolutePath().toString());
        createUpdatedPropertiesFile(PROPERTIES_1, STORE_PROPS_PATH_UPDATED);
        createUpdatedPropertiesFile(PROPERTIES_2, STORE_PROPS_2_PATH_UPDATED);
    }

    @AfterAll
    public static void tearDownStore() {
        miniAccumuloClusterManager1.close();
        miniAccumuloClusterManager2.close();
    }

    private static void createUpdatedPropertiesFile(AccumuloProperties accumuloProperties, String filename) {
        Properties properties = accumuloProperties.getProperties();
        try {
            OutputStream fos = new FileOutputStream(filename);
            properties.store(fos, "AddUpdateTableIteratorTest - " + filename + " with current zookeeper");
            fos.close();
            FileUtils.forceDeleteOnExit(new File(filename));
        } catch (IOException e) {
            LOGGER.error("Failed to write Properties file: " + filename + ": " + e.getMessage());
        }
    }

    @BeforeEach
    @AfterEach
    public void setUpAndTearDown() throws IOException {
        if (new File(FILE_GRAPH_LIBRARY_TEST_PATH).exists()) {
            FileUtils.forceDelete(new File(FILE_GRAPH_LIBRARY_TEST_PATH));
        }
    }

    @Test
    public void shouldRunMainWithFileGraphLibrary() throws Exception {
        // Given
        final String[] args = {GRAPH_ID, SCHEMA_DIR, STORE_PROPS_PATH_UPDATED, "update", FILE_GRAPH_LIBRARY_TEST_PATH};

        // When
        AddUpdateTableIterator.main(args);

        // Then
        final Pair<Schema, StoreProperties> pair = new FileGraphLibrary(FILE_GRAPH_LIBRARY_TEST_PATH).get(GRAPH_ID);
        assertNotNull(pair, "Graph for " + GRAPH_ID + " was not found");
        assertNotNull(pair.getFirst(), "Schema not found");
        assertNotNull(pair.getSecond(), "Store properties not found");
        JsonAssert.assertEquals(Schema.fromJson(Paths.get(SCHEMA_DIR)).toJson(false), pair.getFirst().toJson(false));
        assertEquals(AccumuloProperties.loadStoreProperties(STORE_PROPS_PATH_UPDATED).getProperties(), pair.getSecond().getProperties());
    }

    @Test
    public void shouldOverrideExistingGraphInGraphLibrary() throws Exception {
        // Given
        shouldRunMainWithFileGraphLibrary(); // load version graph version 1 into the library.
        final String[] args = {GRAPH_ID, SCHEMA_2_DIR, STORE_PROPS_2_PATH_UPDATED, "update", FILE_GRAPH_LIBRARY_TEST_PATH};

        // When
        AddUpdateTableIterator.main(args);

        // Then
        final Pair<Schema, StoreProperties> pair = new FileGraphLibrary(FILE_GRAPH_LIBRARY_TEST_PATH).get(GRAPH_ID);
        assertNotNull(pair, "Graph for " + GRAPH_ID + " was not found");
        assertNotNull(pair.getFirst(), "Schema not found");
        assertNotNull(pair.getSecond(), "Store properties not found");
        JsonAssert.assertEquals(Schema.fromJson(Paths.get(SCHEMA_2_DIR)).toJson(false), pair.getFirst().toJson(false));
        assertEquals(AccumuloProperties.loadStoreProperties(STORE_PROPS_2_PATH_UPDATED).getProperties(), pair.getSecond().getProperties());
    }

    @Test
    public void shouldRunMainWithNoGraphLibrary() throws Exception {
        // Given
        final String[] args = {GRAPH_ID, SCHEMA_DIR, STORE_PROPS_PATH_UPDATED, "update"};

        // When
        AddUpdateTableIterator.main(args);

        // Then - no exceptions
        final Pair<Schema, StoreProperties> pair = new FileGraphLibrary(FILE_GRAPH_LIBRARY_TEST_PATH).get(GRAPH_ID);
        assertNull(pair, "Graph should not have been stored");
    }

    @Test
    public void shouldThrowKeyErrorWhenInvalidModifyKeyGiven() throws Exception {
        // Given
        final String[] args = {GRAPH_ID, SCHEMA_DIR, STORE_PROPS_PATH_UPDATED, "invalid key", FILE_GRAPH_LIBRARY_TEST_PATH};

        // When
        IllegalArgumentException actual = assertThrows(IllegalArgumentException.class,
                () -> AddUpdateTableIterator.main(args));

        // Then
        assertEquals("Supplied add or update key (invalid key) was not valid, it must either be add,remove or update.",
                actual.getMessage());
    }

    @Test
    public void shouldReturnStoreClassNameNotFoundWhenStorePropsIsEmpty() throws Exception {
        // Given
        final String[] args = {GRAPH_ID, SCHEMA_DIR, EMPTY_STORE_PROPS_PATH, "update", FILE_GRAPH_LIBRARY_TEST_PATH};

        // When
        IllegalArgumentException actual = assertThrows(IllegalArgumentException.class,
                () -> AddUpdateTableIterator.main(args));

        // Then
        assertEquals("The Store class name was not found in the store properties for key: gaffer.store.class",
                actual.getMessage());
    }

    @Test
    public void shouldReturnInvalidFilePathErrorWhenPathDoesNotExist() throws Exception {
        // Given
        final String[] args = {GRAPH_ID, SCHEMA_DIR, "invalid/file/path", "update", FILE_GRAPH_LIBRARY_TEST_PATH};

        // When
        RuntimeException actual = assertThrows(RuntimeException.class, () -> AddUpdateTableIterator.main(args));

        // Then
        assertEquals("Failed to load store properties file : invalid/file/path",
                actual.getMessage());
    }
}
