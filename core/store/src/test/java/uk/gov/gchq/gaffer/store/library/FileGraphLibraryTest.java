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

package uk.gov.gchq.gaffer.store.library;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.exception.OverwritingException;
import uk.gov.gchq.gaffer.store.schema.Schema;
import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FileGraphLibraryTest {

    private static final String TEST_FILE_PATH = "src/test/resources/graphLibrary";
    private static final String GRAPH_ID = "fileGraphLibraryTestGraphId";
    private static final String SCHEMA_ID = "fileGraphLibraryTestSchemaId";
    private static final String PROPERTIES_ID = "fileGraphLibraryTestPropertiesId";
    private final StoreProperties storeProperties = new StoreProperties(PROPERTIES_ID);
    private final Schema schema = new Schema.Builder().id(SCHEMA_ID).build();

    @Before
    @After
    public void cleanUp() throws IOException {
        if (new File(TEST_FILE_PATH).exists()) {
            FileUtils.forceDelete(new File(TEST_FILE_PATH));
        }
    }

    @Test
    public void shouldThrowExceptionWithInvalidPath() {

        // When / Then
        try {
            new FileGraphLibrary("inv@lidP@th");
            fail("Exception expected");
        } catch (final IllegalArgumentException e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void shouldThrowExceptionWithInvalidGraphId() {

        // Given
        final FileGraphLibrary fileGraphLibrary = new FileGraphLibrary(TEST_FILE_PATH);

        // When / Then
        try {
            fileGraphLibrary.add(GRAPH_ID + "@#", schema, storeProperties);
            fail("Exception expected");
        } catch (IllegalArgumentException e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void shouldThrowExceptionWhenGraphIdWithDifferentSchemaExists() {

        // Given
        final FileGraphLibrary fileGraphLibrary = new FileGraphLibrary(TEST_FILE_PATH);
        final Schema schema1 = new Schema.Builder()
                .id(SCHEMA_ID + "1")
                .build();

        // When / Then
        try {
            fileGraphLibrary.add(GRAPH_ID, schema, storeProperties);
            fileGraphLibrary.add(GRAPH_ID, schema1, storeProperties);
            fail("Exception expected");
        } catch (OverwritingException e) {
            assertTrue(e.getMessage().contains("already exists with a different schema"));
        }
    }

    @Test
    public void shouldThrowExceptionWhenGraphIdWithDifferentPropertiesExists() {

        // Given
        final FileGraphLibrary fileGraphLibrary = new FileGraphLibrary(TEST_FILE_PATH);
        final StoreProperties storeProperties1 = new StoreProperties(PROPERTIES_ID + "1");

        // When / Then
        try {
            fileGraphLibrary.add(GRAPH_ID, schema, storeProperties);
            fileGraphLibrary.add(GRAPH_ID, schema, storeProperties1);
            fail("Exception expected");
        } catch (OverwritingException e) {
            assertNotNull(e.getMessage());
            assertTrue(e.getMessage().contains("already exists with a different store properties"));
        }
    }

    @Test
    public void shouldUpdateWhenGraphIdExists() {

        // Given
        final FileGraphLibrary fileGraphLibrary = new FileGraphLibrary(TEST_FILE_PATH);
        final StoreProperties storeProperties1 = new StoreProperties(PROPERTIES_ID + "1");

        // When
        fileGraphLibrary.addOrUpdate(GRAPH_ID, schema, storeProperties);

        // Then
        assertEquals(fileGraphLibrary.getProperties(PROPERTIES_ID), storeProperties);

        // When
        fileGraphLibrary.addOrUpdate(GRAPH_ID, schema, storeProperties1);

        // Then
        assertEquals(fileGraphLibrary.getProperties(PROPERTIES_ID + "1"), storeProperties1);
    }

    @Test
    public void shouldThrowExceptionWhenStorePropertiesAreNull() {

        // Given
        final FileGraphLibrary fileGraphLibrary = new FileGraphLibrary(TEST_FILE_PATH);

        // When / Then
        try {
            fileGraphLibrary.add(GRAPH_ID, schema, null);
            fail("Exception expected");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("StoreProperties cannot be null"));
        }
    }

    @Test
    public void shouldThrowExceptionWhenSchemaIsNull() {

        // Given
        final FileGraphLibrary fileGraphLibrary = new FileGraphLibrary(TEST_FILE_PATH);

        // When / Then
        try {
            fileGraphLibrary.add(GRAPH_ID, null, storeProperties);
            fail("Exception expected");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Schema cannot be null"));
        }
    }

    @Test
    public void shouldReturnNullWhenPropertyIdIsNotFound() {
        // Given
        final FileGraphLibrary fileGraphLibrary = new FileGraphLibrary(TEST_FILE_PATH);

        // When
        final StoreProperties unknownStoreProperties = fileGraphLibrary.getProperties("unknownPropertyId");

        // Then
        assertNull(unknownStoreProperties);
    }

    @Test
    public void shouldReturnNullWhenSChemaIdIsNotFound() {
        // Given
        final FileGraphLibrary fileGraphLibrary = new FileGraphLibrary(TEST_FILE_PATH);

        // When
        final Schema unknownSchema = fileGraphLibrary.getSchema("unknownSchemaId");

        // Then
        assertNull(unknownSchema);
    }

    @Test
    public void shouldGetIdsWhenAllIdsMatch() {

        // Given
        final FileGraphLibrary fileGraphLibrary = new FileGraphLibrary(TEST_FILE_PATH);
        final StoreProperties storeProperties1 = new StoreProperties(GRAPH_ID);
        final Schema schema1 = new Schema.Builder()
                .id(GRAPH_ID)
                .build();

        // When
        fileGraphLibrary.add(GRAPH_ID, schema1, storeProperties1);

        // Then
        assertEquals(new Pair<>(GRAPH_ID, GRAPH_ID), fileGraphLibrary.getIds(GRAPH_ID));
    }

    @Test
    public void shouldGetIds() {

        // Given
        final FileGraphLibrary fileGraphLibrary = new FileGraphLibrary(TEST_FILE_PATH);

        // When
        fileGraphLibrary.add(GRAPH_ID, schema, storeProperties);

        // Then
        assertEquals(new Pair<>(SCHEMA_ID, PROPERTIES_ID), fileGraphLibrary.getIds(GRAPH_ID));
    }

    @Test
    public void shouldGetSchema() {

        // Given
        final FileGraphLibrary fileGraphLibrary = new FileGraphLibrary(TEST_FILE_PATH);

        // When
        fileGraphLibrary.add(GRAPH_ID, schema, storeProperties);

        // Then
        assertEquals(schema, fileGraphLibrary.getSchema(SCHEMA_ID));
    }

    @Test
    public void shouldGetProperties() {

        // Given
        final FileGraphLibrary fileGraphLibrary = new FileGraphLibrary(TEST_FILE_PATH);

        // When
        fileGraphLibrary.add(GRAPH_ID, schema, storeProperties);

        // Then
        assertEquals(storeProperties, fileGraphLibrary.getProperties(PROPERTIES_ID));
    }
}
