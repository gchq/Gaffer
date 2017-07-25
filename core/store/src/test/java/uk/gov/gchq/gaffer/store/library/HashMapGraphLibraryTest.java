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

import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.exception.OverwritingException;
import uk.gov.gchq.gaffer.store.schema.Schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class HashMapGraphLibraryTest {
    private static final String GRAPH_ID = "hashMapTestGraphId";
    private static final String SCHEMA_ID = "hashMapTestSchemaId";
    private static final String PROPERTIES_ID = "hashMapTestPropertiesId";
    private static final HashMapGraphLibrary LIBRARY = new HashMapGraphLibrary();

    private StoreProperties storeProperties = new StoreProperties(PROPERTIES_ID);
    private Schema schema = new Schema.Builder().id(SCHEMA_ID).build();

    @Test
    public void shouldGetIdsInHashMapGraphLibrary() {

        // When
        LIBRARY.add(GRAPH_ID, schema, storeProperties);

        // Then
        assertEquals(new Pair<>(SCHEMA_ID, PROPERTIES_ID), LIBRARY.getIds(GRAPH_ID));
    }

    @Test
    public void shouldClearHashMaps() {

        // Given
        LIBRARY.add(GRAPH_ID, schema, storeProperties);

        // When
        LIBRARY.clear();

        // Then
        assertEquals(null, LIBRARY.getIds(GRAPH_ID));
        assertEquals(null, LIBRARY.getSchema(SCHEMA_ID));
        assertEquals(null, LIBRARY.getProperties(PROPERTIES_ID));
    }

    @Test
    public void shouldThrowExceptionWithInvalidGraphId() {

        // When / Then
        try {
            LIBRARY.add(GRAPH_ID + "@#", schema, storeProperties);
            fail("Exception expected");
        } catch (IllegalArgumentException e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void shouldThrowExceptionWhenGraphIdWithDifferentSchemaExists() {

        // Given
        LIBRARY.add(GRAPH_ID, schema, storeProperties);
        Schema schema1 = new Schema.Builder()
                .id("hashMapTestSchemaId1")
                .build();

        // When / Then
        try {
            LIBRARY.add(GRAPH_ID, schema1, storeProperties);
            fail("Exception expected");
        } catch (OverwritingException e) {
            assertTrue(e.getMessage().contains("already exists with a different schema"));
        }
    }

    @Test
    public void shouldThrowExceptionWhenGraphIdWithDifferentPropertiesExists() {

        // Given
        LIBRARY.add(GRAPH_ID, schema, storeProperties);
        StoreProperties storeProperties1 = new StoreProperties("hashMapTestPropertiesId1");

        // When / Then
        try {
            LIBRARY.add(GRAPH_ID, schema, storeProperties1);
            fail("Exception expected");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("already exists with a different store properties"));
        }
    }

    @Test
    public void shouldAddAndGetSchema() {
        // When
        LIBRARY.addSchema("schemaId", schema);

        // Then
        JsonAssert.assertEquals(schema.toJson(false), LIBRARY.getSchema("schemaId").toJson(false));
    }

    @Test
    public void shouldNotAddNullSchema() {
        // When
        LIBRARY.addSchema("nullSchema", null);

        // Then
        assertNull(LIBRARY.getSchema("nullSchema"));
    }

    @Test
    public void shouldAddAndGetProperties() {
        // When
        LIBRARY.addProperties("propsId", storeProperties);

        // Then
        assertEquals(storeProperties, LIBRARY.getProperties("propsId"));
    }

    @Test
    public void shouldNotAddNullProperties() {
        // When
        LIBRARY.addProperties("nullProps", null);

        // Then
        assertNull(LIBRARY.getProperties("nullProps"));
    }
}
