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

import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.exception.OverwritingException;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class AbstractGraphLibraryTest {

    protected GraphLibrary graphLibrary;

    private static final String TEST_GRAPH_ID = "testGraphId";
    private static final String TEST_GRAPH_ID_1 = "testGraphId1";
    private static final String TEST_SCHEMA_ID = "testSchemaId";
    private static final String TEST_SCHEMA_ID_1 = "testSchemaId1";
    private static final String TEST_UNKNOWN_SCHEMA_ID = "unknownSchemaId";
    private static final String TEST_PROPERTIES_ID = "testPropertiesId";
    private static final String TEST_PROPERTIES_ID_1 = "testPropertiesId1";
    private static final String TEST_UNKNOWN_PROPERTIES_ID = "unknownPropertiesId";

    private Schema schema = new Schema.Builder().id(TEST_SCHEMA_ID).build();
    private Schema schema1 = new Schema.Builder().id(TEST_SCHEMA_ID_1).build();
    private StoreProperties storeProperties = new StoreProperties(TEST_PROPERTIES_ID);
    private StoreProperties storeProperties1 = new StoreProperties(TEST_PROPERTIES_ID_1);

    public abstract GraphLibrary createGraphLibraryInstance();

    @Before
    public void beforeEach() {
        graphLibrary = createGraphLibraryInstance();
        if (graphLibrary instanceof HashMapGraphLibrary) {
            HashMapGraphLibrary.clear();
        }
    }

    @Test
    public void shouldAddAndGetMultipleIdsInGraphLibrary() {
        // When
        graphLibrary.add(TEST_GRAPH_ID, schema, storeProperties);
        graphLibrary.add(TEST_GRAPH_ID_1, schema1, storeProperties1);

        assertEquals(new Pair<>(TEST_SCHEMA_ID, TEST_PROPERTIES_ID), graphLibrary.getIds(TEST_GRAPH_ID));
        assertEquals(new Pair<>(TEST_SCHEMA_ID_1, TEST_PROPERTIES_ID_1), graphLibrary.getIds(TEST_GRAPH_ID_1));
    }

    @Test
    public void shouldAddAndGetIdsInGraphLibrary() {
        // When
        graphLibrary.add(TEST_GRAPH_ID, schema, storeProperties);

        // Then
        assertEquals(new Pair<>(TEST_SCHEMA_ID, TEST_PROPERTIES_ID), graphLibrary.getIds(TEST_GRAPH_ID));
    }

    @Test
    public void shouldThrowExceptionWithInvalidGraphId() {
        // When / Then
        try {
            graphLibrary.add(TEST_GRAPH_ID + "@#", schema, storeProperties);
            fail("Exception expected");
        } catch (final IllegalArgumentException e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void shouldAddAndGetSchema() {
        // When
        graphLibrary.addSchema(schema);

        // Then
        JsonAssert.assertEquals(schema.toJson(false), graphLibrary.getSchema(schema.getId()).toJson(false));
    }

    @Test
    public void shouldNotAddNullSchema() {
        // When / Then
        try {
            graphLibrary.addSchema(null);
        } catch (final IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Id is invalid: null"));
        }
    }

    @Test
    public void shouldThrowExceptionWhenGraphIdWithDifferentSchemaExists() {

        // Given
        graphLibrary.add(TEST_GRAPH_ID, schema, storeProperties);

        // When / Then
        try {
            graphLibrary.add(TEST_GRAPH_ID, schema1, storeProperties);
            fail("Exception expected");
        } catch (final OverwritingException e) {
            assertTrue(e.getMessage().contains("already exists with a different schema"));
        }
    }

    @Test
    public void shouldAddAndGetProperties() {
        // When
        graphLibrary.addProperties(storeProperties);

        // Then
        assertEquals(storeProperties, graphLibrary.getProperties(storeProperties.getId()));
    }

    @Test
    public void shouldNotAddNullProperties() {
        // When / Then
        try {
            graphLibrary.addProperties(null);
        } catch (final IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Id is invalid: null"));
        }
    }

    @Test
    public void shouldThrowExceptionWhenGraphIdWithDifferentPropertiesExists() {
        // Given
        graphLibrary.add(TEST_GRAPH_ID, schema, storeProperties);

        // When / Then
        try {
            graphLibrary.add(TEST_GRAPH_ID, schema, storeProperties1);
            fail("Exception expected");
        } catch (final Exception e) {
            assertTrue(e.getMessage().contains("already exists with a different store properties"));
        }
    }

    @Test
    public void shouldNotThrowExceptionWhenGraphIdWithSameSchemaExists() {
        // Given
        graphLibrary.add(TEST_GRAPH_ID, schema1, storeProperties);

        final Schema schema1Clone = schema1.clone();

        // When
        graphLibrary.checkExisting(TEST_GRAPH_ID, schema1Clone, storeProperties);

        // Then - no exceptions
    }

    @Test
    public void shouldNotThrowExceptionWhenGraphIdWithSamePropertiesExists() {
        // Given
        graphLibrary.add(TEST_GRAPH_ID, schema1, storeProperties);

        final StoreProperties storePropertiesClone = storeProperties.clone();

        // When
        graphLibrary.checkExisting(TEST_GRAPH_ID, schema1, storePropertiesClone);

        // Then - no exceptions
    }

    @Test
    public void shouldUpdateWhenGraphIdExists() {
        // When
        graphLibrary.addOrUpdate(TEST_GRAPH_ID, schema, storeProperties);

        // Then
        assertEquals(graphLibrary.getProperties(storeProperties.getId()), storeProperties);

        // When
        graphLibrary.addOrUpdate(TEST_GRAPH_ID, schema, storeProperties1);

        // Then
        assertEquals(graphLibrary.getProperties(storeProperties1.getId()), storeProperties1);
    }

    @Test
    public void shouldReturnNullWhenPropertyIdIsNotFound() {
        // When
        final StoreProperties unknownStoreProperties = graphLibrary.getProperties(TEST_UNKNOWN_PROPERTIES_ID);

        // Then
        assertNull(unknownStoreProperties);
    }

    @Test
    public void shouldReturnNullWhenSchemaIdIsNotFound() {
        // When
        final Schema unknownSchema = graphLibrary.getSchema(TEST_UNKNOWN_SCHEMA_ID);

        // Then
        assertNull(unknownSchema);
    }

    @Test
    public void shouldThrowExceptionWhenNewStorePropertiesAreAddedWithSamePropertiesIdAndDifferentProperties() {
        // Given
        final StoreProperties tempStoreProperties = storeProperties.clone();
        tempStoreProperties.set("randomKey", "randomValue");

        // When
        graphLibrary.addProperties(storeProperties);

        // Then
        try {
            graphLibrary.addProperties(tempStoreProperties);
            fail("Exception expected");
        } catch (final OverwritingException e) {
            assertTrue(e.getMessage().contains("already exists with a different store properties"));
        }
    }

    @Test
    public void shouldThrowExceptionWhenNewSchemaIsAddedWithSameSchemaIdAndDifferentSchema() {
        // Given
        final Schema tempSchema = new Schema.Builder()
                .id(TEST_SCHEMA_ID)
                .edge(TestGroups.ENTITY, new SchemaEdgeDefinition.Builder()
                        .build())
                .build();

        // When
        graphLibrary.addSchema(schema);

        // Then
        try {
            graphLibrary.addSchema(tempSchema);
            fail("Exception expected");
        } catch (final OverwritingException e) {
            assertTrue(e.getMessage().contains("already exists with a different schema"));
        }
    }

    @Test
    public void shouldIgnoreDuplicateAdditionWhenStorePropertiesAreIdentical() {
        // Given
        final StoreProperties tempStoreProperties = storeProperties.clone();

        // When
        graphLibrary.addProperties(storeProperties);
        graphLibrary.addProperties(tempStoreProperties);

        // Then - no exception
    }

    @Test
    public void shouldIgnoreDuplicateAdditionWhenSchemasAreIdentical() {
        // Given
        final Schema tempSchema = schema.clone();

        // When
        graphLibrary.addSchema(schema);
        graphLibrary.addSchema(tempSchema);

        // Then - no exceptions
    }

}
