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
import uk.gov.gchq.gaffer.commonutil.JsonUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.exception.OverwritingException;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.Schema.Builder;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
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
    private static final String EXCEPTION_EXPECTED = "Exception expected";

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
            fail(EXCEPTION_EXPECTED);
        } catch (final IllegalArgumentException e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void shouldAddAndGetSchema() {
        // When
        graphLibrary.addSchema(TEST_SCHEMA_ID, schema);

        // Then
        JsonAssert.assertEquals(schema.toJson(false), graphLibrary.getSchema(TEST_SCHEMA_ID).toJson(false));
    }

    @Test
    public void shouldNotAddNullSchema() {
        // When / Then
        try {
            graphLibrary.addSchema(null, null);
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
            fail(EXCEPTION_EXPECTED);
        } catch (final OverwritingException e) {
            assertTrue(e.getMessage().contains("already exists with a different schema"));
        }
    }

    @Test
    public void shouldAddAndGetProperties() {
        // When
        graphLibrary.addProperties(TEST_PROPERTIES_ID, storeProperties);

        // Then
        assertEquals(storeProperties, graphLibrary.getProperties(TEST_PROPERTIES_ID));
    }

    @Test
    public void shouldNotAddNullProperties() {
        // When / Then
        try {
            graphLibrary.addProperties(null, null);
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
            fail(EXCEPTION_EXPECTED);
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
        assertEquals(graphLibrary.getProperties(TEST_PROPERTIES_ID), storeProperties);

        // When
        graphLibrary.addOrUpdate(TEST_GRAPH_ID, schema, storeProperties1);

        // Then
        assertEquals(graphLibrary.getProperties(TEST_PROPERTIES_ID_1), storeProperties1);
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
        graphLibrary.addProperties(TEST_PROPERTIES_ID, storeProperties);

        // Then
        try {
            graphLibrary.addProperties(TEST_PROPERTIES_ID, tempStoreProperties);
            fail(EXCEPTION_EXPECTED);
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
        graphLibrary.addSchema(TEST_SCHEMA_ID, schema);

        // Then
        try {
            graphLibrary.addSchema(TEST_SCHEMA_ID, tempSchema);
            fail(EXCEPTION_EXPECTED);
        } catch (final OverwritingException e) {
            assertTrue(e.getMessage().contains("already exists with a different schema"));
        }
    }

    @Test
    public void shouldIgnoreDuplicateAdditionWhenStorePropertiesAreIdentical() {
        // Given
        final StoreProperties tempStoreProperties = storeProperties.clone();

        // When
        graphLibrary.addProperties(TEST_PROPERTIES_ID, storeProperties);
        graphLibrary.addProperties(TEST_PROPERTIES_ID, tempStoreProperties);

        // Then - no exception
    }

    @Test
    public void shouldIgnoreDuplicateAdditionWhenSchemasAreIdentical() {
        // Given
        final Schema tempSchema = schema.clone();

        // When
        graphLibrary.addSchema(TEST_SCHEMA_ID, schema);
        graphLibrary.addSchema(TEST_SCHEMA_ID, tempSchema);

        // Then - no exceptions
    }

    @Test
    public void shouldNotOverwriteSchemaWithClashingName() throws Exception {
        final String clashingId = "clashingId";
        byte[] entitySchema = new Builder().id(clashingId).entity("e1", new SchemaEntityDefinition.Builder().property("p1", "string").build()).type("string", String.class).build().toJson(true);
        byte[] edgeSchema = new Builder().id(clashingId).edge("e1", new SchemaEdgeDefinition.Builder().property("p1", "string").build()).type("string", String.class).build().toJson(true);

        graphLibrary.addSchema(clashingId, Schema.fromJson(entitySchema));

        try {
            graphLibrary.add("graph", Schema.fromJson(edgeSchema), new StoreProperties());
            fail(EXCEPTION_EXPECTED);
        } catch (final OverwritingException e) {
            assertTrue(e.getMessage().contains("schemaId clashingId already exists with a different schema"));
        }

        Schema schemaFromLibrary = graphLibrary.getSchema(clashingId);

        assertTrue(JsonUtil.equals(entitySchema, schemaFromLibrary.toJson(true)));
        assertFalse(JsonUtil.equals(schemaFromLibrary.toJson(true), edgeSchema));
    }

    @Test
    public void shouldNotOverwriteStorePropertiesWithClashingName() throws Exception {
        final String clashingId = "clashingId";
        StoreProperties propsA = new StoreProperties(clashingId);
        propsA.set("a", "a");
        StoreProperties propsB = new StoreProperties(clashingId);
        propsB.set("b", "b");

        graphLibrary.addProperties(clashingId, propsA);

        try {
            graphLibrary.add("graph", new Schema(), propsB);
            fail(EXCEPTION_EXPECTED);
        } catch (final OverwritingException e) {
            assertTrue(e.getMessage().contains("propertiesId clashingId already exists with a different store properties"));
        }

        StoreProperties storePropertiesFromLibrary = graphLibrary.getProperties(clashingId);

        assertEquals(propsA.getProperties(), storePropertiesFromLibrary.getProperties());
        assertNotEquals(propsB.getProperties(), storePropertiesFromLibrary.getProperties());
    }

    @Test
    public void shouldThrowExceptionWhenAddingAFullLibraryWithNullSchema() throws Exception {
        try {
            graphLibrary.add(TEST_GRAPH_ID, null, storeProperties);
            fail(EXCEPTION_EXPECTED);
        } catch (final IllegalArgumentException e) {
            assertEquals(e.getMessage(), String.format(GraphLibrary.A_GRAPH_LIBRARY_CAN_T_BE_ADDED_WITH_A_NULL_S_GRAPH_ID_S, Schema.class.getSimpleName(), TEST_GRAPH_ID));
        }
    }

    @Test
    public void shouldThrowExeptionWhenAddingAFullLibraryWithNullStoreProperties() throws Exception {
        try {
            graphLibrary.add(TEST_GRAPH_ID, schema, null);
            fail(EXCEPTION_EXPECTED);
        } catch (final IllegalArgumentException e) {
            assertEquals(e.getMessage(), String.format(GraphLibrary.A_GRAPH_LIBRARY_CAN_T_BE_ADDED_WITH_A_NULL_S_GRAPH_ID_S, StoreProperties.class.getSimpleName(), TEST_GRAPH_ID));
        }
    }
}
