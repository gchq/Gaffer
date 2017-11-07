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
import uk.gov.gchq.gaffer.commonutil.exception.OverwritingException;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.store.StoreProperties;
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
    private static final String TEST_UNKNOWN_ID = "unknownId";
    private static final String TEST_SCHEMA_ID = "testSchemaId";
    private static final String TEST_PROPERTIES_ID = "testPropertiesId";
    private static final String EXCEPTION_EXPECTED = "Exception expected";

    private Schema schema = new Schema.Builder().build();
    private Schema schema1 = new Schema.Builder().build();
    private StoreProperties storeProperties = new StoreProperties();
    private StoreProperties storeProperties1 = new StoreProperties();

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

        assertEquals(new Pair<>(TEST_GRAPH_ID, TEST_GRAPH_ID), graphLibrary.getIds(TEST_GRAPH_ID));
        assertEquals(new Pair<>(TEST_GRAPH_ID_1, TEST_GRAPH_ID_1), graphLibrary.getIds(TEST_GRAPH_ID_1));
    }

    @Test
    public void shouldAddAndGetIdsInGraphLibrary() {
        // When
        graphLibrary.add(TEST_GRAPH_ID, schema, storeProperties);

        // Then
        assertEquals(new Pair<>(TEST_GRAPH_ID, TEST_GRAPH_ID), graphLibrary.getIds(TEST_GRAPH_ID));
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
        Schema tempSchema = new Schema.Builder().edge("testEdge", new SchemaEdgeDefinition()).build();

        // When / Then
        try {
            graphLibrary.add(TEST_GRAPH_ID, tempSchema, storeProperties);
            fail(EXCEPTION_EXPECTED);
        } catch (final OverwritingException e) {
            assertTrue(e.getMessage().contains("already exists with a different schema"));
        }
    }

    @Test
    public void shouldUpdateSchema() {
        // Given
        graphLibrary.addOrUpdateSchema(TEST_SCHEMA_ID, schema);
        Schema tempSchema = new Schema.Builder().edge("testEdge", new SchemaEdgeDefinition()).build();

        // Then
        JsonAssert.assertEquals(schema.toJson(false), graphLibrary.getSchema(TEST_SCHEMA_ID).toJson(false));

        // When
        graphLibrary.addOrUpdateSchema(TEST_SCHEMA_ID, tempSchema);

        // Then
        JsonAssert.assertEquals(tempSchema.toJson(false), graphLibrary.getSchema(TEST_SCHEMA_ID).toJson(false));
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
        StoreProperties tempStoreProperties = storeProperties.clone();
        tempStoreProperties.set("testKey", "testValue");

        // When / Then
        try {
            graphLibrary.add(TEST_GRAPH_ID, schema, tempStoreProperties);
            fail(EXCEPTION_EXPECTED);
        } catch (final Exception e) {
            assertTrue(e.getMessage().contains("already exists with a different store properties"));
        }
    }

    @Test
    public void shouldUpdateStoreProperties() {
        // Given
        graphLibrary.addOrUpdateProperties(TEST_PROPERTIES_ID, storeProperties);
        StoreProperties tempStoreProperties = storeProperties.clone();
        tempStoreProperties.set("testKey", "testValue");

        // Then
        assertEquals(storeProperties.getProperties(), graphLibrary.getProperties(TEST_PROPERTIES_ID).getProperties());

        // When
        graphLibrary.addOrUpdateProperties(TEST_PROPERTIES_ID, tempStoreProperties);

        // Then
        assertEquals(tempStoreProperties.getProperties(), graphLibrary.getProperties(TEST_PROPERTIES_ID).getProperties());
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
        assertEquals(storeProperties, graphLibrary.getProperties(TEST_GRAPH_ID));

        // When
        graphLibrary.addOrUpdate(TEST_GRAPH_ID, schema, storeProperties1);

        // Then
        assertEquals(storeProperties1, graphLibrary.getProperties(TEST_GRAPH_ID));
    }

    @Test
    public void shouldReturnNullWhenPropertyIdIsNotFound() {
        // When
        final StoreProperties unknownStoreProperties = graphLibrary.getProperties(TEST_UNKNOWN_ID);

        // Then
        assertNull(unknownStoreProperties);
    }

    @Test
    public void shouldReturnNullWhenSchemaIdIsNotFound() {
        // When
        final Schema unknownSchema = graphLibrary.getSchema(TEST_UNKNOWN_ID);

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
        byte[] entitySchema = new Builder().entity("e1", new SchemaEntityDefinition.Builder().property("p1", "string").build()).type("string", String.class).build().toJson(true);
        byte[] edgeSchema = new Builder().edge("e1", new SchemaEdgeDefinition.Builder().property("p1", "string").build()).type("string", String.class).build().toJson(true);

        graphLibrary.addSchema(clashingId, Schema.fromJson(entitySchema));

        try {
            graphLibrary.add("graph", clashingId, Schema.fromJson(edgeSchema), TEST_PROPERTIES_ID, new StoreProperties());
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
        StoreProperties propsA = new StoreProperties();
        propsA.set("a", "a");
        StoreProperties propsB = new StoreProperties();
        propsB.set("b", "b");

        graphLibrary.addProperties(clashingId, propsA);

        try {
            graphLibrary.add("graph", TEST_SCHEMA_ID, new Schema(), clashingId, propsB);
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
    public void shouldThrowExceptionWhenAddingAFullLibraryWithNullStoreProperties() throws Exception {
        try {
            graphLibrary.add(TEST_GRAPH_ID, schema, null);
            fail(EXCEPTION_EXPECTED);
        } catch (final IllegalArgumentException e) {
            assertEquals(e.getMessage(), String.format(GraphLibrary.A_GRAPH_LIBRARY_CAN_T_BE_ADDED_WITH_A_NULL_S_GRAPH_ID_S, StoreProperties.class.getSimpleName(), TEST_GRAPH_ID));
        }
    }

    @Test
    public void shouldThrowExceptionWhenAddingAFullLibraryWithNullSchemaAndStoreProperties() throws Exception {
        try {
            graphLibrary.add(TEST_GRAPH_ID, null, null);
            fail(EXCEPTION_EXPECTED);
        } catch (final IllegalArgumentException e) {
            assertEquals(e.getMessage(), String.format(GraphLibrary.A_GRAPH_LIBRARY_CAN_T_BE_ADDED_WITH_A_NULL_S_GRAPH_ID_S, Schema.class.getSimpleName() + " and " + StoreProperties.class.getSimpleName(), TEST_GRAPH_ID));
        }
    }
}
