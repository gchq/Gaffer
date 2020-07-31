/*
 * Copyright 2016-2020 Crown Copyright
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

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.hadoop.io.Text;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.MiniAccumuloClusterManager;
import uk.gov.gchq.gaffer.accumulostore.SingleUseAccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.key.AccumuloRuntimeException;
import uk.gov.gchq.gaffer.accumulostore.key.core.impl.byteEntity.ByteEntityAccumuloElementConverter;
import uk.gov.gchq.gaffer.accumulostore.key.impl.ValidatorFilter;
import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.TestTypes;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.koryphe.impl.binaryoperator.StringConcat;
import uk.gov.gchq.koryphe.impl.predicate.Exists;

import java.nio.file.Path;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

public class TableUtilsTest {
    private static final String GRAPH_ID = "graph1";
    private static final String LOCALITY_GRAPH_ID = "localityTest";
    private static final String NO_AGGREGATORS_GRAPH_ID = "noAggGraph";

    private static final AccumuloProperties PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.storeProps(TableUtilsTest.class));

    private static MiniAccumuloClusterManager miniAccumuloClusterManager;

    @BeforeAll
    public static void setUpStore(@TempDir Path tempDir) {
        miniAccumuloClusterManager = new MiniAccumuloClusterManager(PROPERTIES, tempDir.toAbsolutePath().toString());
    }

    @AfterAll
    public static void tearDownStore() {
        miniAccumuloClusterManager.close();
    }

    @Test
    public void shouldCreateTableWithAllRequiredIterators() throws Exception {
        // Given
        final AccumuloStore store = new SingleUseAccumuloStore();
        final Schema schema = new Schema.Builder()
                .type(TestTypes.ID_STRING, new TypeDefinition.Builder()
                        .aggregateFunction(new StringConcat())
                        .clazz(String.class)
                        .build())
                .type(TestTypes.DIRECTED_TRUE, Boolean.class)
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .source(TestTypes.ID_STRING)
                        .destination(TestTypes.ID_STRING)
                        .directed(TestTypes.DIRECTED_TRUE)
                        .build())
                .build();

        store.initialise(GRAPH_ID, schema, PROPERTIES);

        // When
        TableUtils.createTable(store);

        // Then - this call will check the table is configured properly
        TableUtils.ensureTableExists(store);
    }

    @Test
    public void shouldFailTableValidationWhenMissingValidatorIterator() throws Exception {
        final AccumuloStore store = new SingleUseAccumuloStore();

        final Schema schema = new Schema.Builder()
                .type(TestTypes.ID_STRING, new TypeDefinition.Builder()
                        .aggregateFunction(new StringConcat())
                        .validateFunctions(new Exists())
                        .clazz(String.class)
                        .build())
                .type(TestTypes.DIRECTED_TRUE, Boolean.class)
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .source(TestTypes.ID_STRING)
                        .destination(TestTypes.ID_STRING)
                        .directed(TestTypes.DIRECTED_TRUE)
                        .build())
                .build();

        store.initialise(GRAPH_ID, schema, PROPERTIES);

        final Runnable invalidateTable = () -> {
            try {
                AddUpdateTableIterator.removeIterator(store, AccumuloStoreConstants.VALIDATOR_ITERATOR_NAME);
            } catch (final StoreException e) {
                throw new RuntimeException(e);
            }
        };

        shouldFailTableValidationWhenTableInvalid(store, invalidateTable);
    }

    @Test
    public void shouldFailTableValidationWhenMissingAggregatorIterator() throws Exception {
        final AccumuloStore store = new SingleUseAccumuloStore();

        final Schema schema = new Schema.Builder()
                .type(TestTypes.ID_STRING, new TypeDefinition.Builder()
                        .aggregateFunction(new StringConcat())
                        .validateFunctions(new Exists())
                        .clazz(String.class)
                        .build())
                .type(TestTypes.DIRECTED_TRUE, Boolean.class)
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .source(TestTypes.ID_STRING)
                        .destination(TestTypes.ID_STRING)
                        .directed(TestTypes.DIRECTED_TRUE)
                        .build())
                .build();

        store.initialise(GRAPH_ID, schema, PROPERTIES);

        final Runnable invalidateTable = () -> {
            try {
                AddUpdateTableIterator.removeIterator(store, AccumuloStoreConstants.AGGREGATOR_ITERATOR_NAME);
            } catch (final StoreException e) {
                throw new RuntimeException(e);
            }
        };

        shouldFailTableValidationWhenTableInvalid(store, invalidateTable);
    }

    public void shouldFailTableValidationWhenTableInvalid(final AccumuloStore store, final Runnable invalidateTable) throws Exception {
        // Given
        final Schema schema = new Schema.Builder()
                .type(TestTypes.ID_STRING, new TypeDefinition.Builder()
                        .aggregateFunction(new StringConcat())
                        .validateFunctions(new Exists())
                        .clazz(String.class)
                        .build())
                .type(TestTypes.DIRECTED_TRUE, Boolean.class)
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .source(TestTypes.ID_STRING)
                        .destination(TestTypes.ID_STRING)
                        .directed(TestTypes.DIRECTED_TRUE)
                        .build())
                .build();

        final AccumuloProperties props = AccumuloProperties.loadStoreProperties(StreamUtil.storeProps(TableUtilsTest.class));
        store.initialise(GRAPH_ID, schema, props);

        invalidateTable.run();

        // When / Then
        try {
            TableUtils.ensureTableExists(store);
            fail("Exception expected");
        } catch (final StoreException e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void shouldCreateTableWithCorrectLocalityGroups() throws Exception {
        final AccumuloStore store = new SingleUseAccumuloStore();
        final Schema schema = new Schema.Builder()
                .type(TestTypes.ID_STRING, String.class)
                .type(TestTypes.DIRECTED_TRUE, Boolean.class)
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .source(TestTypes.ID_STRING)
                        .destination(TestTypes.ID_STRING)
                        .directed(TestTypes.DIRECTED_TRUE)
                        .build())
                .build();

        store.initialise(LOCALITY_GRAPH_ID, schema, PROPERTIES);

        // When
        TableUtils.createTable(store);

        final Map<String, Set<Text>> localityGroups = store.getConnection().tableOperations().getLocalityGroups(LOCALITY_GRAPH_ID);
        assertEquals(1, localityGroups.size());
        Set<Text> localityGroup = localityGroups.get(TestGroups.EDGE);
        assertEquals(1, localityGroup.size());
        assertEquals(new Text(TestGroups.EDGE), localityGroup.toArray()[0]);
    }

    @Test
    public void shouldCreateTableCorrectlyIfSchemaContainsNoAggregators() throws Exception {
        // Given
        final AccumuloStore store = new SingleUseAccumuloStore();
        final Schema schema = new Schema.Builder()
                .type(TestTypes.ID_STRING, new TypeDefinition.Builder()
                        .clazz(String.class)
                        .validateFunctions(new Exists())
                        .build())
                .type(TestTypes.DIRECTED_TRUE, Boolean.class)
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .source(TestTypes.ID_STRING)
                        .destination(TestTypes.ID_STRING)
                        .directed(TestTypes.DIRECTED_TRUE)
                        .aggregate(false)
                        .build())
                .build();

        store.initialise(NO_AGGREGATORS_GRAPH_ID, schema, PROPERTIES);

        // When
        TableUtils.createTable(store);

        // Then
        final Map<String, EnumSet<IteratorScope>> itrs = store.getConnection().tableOperations().listIterators(NO_AGGREGATORS_GRAPH_ID);
        assertEquals(1, itrs.size());

        final EnumSet<IteratorScope> validator = itrs.get(AccumuloStoreConstants.VALIDATOR_ITERATOR_NAME);
        assertEquals(EnumSet.allOf(IteratorScope.class), validator);
        final IteratorSetting validatorSetting = store.getConnection().tableOperations().getIteratorSetting(NO_AGGREGATORS_GRAPH_ID, AccumuloStoreConstants.VALIDATOR_ITERATOR_NAME, IteratorScope.majc);
        assertEquals(AccumuloStoreConstants.VALIDATOR_ITERATOR_PRIORITY, validatorSetting.getPriority());
        assertEquals(ValidatorFilter.class.getName(), validatorSetting.getIteratorClass());
        final Map<String, String> validatorOptions = validatorSetting.getOptions();
        assertNotNull(Schema.fromJson(validatorOptions.get(AccumuloStoreConstants.SCHEMA).getBytes(CommonConstants.UTF_8)).getEdge(TestGroups.EDGE));
        assertEquals(ByteEntityAccumuloElementConverter.class.getName(), validatorOptions.get(AccumuloStoreConstants.ACCUMULO_ELEMENT_CONVERTER_CLASS));

        final EnumSet<IteratorScope> aggregator = itrs.get(AccumuloStoreConstants.AGGREGATOR_ITERATOR_NAME);
        assertNull(aggregator);
        final IteratorSetting aggregatorSetting = store.getConnection().tableOperations().getIteratorSetting(NO_AGGREGATORS_GRAPH_ID, AccumuloStoreConstants.AGGREGATOR_ITERATOR_NAME, IteratorScope.majc);
        assertNull(aggregatorSetting);

        final Map<String, String> tableProps = new HashMap<>();
        for (final Map.Entry<String, String> entry : store.getConnection()
                .tableOperations().getProperties(NO_AGGREGATORS_GRAPH_ID)) {
            tableProps.put(entry.getKey(), entry.getValue());
        }

        assertEquals(0, Integer.parseInt(tableProps.get(Property.TABLE_FILE_REPLICATION.getKey())));
    }

    @Test
    public void shouldThrowExceptionIfTableNameIsNotSpecified() throws StoreException {
        // Given
        final Schema schema = new Schema.Builder()
                .type("int", Integer.class)
                .type("string", String.class)
                .type("boolean", Boolean.class)
                .edge("EDGE", new SchemaEdgeDefinition.Builder()
                        .source("string")
                        .destination("string")
                        .directed("boolean")
                        .build())
                .build();

        final AccumuloProperties properties = new AccumuloProperties();
        properties.setStoreClass(PROPERTIES.getStoreClass());
        properties.setZookeepers(PROPERTIES.getZookeepers());

        final AccumuloStore store = new AccumuloStore();
        assertThrows(IllegalArgumentException.class,
                () -> store.initialise(null, schema, properties));

        // When
        assertThrows(AccumuloRuntimeException.class,
                () -> TableUtils.ensureTableExists(store));
    }

    @Test
    public void shouldThrowExceptionIfTableNameIsNotSpecifiedWhenCreatingTable() throws StoreException, TableExistsException {
        // Given
        final Schema schema = new Schema.Builder()
                .type("int", Integer.class)
                .type("string", String.class)
                .type("boolean", Boolean.class)
                .edge("EDGE", new SchemaEdgeDefinition.Builder()
                        .source("string")
                        .destination("string")
                        .directed("boolean")
                        .build())
                .build();

        final AccumuloProperties properties = new AccumuloProperties();
        properties.setStoreClass(PROPERTIES.getStoreClass());
        properties.setZookeepers((PROPERTIES.getZookeepers()));

        final AccumuloStore store = new AccumuloStore();
        assertThrows(IllegalArgumentException.class,
                () -> store.initialise(null, schema, properties));

        // When
        assertThrows(AccumuloRuntimeException.class,
                () -> TableUtils.createTable(store));
    }

    @Test
    public void shouldThrowExceptionIfTableNameIsNotSpecifiedWhenCreatingAGraph() {
        // Given
        final Schema schema = new Schema.Builder()
                .type("int", Integer.class)
                .type("string", String.class)
                .type("boolean", Boolean.class)
                .edge("EDGE", new SchemaEdgeDefinition.Builder()
                        .source("string")
                        .destination("string")
                        .directed("boolean")
                        .build())
                .build();

        final AccumuloProperties properties = new AccumuloProperties();
        properties.setStoreClass(PROPERTIES.getStoreClass());
        properties.setZookeepers(PROPERTIES.getZookeepers());

        // When
        assertThrows(IllegalArgumentException.class,
                () -> new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(null)
                        .build())
                .addSchema(schema)
                .storeProperties(properties)
                .build());
    }
}
