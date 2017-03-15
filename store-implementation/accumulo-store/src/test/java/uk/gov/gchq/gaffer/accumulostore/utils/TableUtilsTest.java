/*
 * Copyright 2016 Crown Copyright
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
import org.junit.Test;
import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.MockAccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.SingleUseMockAccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.key.AccumuloRuntimeException;
import uk.gov.gchq.gaffer.accumulostore.key.core.impl.byteEntity.ByteEntityAccumuloElementConverter;
import uk.gov.gchq.gaffer.accumulostore.key.impl.AggregatorIterator;
import uk.gov.gchq.gaffer.accumulostore.key.impl.ValidatorFilter;
import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestTypes;
import uk.gov.gchq.gaffer.function.aggregate.StringConcat;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class TableUtilsTest {
    public static final String TABLE_NAME = "table1";
    public static final String LOCALITY_TABLE_NAME = "localityTest";
    public static final String NO_AGGREGATORS_TABLE_NAME = "table2";

    @Test
    public void shouldCreateTableWithAllRequiredIterators() throws Exception {
        // Given
        final MockAccumuloStore store = new MockAccumuloStore();
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

        final AccumuloProperties props = AccumuloProperties.loadStoreProperties(StreamUtil.storeProps(TableUtilsTest.class));
        props.setTable(TABLE_NAME);
        store.initialise(schema, props);

        // When
        TableUtils.createTable(store);

        // Then
        final Map<String, EnumSet<IteratorScope>> itrs = store.getConnection().tableOperations().listIterators(TABLE_NAME);
        assertEquals(2, itrs.size());

        final EnumSet<IteratorScope> validator = itrs.get(AccumuloStoreConstants.VALIDATOR_ITERATOR_NAME);
        assertEquals(EnumSet.allOf(IteratorScope.class), validator);
        final IteratorSetting validatorSetting = store.getConnection().tableOperations().getIteratorSetting(TABLE_NAME, AccumuloStoreConstants.VALIDATOR_ITERATOR_NAME, IteratorScope.majc);
        assertEquals(AccumuloStoreConstants.VALIDATOR_ITERATOR_PRIORITY, validatorSetting.getPriority());
        assertEquals(ValidatorFilter.class.getName(), validatorSetting.getIteratorClass());
        final Map<String, String> validatorOptions = validatorSetting.getOptions();
        assertNotNull(Schema.fromJson(validatorOptions.get(AccumuloStoreConstants.SCHEMA).getBytes(CommonConstants.UTF_8)).getEdge(TestGroups.EDGE));
        assertEquals(ByteEntityAccumuloElementConverter.class.getName(), validatorOptions.get(AccumuloStoreConstants.ACCUMULO_ELEMENT_CONVERTER_CLASS));

        final EnumSet<IteratorScope> aggregator = itrs.get(AccumuloStoreConstants.AGGREGATOR_ITERATOR_NAME);
        assertEquals(EnumSet.allOf(IteratorScope.class), aggregator);
        final IteratorSetting aggregatorSetting = store.getConnection().tableOperations().getIteratorSetting(TABLE_NAME, AccumuloStoreConstants.AGGREGATOR_ITERATOR_NAME, IteratorScope.majc);
        assertEquals(AccumuloStoreConstants.AGGREGATOR_ITERATOR_PRIORITY, aggregatorSetting.getPriority());
        assertEquals(AggregatorIterator.class.getName(), aggregatorSetting.getIteratorClass());
        final Map<String, String> aggregatorOptions = aggregatorSetting.getOptions();
        assertNotNull(Schema.fromJson(aggregatorOptions.get(AccumuloStoreConstants.SCHEMA).getBytes(CommonConstants.UTF_8)).getEdge(TestGroups.EDGE));
        assertEquals(ByteEntityAccumuloElementConverter.class.getName(), aggregatorOptions.get(AccumuloStoreConstants.ACCUMULO_ELEMENT_CONVERTER_CLASS));


        final Map<String, String> tableProps = new HashMap<>();
        for (final Map.Entry<String, String> entry : store.getConnection()
                .tableOperations().getProperties(TABLE_NAME)) {
            tableProps.put(entry.getKey(), entry.getValue());
        }

        assertEquals(0, Integer.parseInt(tableProps.get(Property.TABLE_FILE_REPLICATION.getKey())));
    }

    @Test
    public void shouldCreateTableWithCorrectLocalityGroups() throws Exception {
        final MockAccumuloStore store = new MockAccumuloStore();
        final Schema schema = new Schema.Builder()
                .type(TestTypes.ID_STRING, String.class)
                .type(TestTypes.DIRECTED_TRUE, Boolean.class)
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .source(TestTypes.ID_STRING)
                        .destination(TestTypes.ID_STRING)
                        .directed(TestTypes.DIRECTED_TRUE)
                        .build())
                .build();

        final AccumuloProperties props = AccumuloProperties.loadStoreProperties(StreamUtil.storeProps(TableUtilsTest.class));
        props.setTable(LOCALITY_TABLE_NAME);
        store.initialise(schema, props);

        // When
        TableUtils.createTable(store);

        final Map<String, Set<Text>> localityGroups = store.getConnection().tableOperations().getLocalityGroups(LOCALITY_TABLE_NAME);
        assertEquals(1, localityGroups.size());
        Set<Text> localityGroup = localityGroups.get(TestGroups.EDGE);
        assertEquals(1, localityGroup.size());
        assertEquals(new Text(TestGroups.EDGE), localityGroup.toArray()[0]);
    }

    @Test
    public void shouldCreateTableCorrectlyIfSchemaContainsNoAggregators() throws Exception {
        // Given
        final MockAccumuloStore store = new MockAccumuloStore();
        final Schema schema = new Schema.Builder()
                .type(TestTypes.ID_STRING, String.class)
                .type(TestTypes.DIRECTED_TRUE, Boolean.class)
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .source(TestTypes.ID_STRING)
                        .destination(TestTypes.ID_STRING)
                        .directed(TestTypes.DIRECTED_TRUE)
                        .build())
                .build();

        final AccumuloProperties props = AccumuloProperties.loadStoreProperties(StreamUtil.storeProps(TableUtilsTest.class));
        props.setTable(NO_AGGREGATORS_TABLE_NAME);
        store.initialise(schema, props);

        // When
        TableUtils.createTable(store);

        // Then
        final Map<String, EnumSet<IteratorScope>> itrs = store.getConnection().tableOperations().listIterators(NO_AGGREGATORS_TABLE_NAME);
        assertEquals(1, itrs.size());

        final EnumSet<IteratorScope> validator = itrs.get(AccumuloStoreConstants.VALIDATOR_ITERATOR_NAME);
        assertEquals(EnumSet.allOf(IteratorScope.class), validator);
        final IteratorSetting validatorSetting = store.getConnection().tableOperations().getIteratorSetting(NO_AGGREGATORS_TABLE_NAME, AccumuloStoreConstants.VALIDATOR_ITERATOR_NAME, IteratorScope.majc);
        assertEquals(AccumuloStoreConstants.VALIDATOR_ITERATOR_PRIORITY, validatorSetting.getPriority());
        assertEquals(ValidatorFilter.class.getName(), validatorSetting.getIteratorClass());
        final Map<String, String> validatorOptions = validatorSetting.getOptions();
        assertNotNull(Schema.fromJson(validatorOptions.get(AccumuloStoreConstants.SCHEMA).getBytes(CommonConstants.UTF_8)).getEdge(TestGroups.EDGE));
        assertEquals(ByteEntityAccumuloElementConverter.class.getName(), validatorOptions.get(AccumuloStoreConstants.ACCUMULO_ELEMENT_CONVERTER_CLASS));

        final EnumSet<IteratorScope> aggregator = itrs.get(AccumuloStoreConstants.AGGREGATOR_ITERATOR_NAME);
        assertNull(aggregator);
        final IteratorSetting aggregatorSetting = store.getConnection().tableOperations().getIteratorSetting(NO_AGGREGATORS_TABLE_NAME, AccumuloStoreConstants.AGGREGATOR_ITERATOR_NAME, IteratorScope.majc);
        assertNull(aggregatorSetting);

        final Map<String, String> tableProps = new HashMap<>();
        for (final Map.Entry<String, String> entry : store.getConnection()
                .tableOperations().getProperties(NO_AGGREGATORS_TABLE_NAME)) {
            tableProps.put(entry.getKey(), entry.getValue());
        }

        assertEquals(0, Integer.parseInt(tableProps.get(Property.TABLE_FILE_REPLICATION.getKey())));
    }

    @Test(expected = AccumuloRuntimeException.class)
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
        properties.setStoreClass(SingleUseMockAccumuloStore.class.getName());

        final AccumuloStore store = new AccumuloStore();
        store.initialise(schema, properties);

        // When
        TableUtils.ensureTableExists(store);
        fail("The expected exception was not thrown.");
    }

    @Test(expected = AccumuloRuntimeException.class)
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
        properties.setStoreClass(SingleUseMockAccumuloStore.class.getName());

        final AccumuloStore store = new AccumuloStore();
        store.initialise(schema, properties);

        // When
        TableUtils.createTable(store);
        fail("The expected exception was not thrown.");
    }

    @Test(expected = AccumuloRuntimeException.class)
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
        properties.setStoreClass(SingleUseMockAccumuloStore.class.getName());

        // When
        new Graph.Builder()
                .addSchema(schema)
                .storeProperties(properties)
                .build();

        fail("The expected exception was not thrown.");
    }
}
