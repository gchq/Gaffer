/*
 * Copyright 2016-2019 Crown Copyright
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
package uk.gov.gchq.gaffer.accumulostore.key.core.impl;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.SingleUseMockAccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.key.AccumuloElementConverter;
import uk.gov.gchq.gaffer.accumulostore.key.core.impl.byteEntity.ByteEntityAccumuloElementConverter;
import uk.gov.gchq.gaffer.accumulostore.key.core.impl.classic.ClassicAccumuloElementConverter;
import uk.gov.gchq.gaffer.accumulostore.utils.AccumuloPropertyNames;
import uk.gov.gchq.gaffer.accumulostore.utils.AccumuloStoreConstants;
import uk.gov.gchq.gaffer.accumulostore.utils.IteratorSettingBuilder;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static uk.gov.gchq.gaffer.accumulostore.utils.TableUtils.createTable;

public class CoreKeyGroupByAggregatorIteratorTest {
    private static AccumuloStore byteEntityStore;
    private static AccumuloStore gaffer1KeyStore;
    private static final Schema SCHEMA = Schema.fromJson(StreamUtil.schemas(CoreKeyGroupByAggregatorIteratorTest.class));
    private static final AccumuloProperties PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.storeProps(CoreKeyGroupByAggregatorIteratorTest.class));
    private static final AccumuloProperties CLASSIC_PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.openStream(CoreKeyGroupByAggregatorIteratorTest.class, "/accumuloStoreClassicKeys.properties"));

    private static AccumuloElementConverter byteEntityElementConverter;
    private static AccumuloElementConverter gaffer1ElementConverter;

    @BeforeClass
    public static void setup() {
        byteEntityStore = new SingleUseMockAccumuloStore();
        gaffer1KeyStore = new SingleUseMockAccumuloStore();
        gaffer1ElementConverter = new ClassicAccumuloElementConverter(SCHEMA);
        byteEntityElementConverter = new ByteEntityAccumuloElementConverter(SCHEMA);
    }

    @Before
    public void reInitialise() throws StoreException, TableExistsException {
        byteEntityStore.initialise("byteEntityGraph", SCHEMA, PROPERTIES);
        gaffer1KeyStore.initialise("gaffer1Graph", SCHEMA, CLASSIC_PROPERTIES);
        createTable(byteEntityStore);
        createTable(gaffer1KeyStore);
    }

    @AfterClass
    public static void tearDown() {
        gaffer1KeyStore = null;
        byteEntityStore = null;
    }

    @Test
    public void shouldMultiplePropertySetsAggregateInByteEntityStore() throws StoreException {
        testAggregatingMultiplePropertySets(byteEntityStore, byteEntityElementConverter);
    }

    @Test
    public void shouldMultiplePropertySetsAggregateInGafferOneStore() throws StoreException {
        testAggregatingMultiplePropertySets(gaffer1KeyStore, gaffer1ElementConverter);
    }

    private void testAggregatingMultiplePropertySets(final AccumuloStore store, final AccumuloElementConverter elementConverter) throws StoreException {
        String visibilityString = "public";
        try {

            // Create edge
            final Edge edge = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("1")
                    .dest("2")
                    .directed(true)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 8)
                    .property(AccumuloPropertyNames.PROP_1, 0)
                    .property(AccumuloPropertyNames.PROP_2, 0)
                    .property(AccumuloPropertyNames.PROP_3, 0)
                    .property(AccumuloPropertyNames.PROP_4, 0)
                    .property(AccumuloPropertyNames.COUNT, 1)
                    .build();

            //THIS EDGE WILL BE REDUCED MEANING ITS CQ (columnQualifier) will only occur once because its key is equal.
            final Edge edge2 = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("1")
                    .dest("2")
                    .directed(true)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 1)
                    .property(AccumuloPropertyNames.PROP_1, 0)
                    .property(AccumuloPropertyNames.PROP_2, 0)
                    .property(AccumuloPropertyNames.PROP_3, 0)
                    .property(AccumuloPropertyNames.PROP_4, 0)
                    .property(AccumuloPropertyNames.COUNT, 2)
                    .build();

            final Edge edge3 = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("1")
                    .dest("2")
                    .directed(true)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 1)
                    .property(AccumuloPropertyNames.PROP_1, 0)
                    .property(AccumuloPropertyNames.PROP_2, 0)
                    .property(AccumuloPropertyNames.PROP_3, 0)
                    .property(AccumuloPropertyNames.PROP_4, 0)
                    .property(AccumuloPropertyNames.COUNT, 10)
                    .build();

            // Accumulo key
            final Key key = elementConverter.getKeysFromEdge(edge).getFirst();
            final Key key2 = elementConverter.getKeysFromEdge(edge2).getFirst();
            final Key key3 = elementConverter.getKeysFromEdge(edge3).getFirst();

            // Accumulo values
            final Value value1 = elementConverter.getValueFromProperties(TestGroups.EDGE, edge.getProperties());
            final Value value2 = elementConverter.getValueFromProperties(TestGroups.EDGE, edge2.getProperties());
            final Value value3 = elementConverter.getValueFromProperties(TestGroups.EDGE, edge3.getProperties());

            // Create mutation
            final Mutation m1 = new Mutation(key.getRow());
            m1.put(key.getColumnFamily(), key.getColumnQualifier(), new ColumnVisibility(key.getColumnVisibility()), key.getTimestamp(), value1);
            final Mutation m2 = new Mutation(key.getRow());
            m2.put(key.getColumnFamily(), key.getColumnQualifier(), new ColumnVisibility(key.getColumnVisibility()), key.getTimestamp(), value2);
            final Mutation m3 = new Mutation(key.getRow());
            m3.put(key.getColumnFamily(), key.getColumnQualifier(), new ColumnVisibility(key.getColumnVisibility()), key.getTimestamp(), value3);
            final Mutation m4 = new Mutation(key2.getRow());
            m4.put(key2.getColumnFamily(), key2.getColumnQualifier(), new ColumnVisibility(key2.getColumnVisibility()), key2.getTimestamp(), value1);
            final Mutation m5 = new Mutation(key.getRow());
            m5.put(key3.getColumnFamily(), key3.getColumnQualifier(), new ColumnVisibility(key3.getColumnVisibility()), key3.getTimestamp(), value1);

            // Write mutation
            final BatchWriterConfig writerConfig = new BatchWriterConfig();
            writerConfig.setMaxMemory(1000000L);
            writerConfig.setMaxLatency(1000L, TimeUnit.MILLISECONDS);
            writerConfig.setMaxWriteThreads(1);
            final BatchWriter writer = store.getConnection().createBatchWriter(store.getTableName(), writerConfig);
            writer.addMutation(m1);
            writer.addMutation(m2);
            writer.addMutation(m3);
            writer.addMutation(m4);
            writer.addMutation(m5);
            writer.close();

            // Read data back and check we get one merged element
            final Authorizations authorizations = new Authorizations(visibilityString);
            final Scanner scanner = store.getConnection().createScanner(store.getTableName(), authorizations);
            final IteratorSetting iteratorSetting = new IteratorSettingBuilder(AccumuloStoreConstants.COLUMN_QUALIFIER_AGGREGATOR_ITERATOR_PRIORITY,
                    "KeyCombiner", CoreKeyGroupByAggregatorIterator.class)
                    .all()
                    .schema(store.getSchema())
                    .view(new View.Builder()
                            .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                                    .groupBy()
                                    .build())
                            .build())
                    .keyConverter(store.getKeyPackage().getKeyConverter())
                    .build();
            scanner.addScanIterator(iteratorSetting);
            final Iterator<Entry<Key, Value>> it = scanner.iterator();
            final Entry<Key, Value> entry = it.next();
            final Element readEdge = elementConverter.getFullElement(entry.getKey(), entry.getValue(), false);

            final Edge expectedEdge = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("1")
                    .dest("2")
                    .directed(true)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 9)
                    .property(AccumuloPropertyNames.COUNT, 15)
                    .property(AccumuloPropertyNames.PROP_1, 0)
                    .property(AccumuloPropertyNames.PROP_2, 0)
                    .property(AccumuloPropertyNames.PROP_3, 0)
                    .property(AccumuloPropertyNames.PROP_4, 0)
                    .build();

            assertEquals(expectedEdge, readEdge);
            assertEquals(9, readEdge.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER));
            assertEquals(15, readEdge.getProperty(AccumuloPropertyNames.COUNT));
            // Check no more entries
            if (it.hasNext()) {
                fail("Additional row found.");
            }

        } catch (final AccumuloException | TableNotFoundException e) {
            fail(this.getClass().getSimpleName() + " failed with exception: " + e);
        }
    }

    @Test
    public void shouldSinglePropertySetAggregateInByteEntityStore() throws StoreException {
        testAggregatingSinglePropertySet(byteEntityStore, byteEntityElementConverter);
    }

    @Test
    public void shouldSinglePropertySetAggregateInGafferOneStore() throws StoreException {
        testAggregatingSinglePropertySet(gaffer1KeyStore, gaffer1ElementConverter);
    }

    public void testAggregatingSinglePropertySet(final AccumuloStore store, final AccumuloElementConverter elementConverter) throws StoreException {
        String visibilityString = "public";
        try {
            // Create edge
            final Edge edge = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("1")
                    .dest("2")
                    .directed(true)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 8)
                    .property(AccumuloPropertyNames.COUNT, 1)
                    .build();

            final Properties properties1 = new Properties();
            properties1.put(AccumuloPropertyNames.COUNT, 1);

            // Accumulo key
            final Key key = elementConverter.getKeysFromEdge(edge).getFirst();

            // Accumulo values
            final Value value1 = elementConverter.getValueFromProperties(TestGroups.EDGE, properties1);

            // Create mutation
            final Mutation m1 = new Mutation(key.getRow());
            m1.put(key.getColumnFamily(), key.getColumnQualifier(), new ColumnVisibility(key.getColumnVisibility()), key.getTimestamp(), value1);

            // Write mutation
            final BatchWriterConfig writerConfig = new BatchWriterConfig();
            writerConfig.setMaxMemory(1000000L);
            writerConfig.setMaxLatency(1000L, TimeUnit.MILLISECONDS);
            writerConfig.setMaxWriteThreads(1);
            final BatchWriter writer = store.getConnection().createBatchWriter(store.getTableName(), writerConfig);
            writer.addMutation(m1);
            writer.close();

            final Edge expectedEdge = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("1")
                    .dest("2")
                    .directed(true)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 8)
                    .property(AccumuloPropertyNames.COUNT, 1)
                    .build();

            // Read data back and check we get one merged element
            final Authorizations authorizations = new Authorizations(visibilityString);
            final Scanner scanner = store.getConnection().createScanner(store.getTableName(), authorizations);
            final IteratorSetting iteratorSetting = new IteratorSettingBuilder(AccumuloStoreConstants.COLUMN_QUALIFIER_AGGREGATOR_ITERATOR_PRIORITY,
                    "KeyCombiner", CoreKeyGroupByAggregatorIterator.class)
                    .all()
                    .view(new View.Builder()
                            .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                                    .groupBy()
                                    .build())
                            .build())
                    .schema(store.getSchema())
                    .keyConverter(store.getKeyPackage().getKeyConverter())
                    .build();
            scanner.addScanIterator(iteratorSetting);
            final Iterator<Entry<Key, Value>> it = scanner.iterator();
            final Entry<Key, Value> entry = it.next();
            final Element readEdge = elementConverter.getFullElement(entry.getKey(), entry.getValue(), false);
            assertEquals(expectedEdge, readEdge);
            assertEquals(8, readEdge.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER));
            assertEquals(1, readEdge.getProperty(AccumuloPropertyNames.COUNT));
            // Check no more entries
            if (it.hasNext()) {
                fail("Additional row found.");
            }

        } catch (final AccumuloException | TableNotFoundException e) {
            fail(this.getClass().getSimpleName() + " failed with exception: " + e);
        }
    }

    @Test
    public void shouldEmptyColumnQualifierAggregateInByteEntityStore() throws StoreException {
        testAggregatingEmptyColumnQualifier(byteEntityStore, byteEntityElementConverter);
    }

    @Test
    public void shouldEmptyColumnQualifierAggregateInGafferOneStore() throws StoreException {
        testAggregatingEmptyColumnQualifier(gaffer1KeyStore, gaffer1ElementConverter);
    }

    public void testAggregatingEmptyColumnQualifier(final AccumuloStore store, final AccumuloElementConverter elementConverter) throws StoreException {
        final String visibilityString = "public";
        try {
            // Create edge
            final Edge edge = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("1")
                    .dest("2")
                    .directed(true)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 8)
                    .property(AccumuloPropertyNames.PROP_1, 0)
                    .property(AccumuloPropertyNames.PROP_2, 0)
                    .property(AccumuloPropertyNames.PROP_3, 0)
                    .property(AccumuloPropertyNames.PROP_4, 0)
                    .property(AccumuloPropertyNames.COUNT, 1)
                    .build();

            //THIS EDGE WILL BE REDUCED MEANING ITS CQ (columnQualifier) will only occur once because its key is equal.
            final Edge edge2 = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("1")
                    .dest("2")
                    .directed(true)
                    .property(AccumuloPropertyNames.PROP_1, 0)
                    .property(AccumuloPropertyNames.PROP_2, 0)
                    .property(AccumuloPropertyNames.PROP_3, 0)
                    .property(AccumuloPropertyNames.PROP_4, 0)
                    .property(AccumuloPropertyNames.COUNT, 2)
                    .build();

            final Edge edge3 = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("1")
                    .dest("2")
                    .directed(true)
                    .property(AccumuloPropertyNames.PROP_1, 0)
                    .property(AccumuloPropertyNames.PROP_2, 0)
                    .property(AccumuloPropertyNames.PROP_3, 0)
                    .property(AccumuloPropertyNames.PROP_4, 0)
                    .property(AccumuloPropertyNames.COUNT, 10)
                    .build();

            // Accumulo key
            final Key key = elementConverter.getKeysFromEdge(edge).getFirst();
            final Key key2 = elementConverter.getKeysFromEdge(edge2).getFirst();
            final Key key3 = elementConverter.getKeysFromEdge(edge3).getFirst();

            // Accumulo values
            final Value value1 = elementConverter.getValueFromProperties(TestGroups.EDGE, edge.getProperties());
            final Value value2 = elementConverter.getValueFromProperties(TestGroups.EDGE, edge2.getProperties());
            final Value value3 = elementConverter.getValueFromProperties(TestGroups.EDGE, edge3.getProperties());

            // Create mutation
            final Mutation m1 = new Mutation(key.getRow());
            m1.put(key.getColumnFamily(), key.getColumnQualifier(), new ColumnVisibility(key.getColumnVisibility()), key.getTimestamp(), value1);
            final Mutation m2 = new Mutation(key.getRow());
            m2.put(key.getColumnFamily(), key.getColumnQualifier(), new ColumnVisibility(key.getColumnVisibility()), key.getTimestamp(), value2);
            final Mutation m3 = new Mutation(key.getRow());
            m3.put(key.getColumnFamily(), key.getColumnQualifier(), new ColumnVisibility(key.getColumnVisibility()), key.getTimestamp(), value3);
            final Mutation m4 = new Mutation(key2.getRow());
            m4.put(key2.getColumnFamily(), key2.getColumnQualifier(), new ColumnVisibility(key2.getColumnVisibility()), key2.getTimestamp(), value1);
            final Mutation m5 = new Mutation(key.getRow());
            m5.put(key3.getColumnFamily(), key3.getColumnQualifier(), new ColumnVisibility(key3.getColumnVisibility()), key3.getTimestamp(), value1);

            // Write mutation
            final BatchWriterConfig writerConfig = new BatchWriterConfig();
            writerConfig.setMaxMemory(1000000L);
            writerConfig.setMaxLatency(1000L, TimeUnit.MILLISECONDS);
            writerConfig.setMaxWriteThreads(1);
            final BatchWriter writer = store.getConnection().createBatchWriter(store.getTableName(), writerConfig);
            writer.addMutation(m1);
            writer.addMutation(m2);
            writer.addMutation(m3);
            writer.addMutation(m4);
            writer.addMutation(m5);
            writer.close();

            Edge expectedEdge = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("1")
                    .dest("2")
                    .directed(true)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 8)
                    .property(AccumuloPropertyNames.COUNT, 15)
                    .property(AccumuloPropertyNames.PROP_1, 0)
                    .property(AccumuloPropertyNames.PROP_2, 0)
                    .property(AccumuloPropertyNames.PROP_3, 0)
                    .property(AccumuloPropertyNames.PROP_4, 0)
                    .build();

            // Read data back and check we get one merged element
            final Authorizations authorizations = new Authorizations(visibilityString);
            final Scanner scanner = store.getConnection().createScanner(store.getTableName(), authorizations);
            final IteratorSetting iteratorSetting = new IteratorSettingBuilder(AccumuloStoreConstants.COLUMN_QUALIFIER_AGGREGATOR_ITERATOR_PRIORITY,
                    "KeyCombiner", CoreKeyGroupByAggregatorIterator.class)
                    .all()
                    .view(new View.Builder()
                            .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                                    .groupBy()
                                    .build())
                            .build())
                    .schema(store.getSchema())
                    .keyConverter(store.getKeyPackage().getKeyConverter())
                    .build();
            scanner.addScanIterator(iteratorSetting);
            final Iterator<Entry<Key, Value>> it = scanner.iterator();
            final Entry<Key, Value> entry = it.next();
            final Element readEdge = elementConverter.getFullElement(entry.getKey(), entry.getValue(), false);
            assertEquals(expectedEdge, readEdge);
            assertEquals(8, readEdge.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER));
            assertEquals(15, readEdge.getProperty(AccumuloPropertyNames.COUNT));
            // Check no more entries
            if (it.hasNext()) {
                fail("Additional row found.");
            }

        } catch (final AccumuloException | TableNotFoundException e) {
            fail(this.getClass().getSimpleName() + " failed with exception: " + e);
        }
    }

    @Test
    public void shouldPartiallyAggregateColumnQualifierOverCQ1GroupByInByteEntityStore() throws StoreException {
        shouldPartiallyAggregateColumnQualifierOverCQ1GroupBy(byteEntityStore, byteEntityElementConverter);
    }

    @Test
    public void shouldPartiallyAggregateColumnQualifierOverCQ1GroupByInGafferOneStore() throws StoreException {
        shouldPartiallyAggregateColumnQualifierOverCQ1GroupBy(gaffer1KeyStore, gaffer1ElementConverter);
    }

    public void shouldPartiallyAggregateColumnQualifierOverCQ1GroupBy(final AccumuloStore store, final AccumuloElementConverter elementConverter) throws StoreException {
        final String visibilityString = "public";
        try {
            // Create edge
            final Edge edge = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("1")
                    .dest("2")
                    .directed(true)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 1)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER_2, 1)
                    .property(AccumuloPropertyNames.PROP_1, 0)
                    .property(AccumuloPropertyNames.PROP_2, 0)
                    .property(AccumuloPropertyNames.PROP_3, 0)
                    .property(AccumuloPropertyNames.PROP_4, 0)
                    .property(AccumuloPropertyNames.COUNT, 1)
                    .build();

            final Edge edge2 = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("1")
                    .dest("2")
                    .directed(true)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 1)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER_2, 2)
                    .property(AccumuloPropertyNames.PROP_1, 0)
                    .property(AccumuloPropertyNames.PROP_2, 0)
                    .property(AccumuloPropertyNames.PROP_3, 0)
                    .property(AccumuloPropertyNames.PROP_4, 0)
                    .property(AccumuloPropertyNames.COUNT, 1)
                    .build();

            final Edge edge3 = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("1")
                    .dest("2")
                    .directed(true)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 1)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER_2, 3)
                    .property(AccumuloPropertyNames.PROP_1, 0)
                    .property(AccumuloPropertyNames.PROP_2, 0)
                    .property(AccumuloPropertyNames.PROP_3, 0)
                    .property(AccumuloPropertyNames.PROP_4, 0)
                    .property(AccumuloPropertyNames.COUNT, 1)
                    .build();


            //THIS EDGE WILL BE REDUCED MEANING ITS CQ (columnQualifier) will only occur once because its key is equal.
            final Edge edge4 = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("1")
                    .dest("2")
                    .directed(true)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 2)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER_2, 4)
                    .property(AccumuloPropertyNames.PROP_1, 0)
                    .property(AccumuloPropertyNames.PROP_2, 0)
                    .property(AccumuloPropertyNames.PROP_3, 0)
                    .property(AccumuloPropertyNames.PROP_4, 0)
                    .property(AccumuloPropertyNames.COUNT, 2)
                    .build();

            final Edge edge5 = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("1")
                    .dest("2")
                    .directed(true)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 3)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER_2, 5)
                    .property(AccumuloPropertyNames.PROP_1, 0)
                    .property(AccumuloPropertyNames.PROP_2, 0)
                    .property(AccumuloPropertyNames.PROP_3, 0)
                    .property(AccumuloPropertyNames.PROP_4, 0)
                    .property(AccumuloPropertyNames.COUNT, 10)
                    .build();

            final Edge edge6 = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("1")
                    .dest("2")
                    .directed(true)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 4)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER_2, 6)
                    .property(AccumuloPropertyNames.PROP_1, 0)
                    .property(AccumuloPropertyNames.PROP_2, 0)
                    .property(AccumuloPropertyNames.PROP_3, 0)
                    .property(AccumuloPropertyNames.PROP_4, 0)
                    .property(AccumuloPropertyNames.COUNT, 5)
                    .build();

            // Accumulo key
            final Key key = elementConverter.getKeysFromEdge(edge).getFirst();
            final Key key2 = elementConverter.getKeysFromEdge(edge2).getFirst();
            final Key key3 = elementConverter.getKeysFromEdge(edge3).getFirst();
            final Key key4 = elementConverter.getKeysFromEdge(edge4).getFirst();
            final Key key5 = elementConverter.getKeysFromEdge(edge5).getFirst();
            final Key key6 = elementConverter.getKeysFromEdge(edge6).getFirst();

            // Accumulo values
            final Value value1 = elementConverter.getValueFromProperties(TestGroups.EDGE, edge.getProperties());
            final Value value2 = elementConverter.getValueFromProperties(TestGroups.EDGE, edge2.getProperties());
            final Value value3 = elementConverter.getValueFromProperties(TestGroups.EDGE, edge3.getProperties());
            final Value value4 = elementConverter.getValueFromProperties(TestGroups.EDGE, edge4.getProperties());
            final Value value5 = elementConverter.getValueFromProperties(TestGroups.EDGE, edge5.getProperties());
            final Value value6 = elementConverter.getValueFromProperties(TestGroups.EDGE, edge6.getProperties());

            // Create mutation
            final Mutation m1 = new Mutation(key.getRow());
            m1.put(key.getColumnFamily(), key.getColumnQualifier(), new ColumnVisibility(key.getColumnVisibility()), key.getTimestamp(), value1);
            final Mutation m2 = new Mutation(key2.getRow());
            m2.put(key2.getColumnFamily(), key2.getColumnQualifier(), new ColumnVisibility(key2.getColumnVisibility()), key2.getTimestamp(), value2);
            final Mutation m3 = new Mutation(key.getRow());
            m3.put(key3.getColumnFamily(), key3.getColumnQualifier(), new ColumnVisibility(key3.getColumnVisibility()), key3.getTimestamp(), value3);
            final Mutation m4 = new Mutation(key.getRow());
            m4.put(key4.getColumnFamily(), key4.getColumnQualifier(), new ColumnVisibility(key4.getColumnVisibility()), key4.getTimestamp(), value4);
            final Mutation m5 = new Mutation(key.getRow());
            m5.put(key5.getColumnFamily(), key5.getColumnQualifier(), new ColumnVisibility(key5.getColumnVisibility()), key5.getTimestamp(), value5);
            final Mutation m6 = new Mutation(key.getRow());
            m6.put(key6.getColumnFamily(), key6.getColumnQualifier(), new ColumnVisibility(key6.getColumnVisibility()), key6.getTimestamp(), value6);

            // Write mutation
            final BatchWriterConfig writerConfig = new BatchWriterConfig();
            writerConfig.setMaxMemory(1000000L);
            writerConfig.setMaxLatency(1000L, TimeUnit.MILLISECONDS);
            writerConfig.setMaxWriteThreads(1);
            final BatchWriter writer = store.getConnection().createBatchWriter(store.getTableName(), writerConfig);
            writer.addMutation(m1);
            writer.addMutation(m2);
            writer.addMutation(m3);
            writer.addMutation(m4);
            writer.addMutation(m5);
            writer.addMutation(m6);
            writer.close();

            Edge expectedEdge1 = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("1")
                    .dest("2")
                    .directed(true)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 1)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER_2, 6)
                    .property(AccumuloPropertyNames.COUNT, 3)
                    .property(AccumuloPropertyNames.PROP_1, 0)
                    .property(AccumuloPropertyNames.PROP_2, 0)
                    .property(AccumuloPropertyNames.PROP_3, 0)
                    .property(AccumuloPropertyNames.PROP_4, 0)
                    .build();

            Edge expectedEdge2 = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("1")
                    .dest("2")
                    .directed(true)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 2)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER_2, 4)
                    .property(AccumuloPropertyNames.COUNT, 2)
                    .property(AccumuloPropertyNames.PROP_1, 0)
                    .property(AccumuloPropertyNames.PROP_2, 0)
                    .property(AccumuloPropertyNames.PROP_3, 0)
                    .property(AccumuloPropertyNames.PROP_4, 0)
                    .build();

            Edge expectedEdge3 = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("1")
                    .dest("2")
                    .directed(true)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 3)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER_2, 5)
                    .property(AccumuloPropertyNames.COUNT, 10)
                    .property(AccumuloPropertyNames.PROP_1, 0)
                    .property(AccumuloPropertyNames.PROP_2, 0)
                    .property(AccumuloPropertyNames.PROP_3, 0)
                    .property(AccumuloPropertyNames.PROP_4, 0)
                    .build();

            Edge expectedEdge4 = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("1")
                    .dest("2")
                    .directed(true)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 4)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER_2, 6)
                    .property(AccumuloPropertyNames.COUNT, 5)
                    .property(AccumuloPropertyNames.PROP_1, 0)
                    .property(AccumuloPropertyNames.PROP_2, 0)
                    .property(AccumuloPropertyNames.PROP_3, 0)
                    .property(AccumuloPropertyNames.PROP_4, 0)
                    .build();

            // Read data back and check we get one merged element
            final Authorizations authorizations = new Authorizations(visibilityString);
            final Scanner scanner = store.getConnection().createScanner(store.getTableName(), authorizations);
            final IteratorSetting iteratorSetting = new IteratorSettingBuilder(AccumuloStoreConstants.COLUMN_QUALIFIER_AGGREGATOR_ITERATOR_PRIORITY,
                    "KeyCombiner", CoreKeyGroupByAggregatorIterator.class)
                    .all()
                    .view(new View.Builder()
                            .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                                    .groupBy(AccumuloPropertyNames.COLUMN_QUALIFIER)
                                    .build())
                            .build())
                    .schema(store.getSchema())
                    .keyConverter(store.getKeyPackage().getKeyConverter())
                    .build();
            scanner.addScanIterator(iteratorSetting);
            final Iterator<Entry<Key, Value>> it = scanner.iterator();
            Entry<Key, Value> entry = it.next();
            Element readEdge = elementConverter.getFullElement(entry.getKey(), entry.getValue(), false);
            assertEquals(expectedEdge1, readEdge);
            assertEquals(1, readEdge.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER));
            assertEquals(6, readEdge.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER_2));
            assertEquals(3, readEdge.getProperty(AccumuloPropertyNames.COUNT));
            entry = it.next();
            readEdge = elementConverter.getFullElement(entry.getKey(), entry.getValue(), false);
            assertEquals(expectedEdge2, readEdge);
            assertEquals(2, readEdge.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER));
            assertEquals(4, readEdge.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER_2));
            assertEquals(2, readEdge.getProperty(AccumuloPropertyNames.COUNT));
            entry = it.next();
            readEdge = elementConverter.getFullElement(entry.getKey(), entry.getValue(), false);
            assertEquals(expectedEdge3, readEdge);
            assertEquals(3, readEdge.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER));
            assertEquals(5, readEdge.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER_2));
            assertEquals(10, readEdge.getProperty(AccumuloPropertyNames.COUNT));
            entry = it.next();
            readEdge = elementConverter.getFullElement(entry.getKey(), entry.getValue(), false);
            assertEquals(expectedEdge4, readEdge);
            assertEquals(4, readEdge.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER));
            assertEquals(6, readEdge.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER_2));
            assertEquals(5, readEdge.getProperty(AccumuloPropertyNames.COUNT));

            if (it.hasNext()) {
                fail("Additional row found.");
            }

        } catch (final AccumuloException | TableNotFoundException e) {
            fail(this.getClass().getSimpleName() + " failed with exception: " + e);
        }
    }

    @Test
    public void shouldAggregatePropertiesOnlyWhenGroupByIsSetToCQ1CQ2InByteEntityStore() throws StoreException {
        shouldAggregatePropertiesOnlyWhenGroupByIsSetToCQ1CQ2(byteEntityStore, byteEntityElementConverter);
    }

    @Test
    public void shouldAggregatePropertiesOnlyWhenGroupByIsSetToCQ1CQ2InGafferOneStore() throws StoreException {
        shouldAggregatePropertiesOnlyWhenGroupByIsSetToCQ1CQ2(gaffer1KeyStore, gaffer1ElementConverter);
    }

    public void shouldAggregatePropertiesOnlyWhenGroupByIsSetToCQ1CQ2(final AccumuloStore store, final AccumuloElementConverter elementConverter) throws StoreException {
        final String visibilityString = "public";
        try {
            // Create edge
            final Edge edge = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("1")
                    .dest("2")
                    .directed(true)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 1)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER_2, 1)
                    .property(AccumuloPropertyNames.PROP_1, 0)
                    .property(AccumuloPropertyNames.PROP_2, 0)
                    .property(AccumuloPropertyNames.PROP_3, 0)
                    .property(AccumuloPropertyNames.PROP_4, 0)
                    .property(AccumuloPropertyNames.COUNT, 1)
                    .build();

            final Edge edge2 = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("1")
                    .dest("2")
                    .directed(true)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 1)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER_2, 1)
                    .property(AccumuloPropertyNames.PROP_1, 0)
                    .property(AccumuloPropertyNames.PROP_2, 0)
                    .property(AccumuloPropertyNames.PROP_3, 0)
                    .property(AccumuloPropertyNames.PROP_4, 0)
                    .property(AccumuloPropertyNames.COUNT, 1)
                    .build();

            final Edge edge3 = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("1")
                    .dest("2")
                    .directed(true)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 1)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER_2, 1)
                    .property(AccumuloPropertyNames.PROP_1, 0)
                    .property(AccumuloPropertyNames.PROP_2, 0)
                    .property(AccumuloPropertyNames.PROP_3, 0)
                    .property(AccumuloPropertyNames.PROP_4, 0)
                    .property(AccumuloPropertyNames.COUNT, 1)
                    .build();

            //THIS EDGE WILL BE REDUCED MEANING ITS CQ (columnQualifier) will only occur once because its key is equal.
            final Edge edge4 = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("1")
                    .dest("2")
                    .directed(true)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 1)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER_2, 4)
                    .property(AccumuloPropertyNames.PROP_1, 0)
                    .property(AccumuloPropertyNames.PROP_2, 0)
                    .property(AccumuloPropertyNames.PROP_3, 0)
                    .property(AccumuloPropertyNames.PROP_4, 0)
                    .property(AccumuloPropertyNames.COUNT, 2)
                    .build();

            final Edge edge5 = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("1")
                    .dest("2")
                    .directed(true)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 3)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER_2, 5)
                    .property(AccumuloPropertyNames.PROP_1, 0)
                    .property(AccumuloPropertyNames.PROP_2, 0)
                    .property(AccumuloPropertyNames.PROP_3, 0)
                    .property(AccumuloPropertyNames.PROP_4, 0)
                    .property(AccumuloPropertyNames.COUNT, 10)
                    .build();

            final Edge edge6 = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("1")
                    .dest("2")
                    .directed(true)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 3)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER_2, 5)
                    .property(AccumuloPropertyNames.PROP_1, 0)
                    .property(AccumuloPropertyNames.PROP_2, 0)
                    .property(AccumuloPropertyNames.PROP_3, 0)
                    .property(AccumuloPropertyNames.PROP_4, 0)
                    .property(AccumuloPropertyNames.COUNT, 5)
                    .build();

            // Accumulo key
            final Key key = elementConverter.getKeysFromEdge(edge).getFirst();
            final Key key2 = elementConverter.getKeysFromEdge(edge2).getFirst();
            final Key key3 = elementConverter.getKeysFromEdge(edge3).getFirst();
            final Key key4 = elementConverter.getKeysFromEdge(edge4).getFirst();
            final Key key5 = elementConverter.getKeysFromEdge(edge5).getFirst();
            final Key key6 = elementConverter.getKeysFromEdge(edge6).getFirst();

            // Accumulo values
            final Value value1 = elementConverter.getValueFromProperties(TestGroups.EDGE, edge.getProperties());
            final Value value2 = elementConverter.getValueFromProperties(TestGroups.EDGE, edge2.getProperties());
            final Value value3 = elementConverter.getValueFromProperties(TestGroups.EDGE, edge3.getProperties());
            final Value value4 = elementConverter.getValueFromProperties(TestGroups.EDGE, edge4.getProperties());
            final Value value5 = elementConverter.getValueFromProperties(TestGroups.EDGE, edge5.getProperties());
            final Value value6 = elementConverter.getValueFromProperties(TestGroups.EDGE, edge6.getProperties());

            // Create mutation
            final Mutation m1 = new Mutation(key.getRow());
            m1.put(key.getColumnFamily(), key.getColumnQualifier(), new ColumnVisibility(key.getColumnVisibility()), key.getTimestamp(), value1);
            final Mutation m2 = new Mutation(key2.getRow());
            m2.put(key2.getColumnFamily(), key2.getColumnQualifier(), new ColumnVisibility(key2.getColumnVisibility()), key2.getTimestamp(), value2);
            final Mutation m3 = new Mutation(key.getRow());
            m3.put(key3.getColumnFamily(), key3.getColumnQualifier(), new ColumnVisibility(key3.getColumnVisibility()), key3.getTimestamp(), value3);
            final Mutation m4 = new Mutation(key.getRow());
            m4.put(key4.getColumnFamily(), key4.getColumnQualifier(), new ColumnVisibility(key4.getColumnVisibility()), key4.getTimestamp(), value4);
            final Mutation m5 = new Mutation(key.getRow());
            m5.put(key5.getColumnFamily(), key5.getColumnQualifier(), new ColumnVisibility(key5.getColumnVisibility()), key5.getTimestamp(), value5);
            final Mutation m6 = new Mutation(key.getRow());
            m6.put(key6.getColumnFamily(), key6.getColumnQualifier(), new ColumnVisibility(key6.getColumnVisibility()), key6.getTimestamp(), value6);

            // Write mutation
            final BatchWriterConfig writerConfig = new BatchWriterConfig();
            writerConfig.setMaxMemory(1000000L);
            writerConfig.setMaxLatency(1000L, TimeUnit.MILLISECONDS);
            writerConfig.setMaxWriteThreads(1);
            final BatchWriter writer = store.getConnection().createBatchWriter(store.getTableName(), writerConfig);
            writer.addMutation(m1);
            writer.addMutation(m2);
            writer.addMutation(m3);
            writer.addMutation(m4);
            writer.addMutation(m5);
            writer.addMutation(m6);
            writer.close();

            Edge expectedEdge1 = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("1")
                    .dest("2")
                    .directed(true)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 1)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER_2, 1)
                    .property(AccumuloPropertyNames.COUNT, 3)
                    .property(AccumuloPropertyNames.PROP_1, 0)
                    .property(AccumuloPropertyNames.PROP_2, 0)
                    .property(AccumuloPropertyNames.PROP_3, 0)
                    .property(AccumuloPropertyNames.PROP_4, 0)
                    .build();

            Edge expectedEdge2 = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("1")
                    .dest("2")
                    .directed(true)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 1)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER_2, 4)
                    .property(AccumuloPropertyNames.COUNT, 2)
                    .property(AccumuloPropertyNames.PROP_1, 0)
                    .property(AccumuloPropertyNames.PROP_2, 0)
                    .property(AccumuloPropertyNames.PROP_3, 0)
                    .property(AccumuloPropertyNames.PROP_4, 0)
                    .build();

            Edge expectedEdge3 = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("1")
                    .dest("2")
                    .directed(true)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 3)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER_2, 5)
                    .property(AccumuloPropertyNames.COUNT, 15)
                    .property(AccumuloPropertyNames.PROP_1, 0)
                    .property(AccumuloPropertyNames.PROP_2, 0)
                    .property(AccumuloPropertyNames.PROP_3, 0)
                    .property(AccumuloPropertyNames.PROP_4, 0)
                    .build();

            // Read data back and check we get one merged element
            final Authorizations authorizations = new Authorizations(visibilityString);
            final Scanner scanner = store.getConnection().createScanner(store.getTableName(), authorizations);
            final IteratorSetting iteratorSetting = new IteratorSettingBuilder(AccumuloStoreConstants.COLUMN_QUALIFIER_AGGREGATOR_ITERATOR_PRIORITY,
                    "KeyCombiner", CoreKeyGroupByAggregatorIterator.class)
                    .all()
                    .view(new View.Builder()
                            .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                                    .groupBy(AccumuloPropertyNames.COLUMN_QUALIFIER, AccumuloPropertyNames.COLUMN_QUALIFIER_2)
                                    .build())
                            .build())
                    .schema(store.getSchema())
                    .keyConverter(store.getKeyPackage().getKeyConverter())
                    .build();
            scanner.addScanIterator(iteratorSetting);
            final Iterator<Entry<Key, Value>> it = scanner.iterator();
            Entry<Key, Value> entry = it.next();
            Element readEdge = elementConverter.getFullElement(entry.getKey(), entry.getValue(), false);
            assertEquals(expectedEdge1, readEdge);
            assertEquals(1, readEdge.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER));
            assertEquals(1, readEdge.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER_2));
            assertEquals(3, readEdge.getProperty(AccumuloPropertyNames.COUNT));
            entry = it.next();
            readEdge = elementConverter.getFullElement(entry.getKey(), entry.getValue(), false);
            assertEquals(expectedEdge2, readEdge);
            assertEquals(1, readEdge.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER));
            assertEquals(4, readEdge.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER_2));
            assertEquals(2, readEdge.getProperty(AccumuloPropertyNames.COUNT));
            entry = it.next();
            readEdge = elementConverter.getFullElement(entry.getKey(), entry.getValue(), false);
            assertEquals(expectedEdge3, readEdge);
            assertEquals(3, readEdge.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER));
            assertEquals(5, readEdge.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER_2));
            assertEquals(15, readEdge.getProperty(AccumuloPropertyNames.COUNT));

            if (it.hasNext()) {
                fail("Additional row found.");
            }

        } catch (final AccumuloException | TableNotFoundException e) {
            fail(this.getClass().getSimpleName() + " failed with exception: " + e);
        }
    }

    @Test
    public void shouldAggregateEverythingWhenGroupByIsSetToBlankInByteEntityStore() throws StoreException {
        shouldAggregateEverythingWhenGroupByIsSetToBlank(byteEntityStore, byteEntityElementConverter);
    }

    @Test
    public void shouldAggregateEverythingWhenGroupByIsSetToBlankInGafferOneStore() throws StoreException {
        shouldAggregateEverythingWhenGroupByIsSetToBlank(gaffer1KeyStore, gaffer1ElementConverter);
    }

    public void shouldAggregateEverythingWhenGroupByIsSetToBlank(final AccumuloStore store, final AccumuloElementConverter elementConverter) throws StoreException {
        final String visibilityString = "public";
        try {
            // Create edge
            final Edge edge = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("1")
                    .dest("2")
                    .directed(true)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 1)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER_2, 1)
                    .property(AccumuloPropertyNames.PROP_1, 0)
                    .property(AccumuloPropertyNames.PROP_2, 0)
                    .property(AccumuloPropertyNames.PROP_3, 0)
                    .property(AccumuloPropertyNames.PROP_4, 0)
                    .property(AccumuloPropertyNames.COUNT, 1)
                    .build();

            final Edge edge2 = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("1")
                    .dest("2")
                    .directed(true)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 1)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER_2, 1)
                    .property(AccumuloPropertyNames.PROP_1, 0)
                    .property(AccumuloPropertyNames.PROP_2, 0)
                    .property(AccumuloPropertyNames.PROP_3, 0)
                    .property(AccumuloPropertyNames.PROP_4, 0)
                    .property(AccumuloPropertyNames.COUNT, 1)
                    .build();

            final Edge edge3 = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("1")
                    .dest("2")
                    .directed(true)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 1)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER_2, 1)
                    .property(AccumuloPropertyNames.PROP_1, 0)
                    .property(AccumuloPropertyNames.PROP_2, 0)
                    .property(AccumuloPropertyNames.PROP_3, 0)
                    .property(AccumuloPropertyNames.PROP_4, 0)
                    .property(AccumuloPropertyNames.COUNT, 1)
                    .build();


            //THIS EDGE WILL BE REDUCED MEANING ITS CQ (columnQualifier) will only occur once because its key is equal.
            final Edge edge4 = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("1")
                    .dest("2")
                    .directed(true)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 1)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER_2, 4)
                    .property(AccumuloPropertyNames.PROP_1, 0)
                    .property(AccumuloPropertyNames.PROP_2, 0)
                    .property(AccumuloPropertyNames.PROP_3, 0)
                    .property(AccumuloPropertyNames.PROP_4, 0)
                    .property(AccumuloPropertyNames.COUNT, 2)
                    .build();

            final Edge edge5 = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("1")
                    .dest("2")
                    .directed(true)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 3)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER_2, 5)
                    .property(AccumuloPropertyNames.PROP_1, 0)
                    .property(AccumuloPropertyNames.PROP_2, 0)
                    .property(AccumuloPropertyNames.PROP_3, 0)
                    .property(AccumuloPropertyNames.PROP_4, 0)
                    .property(AccumuloPropertyNames.COUNT, 10)
                    .build();

            final Edge edge6 = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("1")
                    .dest("2")
                    .directed(true)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 3)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER_2, 5)
                    .property(AccumuloPropertyNames.PROP_1, 0)
                    .property(AccumuloPropertyNames.PROP_2, 0)
                    .property(AccumuloPropertyNames.PROP_3, 0)
                    .property(AccumuloPropertyNames.PROP_4, 0)
                    .property(AccumuloPropertyNames.COUNT, 5)
                    .build();

            // Accumulo key
            final Key key = elementConverter.getKeysFromEdge(edge).getFirst();
            final Key key2 = elementConverter.getKeysFromEdge(edge2).getFirst();
            final Key key3 = elementConverter.getKeysFromEdge(edge3).getFirst();
            final Key key4 = elementConverter.getKeysFromEdge(edge4).getFirst();
            final Key key5 = elementConverter.getKeysFromEdge(edge5).getFirst();
            final Key key6 = elementConverter.getKeysFromEdge(edge6).getFirst();

            // Accumulo values
            final Value value1 = elementConverter.getValueFromProperties(TestGroups.EDGE, edge.getProperties());
            final Value value2 = elementConverter.getValueFromProperties(TestGroups.EDGE, edge2.getProperties());
            final Value value3 = elementConverter.getValueFromProperties(TestGroups.EDGE, edge3.getProperties());
            final Value value4 = elementConverter.getValueFromProperties(TestGroups.EDGE, edge4.getProperties());
            final Value value5 = elementConverter.getValueFromProperties(TestGroups.EDGE, edge5.getProperties());
            final Value value6 = elementConverter.getValueFromProperties(TestGroups.EDGE, edge6.getProperties());

            // Create mutation
            final Mutation m1 = new Mutation(key.getRow());
            m1.put(key.getColumnFamily(), key.getColumnQualifier(), new ColumnVisibility(key.getColumnVisibility()), key.getTimestamp(), value1);
            final Mutation m2 = new Mutation(key2.getRow());
            m2.put(key2.getColumnFamily(), key2.getColumnQualifier(), new ColumnVisibility(key2.getColumnVisibility()), key2.getTimestamp(), value2);
            final Mutation m3 = new Mutation(key.getRow());
            m3.put(key3.getColumnFamily(), key3.getColumnQualifier(), new ColumnVisibility(key3.getColumnVisibility()), key3.getTimestamp(), value3);
            final Mutation m4 = new Mutation(key.getRow());
            m4.put(key4.getColumnFamily(), key4.getColumnQualifier(), new ColumnVisibility(key4.getColumnVisibility()), key4.getTimestamp(), value4);
            final Mutation m5 = new Mutation(key.getRow());
            m5.put(key5.getColumnFamily(), key5.getColumnQualifier(), new ColumnVisibility(key5.getColumnVisibility()), key5.getTimestamp(), value5);
            final Mutation m6 = new Mutation(key.getRow());
            m6.put(key6.getColumnFamily(), key6.getColumnQualifier(), new ColumnVisibility(key6.getColumnVisibility()), key6.getTimestamp(), value6);

            // Write mutation
            final BatchWriterConfig writerConfig = new BatchWriterConfig();
            writerConfig.setMaxMemory(1000000L);
            writerConfig.setMaxLatency(1000L, TimeUnit.MILLISECONDS);
            writerConfig.setMaxWriteThreads(1);
            final BatchWriter writer = store.getConnection().createBatchWriter(store.getTableName(), writerConfig);
            writer.addMutation(m1);
            writer.addMutation(m2);
            writer.addMutation(m3);
            writer.addMutation(m4);
            writer.addMutation(m5);
            writer.addMutation(m6);
            writer.close();

            Edge expectedEdge1 = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("1")
                    .dest("2")
                    .directed(true)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 5)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER_2, 10)
                    .property(AccumuloPropertyNames.COUNT, 20)
                    .property(AccumuloPropertyNames.PROP_1, 0)
                    .property(AccumuloPropertyNames.PROP_2, 0)
                    .property(AccumuloPropertyNames.PROP_3, 0)
                    .property(AccumuloPropertyNames.PROP_4, 0)
                    .build();

            // Read data back and check we get one merged element
            final Authorizations authorizations = new Authorizations(visibilityString);
            final Scanner scanner = store.getConnection().createScanner(store.getTableName(), authorizations);
            final IteratorSetting iteratorSetting = new IteratorSettingBuilder(AccumuloStoreConstants.COLUMN_QUALIFIER_AGGREGATOR_ITERATOR_PRIORITY,
                    "KeyCombiner", CoreKeyGroupByAggregatorIterator.class)
                    .all()
                    .view(new View.Builder()
                            .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                                    .groupBy()
                                    .build())
                            .build())
                    .schema(store.getSchema())
                    .keyConverter(store.getKeyPackage().getKeyConverter())
                    .build();
            scanner.addScanIterator(iteratorSetting);
            final Iterator<Entry<Key, Value>> it = scanner.iterator();
            Entry<Key, Value> entry = it.next();
            Element readEdge = elementConverter.getFullElement(entry.getKey(), entry.getValue(), false);
            assertEquals(expectedEdge1, readEdge);
            assertEquals(5, readEdge.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER));
            assertEquals(10, readEdge.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER_2));
            assertEquals(20, readEdge.getProperty(AccumuloPropertyNames.COUNT));

            if (it.hasNext()) {
                fail("Additional row found.");
            }

        } catch (final AccumuloException | TableNotFoundException e) {
            fail(this.getClass().getSimpleName() + " failed with exception: " + e);
        }
    }
}
