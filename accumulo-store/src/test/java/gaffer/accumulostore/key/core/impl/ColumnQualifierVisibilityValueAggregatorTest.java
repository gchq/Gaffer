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
package gaffer.accumulostore.key.core.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import gaffer.accumulostore.MockAccumuloStore;
import gaffer.accumulostore.MockAccumuloStoreForTest;
import gaffer.accumulostore.key.AccumuloElementConverter;
import gaffer.accumulostore.key.core.impl.byteEntity.ByteEntityAccumuloElementConverter;
import gaffer.accumulostore.key.core.impl.byteEntity.ByteEntityKeyPackage;
import gaffer.accumulostore.key.core.impl.classic.ClassicAccumuloElementConverter;
import gaffer.accumulostore.key.core.impl.classic.ClassicKeyPackage;
import gaffer.accumulostore.key.exception.AccumuloElementConversionException;
import gaffer.accumulostore.utils.AccumuloPropertyNames;
import gaffer.accumulostore.utils.AccumuloStoreConstants;
import gaffer.accumulostore.utils.IteratorSettingBuilder;
import gaffer.accumulostore.utils.TableUtils;
import gaffer.commonutil.TestGroups;
import gaffer.data.element.Edge;
import gaffer.data.element.Element;
import gaffer.data.element.Properties;
import gaffer.store.StoreException;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
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
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

public class ColumnQualifierVisibilityValueAggregatorTest {
    private MockAccumuloStore byteEntityStore;
    private MockAccumuloStore gaffer1KeyStore;
    private AccumuloElementConverter byteEntityElementConverter;
    private AccumuloElementConverter gaffer1ElementConverter;

    @Before
    public void setup() throws StoreException, AccumuloException, AccumuloSecurityException, IOException {
        byteEntityStore = new MockAccumuloStoreForTest(ByteEntityKeyPackage.class);
        gaffer1KeyStore = new MockAccumuloStoreForTest(ClassicKeyPackage.class);

        byteEntityStore.getProperties().setTable("Test");
        gaffer1KeyStore.getProperties().setTable("Test2");

        gaffer1ElementConverter = new ClassicAccumuloElementConverter(gaffer1KeyStore.getSchema());
        byteEntityElementConverter = new ByteEntityAccumuloElementConverter(byteEntityStore.getSchema());
    }

    @Test
    public void shouldMultiplePropertySetsAggregateInByteEntityStore() throws StoreException, AccumuloElementConversionException {
        testAggregatingMultiplePropertySets(byteEntityStore, byteEntityElementConverter);
    }

    @Test
    public void shouldMultiplePropertySetsAggregateInGafferOneStore() throws StoreException, AccumuloElementConversionException {
        testAggregatingMultiplePropertySets(gaffer1KeyStore, gaffer1ElementConverter);
    }

    private void testAggregatingMultiplePropertySets(final MockAccumuloStore store, final AccumuloElementConverter elementConverter) throws StoreException, AccumuloElementConversionException {
        String visibilityString = "public";
        try {
            // Create table
            // (this method creates the table, removes the versioning iterator, and adds the SetOfStatisticsCombiner iterator).
            TableUtils.createTable(store);

            final Properties properties1 = new Properties();
            properties1.put(AccumuloPropertyNames.COUNT, 1);

            final Properties properties2 = new Properties();
            properties2.put(AccumuloPropertyNames.COUNT, 2);

            final Properties properties3 = new Properties();
            properties3.put(AccumuloPropertyNames.COUNT, 10);

            // Create edge
            final Edge edge = new Edge(TestGroups.EDGE);
            edge.setSource("1");
            edge.setDestination("2");
            edge.setDirected(true);
            edge.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 8);
            edge.putProperty(AccumuloPropertyNames.PROP_1, 0);
            edge.putProperty(AccumuloPropertyNames.PROP_2, 0);
            edge.putProperty(AccumuloPropertyNames.PROP_3, 3);
            edge.putProperty(AccumuloPropertyNames.PROP_4, 0);
            edge.putProperty(AccumuloPropertyNames.COUNT, 15);

            //THIS EDGE WILL BE REDUCED MEANING ITS CQ (columnQualifier) will only occur once because its key is equal.
            final Edge edge2 = new Edge(TestGroups.EDGE);
            edge2.setSource("1");
            edge2.setDestination("2");
            edge2.setDirected(true);
            edge2.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 1);
            edge2.putProperty(AccumuloPropertyNames.PROP_1, 0);
            edge2.putProperty(AccumuloPropertyNames.PROP_2, 0);
            edge2.putProperty(AccumuloPropertyNames.PROP_3, 0);
            edge2.putProperty(AccumuloPropertyNames.PROP_4, 0);

            final Edge edge3 = new Edge(TestGroups.EDGE);
            edge3.setSource("1");
            edge3.setDestination("2");
            edge3.setDirected(true);
            edge3.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 1);
            edge3.putProperty(AccumuloPropertyNames.PROP_1, 0);
            edge3.putProperty(AccumuloPropertyNames.PROP_2, 0);
            edge3.putProperty(AccumuloPropertyNames.PROP_3, 0);
            edge3.putProperty(AccumuloPropertyNames.PROP_4, 0);


            // Accumulo key
            final Key key = elementConverter.getKeysFromEdge(edge).getFirst();
            final Key key2 = elementConverter.getKeysFromEdge(edge2).getFirst();
            final Key key3 = elementConverter.getKeysFromEdge(edge3).getFirst();

            // Accumulo values
            final Value value1 = elementConverter.getValueFromProperties(properties1, TestGroups.EDGE);
            final Value value2 = elementConverter.getValueFromProperties(properties2, TestGroups.EDGE);
            final Value value3 = elementConverter.getValueFromProperties(properties3, TestGroups.EDGE);

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
            final BatchWriter writer = store.getMockConnector().createBatchWriter(store.getProperties().getTable(), writerConfig);
            writer.addMutation(m1);
            writer.addMutation(m2);
            writer.addMutation(m3);
            writer.addMutation(m4);
            writer.addMutation(m5);
            writer.close();

            // Read data back and check we get one merged element
            final Authorizations authorizations = new Authorizations(visibilityString);
            final Scanner scanner = store.getMockConnector().createScanner(store.getProperties().getTable(), authorizations);
            final IteratorSetting iteratorSetting = new IteratorSettingBuilder(AccumuloStoreConstants.QUERY_TIME_AGGREGATOR_PRIORITY,
                    "KeyCombiner", CoreKeyColumnQualifierVisibilityValueAggregatorIterator.class)
                    .all()
                    .schema(store.getSchema())
                    .keyConverter(store.getKeyPackage().getKeyConverter())
                    .build();
            scanner.addScanIterator(iteratorSetting);
            final Iterator<Entry<Key, Value>> it = scanner.iterator();
            final Entry<Key, Value> entry = it.next();
            final Element readEdge = elementConverter.getFullElement(entry.getKey(), entry.getValue());

            final Edge expectedEdge = new Edge(TestGroups.EDGE);
            expectedEdge.setSource("1");
            expectedEdge .setDestination("2");
            expectedEdge.setDirected(true);
            expectedEdge.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 9);
            expectedEdge.putProperty(AccumuloPropertyNames.PROP_1, 0);
            expectedEdge.putProperty(AccumuloPropertyNames.PROP_2, 0);
            expectedEdge.putProperty(AccumuloPropertyNames.PROP_3, 0);
            expectedEdge.putProperty(AccumuloPropertyNames.PROP_4, 0);
            expectedEdge.putProperty(AccumuloPropertyNames.COUNT, 15);

            assertEquals(readEdge, expectedEdge);
            assertEquals(9, readEdge.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER));
            assertEquals(15, readEdge.getProperty(AccumuloPropertyNames.COUNT));
            // Check no more entries
            if (it.hasNext()) {
                fail("Additional row found.");
            }

        } catch (AccumuloException | TableExistsException | TableNotFoundException e) {
            fail(this.getClass().getSimpleName() + " failed with exception: " + e);
        }
    }

    @Test
    public void shouldSinglePropertySetAggregateInByteEntityStore() throws StoreException, AccumuloElementConversionException {
        testAggregatingSinglePropertySet(byteEntityStore, byteEntityElementConverter);
    }

    @Test
    public void shouldSinglePropertySetAggregateInGafferOneStore() throws StoreException, AccumuloElementConversionException {
        testAggregatingSinglePropertySet(gaffer1KeyStore, gaffer1ElementConverter);
    }

    public void testAggregatingSinglePropertySet(final MockAccumuloStore store, final AccumuloElementConverter elementConverter) throws StoreException, AccumuloElementConversionException {
        String visibilityString = "public";
        try {
            // Create table
            // (this method creates the table, removes the versioning iterator, and adds the SetOfStatisticsCombiner iterator).
            TableUtils.createTable(store);

            // Create edge
            final Edge edge = new Edge(TestGroups.EDGE);
            edge.setSource("1");
            edge.setDestination("2");
            edge.setDirected(true);
            edge.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 8);
            edge.putProperty(AccumuloPropertyNames.COUNT, 1);

            final Properties properties1 = new Properties();
            properties1.put(AccumuloPropertyNames.COUNT, 1);

            // Accumulo key
            final Key key = elementConverter.getKeysFromEdge(edge).getFirst();

            // Accumulo values
            final Value value1 = elementConverter.getValueFromProperties(properties1, TestGroups.EDGE);

            // Create mutation
            final Mutation m1 = new Mutation(key.getRow());
            m1.put(key.getColumnFamily(), key.getColumnQualifier(), new ColumnVisibility(key.getColumnVisibility()), key.getTimestamp(), value1);

            // Write mutation
            final BatchWriterConfig writerConfig = new BatchWriterConfig();
            writerConfig.setMaxMemory(1000000L);
            writerConfig.setMaxLatency(1000L, TimeUnit.MILLISECONDS);
            writerConfig.setMaxWriteThreads(1);
            final BatchWriter writer = store.getMockConnector().createBatchWriter(store.getProperties().getTable(), writerConfig);
            writer.addMutation(m1);
            writer.close();

            // Read data back and check we get one merged element
            final Authorizations authorizations = new Authorizations(visibilityString);
            final Scanner scanner = store.getMockConnector().createScanner(store.getProperties().getTable(), authorizations);
            final IteratorSetting iteratorSetting = new IteratorSettingBuilder(AccumuloStoreConstants.QUERY_TIME_AGGREGATOR_PRIORITY,
                    "KeyCombiner", CoreKeyColumnQualifierVisibilityValueAggregatorIterator.class)
                    .all()
                    .schema(store.getSchema())
                    .keyConverter(store.getKeyPackage().getKeyConverter())
                    .build();
            scanner.addScanIterator(iteratorSetting);
            final Iterator<Entry<Key, Value>> it = scanner.iterator();
            final Entry<Key, Value> entry = it.next();
            final Element readEdge = elementConverter.getFullElement(entry.getKey(), entry.getValue());
            assertEquals(readEdge, edge);
            assertEquals(8, readEdge.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER));
            assertEquals(1, readEdge.getProperty(AccumuloPropertyNames.COUNT));
            // Check no more entries
            if (it.hasNext()) {
                fail("Additional row found.");
            }

        } catch (AccumuloException | TableExistsException | TableNotFoundException e) {
            fail(this.getClass().getSimpleName() + " failed with exception: " + e);
        }
    }

    @Test
    public void shouldEmptyColumnQualifierAggregateInByteEntityStore() throws StoreException, AccumuloElementConversionException {
        testAggregatingEmptyColumnQualifier(byteEntityStore, byteEntityElementConverter);
    }

    @Test
    public void shouldEmptyColumnQualifierAggregateInGafferOneStore() throws StoreException, AccumuloElementConversionException {
        testAggregatingEmptyColumnQualifier(gaffer1KeyStore, gaffer1ElementConverter);
    }

    public void testAggregatingEmptyColumnQualifier(final MockAccumuloStore store, final AccumuloElementConverter elementConverter) throws StoreException, AccumuloElementConversionException {
        final String visibilityString = "public";
        try {
            // Create table
            // (this method creates the table, removes the versioning iterator, and adds the SetOfStatisticsCombiner iterator).
            TableUtils.createTable(store);

            // Create edge
            final Edge edge = new Edge(TestGroups.EDGE);
            edge.setSource("1");
            edge.setDestination("2");
            edge.setDirected(true);
            edge.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 8);
            edge.putProperty(AccumuloPropertyNames.PROP_1, 0);
            edge.putProperty(AccumuloPropertyNames.PROP_2, 0);
            edge.putProperty(AccumuloPropertyNames.PROP_3, 0);
            edge.putProperty(AccumuloPropertyNames.PROP_4, 0);
            edge.putProperty(AccumuloPropertyNames.COUNT, 15);

            //THIS EDGE WILL BE REDUCED MEANING ITS CQ (columnQualifier) will only occur once because its key is equal.
            final Edge edge2 = new Edge(TestGroups.EDGE);
            edge2.setSource("1");
            edge2.setDestination("2");
            edge2.setDirected(true);
            edge2.putProperty(AccumuloPropertyNames.PROP_1, 0);
            edge2.putProperty(AccumuloPropertyNames.PROP_2, 0);
            edge2.putProperty(AccumuloPropertyNames.PROP_3, 0);
            edge2.putProperty(AccumuloPropertyNames.PROP_4, 0);

            final Edge edge3 = new Edge(TestGroups.EDGE);
            edge3.setSource("1");
            edge3.setDestination("2");
            edge3.setDirected(true);
            edge3.putProperty(AccumuloPropertyNames.PROP_1, 0);
            edge3.putProperty(AccumuloPropertyNames.PROP_2, 0);
            edge3.putProperty(AccumuloPropertyNames.PROP_3, 0);
            edge3.putProperty(AccumuloPropertyNames.PROP_4, 0);

            final Properties properties1 = new Properties();
            properties1.put(AccumuloPropertyNames.COUNT, 1);

            final Properties properties2 = new Properties();
            properties2.put(AccumuloPropertyNames.COUNT, 2);

            final Properties properties3 = new Properties();
            properties3.put(AccumuloPropertyNames.COUNT, 10);

            // Accumulo key
            final Key key = elementConverter.getKeysFromEdge(edge).getFirst();
            final Key key2 = elementConverter.getKeysFromEdge(edge2).getFirst();
            final Key key3 = elementConverter.getKeysFromEdge(edge3).getFirst();

            // Accumulo values
            final Value value1 = elementConverter.getValueFromProperties(properties1, TestGroups.EDGE);
            final Value value2 = elementConverter.getValueFromProperties(properties2, TestGroups.EDGE);
            final Value value3 = elementConverter.getValueFromProperties(properties3, TestGroups.EDGE);

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
            final BatchWriter writer = store.getMockConnector().createBatchWriter(store.getProperties().getTable(), writerConfig);
            writer.addMutation(m1);
            writer.addMutation(m2);
            writer.addMutation(m3);
            writer.addMutation(m4);
            writer.addMutation(m5);
            writer.close();

            // Read data back and check we get one merged element
            final Authorizations authorizations = new Authorizations(visibilityString);
            final Scanner scanner = store.getMockConnector().createScanner(store.getProperties().getTable(), authorizations);
            final IteratorSetting iteratorSetting = new IteratorSettingBuilder(AccumuloStoreConstants.QUERY_TIME_AGGREGATOR_PRIORITY,
                    "KeyCombiner", CoreKeyColumnQualifierVisibilityValueAggregatorIterator.class)
                    .all()
                    .schema(store.getSchema())
                    .keyConverter(store.getKeyPackage().getKeyConverter())
                    .build();
            scanner.addScanIterator(iteratorSetting);
            final Iterator<Entry<Key, Value>> it = scanner.iterator();
            final Entry<Key, Value> entry = it.next();
            final Element readEdge = elementConverter.getFullElement(entry.getKey(), entry.getValue());
            assertEquals(readEdge, edge);
            assertEquals(8, readEdge.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER));
            assertEquals(15, readEdge.getProperty(AccumuloPropertyNames.COUNT));
            // Check no more entries
            if (it.hasNext()) {
                fail("Additional row found.");
            }

        } catch (AccumuloException | TableExistsException | TableNotFoundException e) {
            fail(this.getClass().getSimpleName() + " failed with exception: " + e);
        }
    }


}