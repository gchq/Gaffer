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

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

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

        gaffer1ElementConverter = new ClassicAccumuloElementConverter(gaffer1KeyStore.getStoreSchema());
        byteEntityElementConverter = new ByteEntityAccumuloElementConverter(byteEntityStore.getStoreSchema());
    }

    @Test
    public void testAggregatingMultiplePropertySets() throws StoreException, AccumuloElementConversionException {
        testAggregatingMultiplePropertySets(byteEntityStore, byteEntityElementConverter);
        testAggregatingMultiplePropertySets(gaffer1KeyStore, gaffer1ElementConverter);
    }

    public void testAggregatingMultiplePropertySets(final MockAccumuloStore store, final AccumuloElementConverter elementConverter) throws StoreException, AccumuloElementConversionException {
        String visibilityString = "public";
        try {
            // Create table
            // (this method creates the table, removes the versioning iterator, and adds the SetOfStatisticsCombiner iterator).
            TableUtils.createTable(store);

            // Create edge
            Edge edge = new Edge(TestGroups.EDGE);
            edge.setSource("1");
            edge.setDestination("2");
            edge.setDirected(true);
            edge.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 8);

            //THIS EDGE WILL BE REDUCED MEANING ITS CQ (columnQualifier) will only occur once because its key is equal.
            Edge edge2 = new Edge(TestGroups.EDGE);
            edge2.setSource("1");
            edge2.setDestination("2");
            edge2.setDirected(true);
            edge2.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 1);

            Edge edge3 = new Edge(TestGroups.EDGE);
            edge3.setSource("1");
            edge3.setDestination("2");
            edge3.setDirected(true);
            edge3.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 1);

            Properties properties1 = new Properties();
            properties1.put(AccumuloPropertyNames.COUNT, 1);

            Properties properties2 = new Properties();
            properties2.put(AccumuloPropertyNames.COUNT, 2);

            Properties properties3 = new Properties();
            properties3.put(AccumuloPropertyNames.COUNT, 10);

            // Accumulo key
            Key key = elementConverter.getKeysFromEdge(edge).getFirst();
            Key key2 = elementConverter.getKeysFromEdge(edge2).getFirst();
            Key key3 = elementConverter.getKeysFromEdge(edge3).getFirst();

            // Accumulo values
            Value value1 = elementConverter.getValueFromProperties(properties1, TestGroups.EDGE);
            Value value2 = elementConverter.getValueFromProperties(properties2, TestGroups.EDGE);
            Value value3 = elementConverter.getValueFromProperties(properties3, TestGroups.EDGE);

            // Create mutation
            Mutation m1 = new Mutation(key.getRow());
            m1.put(key.getColumnFamily(), key.getColumnQualifier(), new ColumnVisibility(key.getColumnVisibility()), key.getTimestamp(), value1);
            Mutation m2 = new Mutation(key.getRow());
            m2.put(key.getColumnFamily(), key.getColumnQualifier(), new ColumnVisibility(key.getColumnVisibility()), key.getTimestamp(), value2);
            Mutation m3 = new Mutation(key.getRow());
            m3.put(key.getColumnFamily(), key.getColumnQualifier(), new ColumnVisibility(key.getColumnVisibility()), key.getTimestamp(), value3);
            Mutation m4 = new Mutation(key2.getRow());
            m4.put(key2.getColumnFamily(), key2.getColumnQualifier(), new ColumnVisibility(key2.getColumnVisibility()), key2.getTimestamp(), value1);
            Mutation m5 = new Mutation(key.getRow());
            m5.put(key3.getColumnFamily(), key3.getColumnQualifier(), new ColumnVisibility(key3.getColumnVisibility()), key3.getTimestamp(), value1);

            // Write mutation
            BatchWriterConfig writerConfig = new BatchWriterConfig();
            writerConfig.setMaxMemory(1000000L);
            writerConfig.setMaxLatency(1000L, TimeUnit.MILLISECONDS);
            writerConfig.setMaxWriteThreads(1);
            BatchWriter writer = store.getMockConnector().createBatchWriter(store.getProperties().getTable(), writerConfig);
            writer.addMutation(m1);
            writer.addMutation(m2);
            writer.addMutation(m3);
            writer.addMutation(m4);
            writer.addMutation(m5);
            writer.close();

            // Read data back and check we get one merged element
            Authorizations authorizations = new Authorizations(visibilityString);
            Scanner scanner = store.getMockConnector().createScanner(store.getProperties().getTable(), authorizations);
            IteratorSetting iteratorSetting = new IteratorSettingBuilder(AccumuloStoreConstants.QUERY_TIME_AGGREGATOR_PRIORITY,
                    "KeyCombiner", CoreKeyColumnQualifierVisibilityValueAggregatorIterator.class)
                    .all()
                    .dataSchema(store.getDataSchema())
                    .storeSchema(store.getStoreSchema())
                    .keyConverter(store.getKeyPackage().getKeyConverter())
                    .build();
            scanner.addScanIterator(iteratorSetting);
            Iterator<Entry<Key, Value>> it = scanner.iterator();
            Entry<Key, Value> entry = it.next();
            Element readEdge = elementConverter.getFullElement(entry.getKey(), entry.getValue());
            assertEquals(readEdge, edge);
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
    public void testAggregatingSinglePropertySet() throws StoreException, AccumuloElementConversionException {
        testAggregatingSinglePropertySet(byteEntityStore, byteEntityElementConverter);
        testAggregatingSinglePropertySet(gaffer1KeyStore, gaffer1ElementConverter);
    }

    public void testAggregatingSinglePropertySet(final MockAccumuloStore store, final AccumuloElementConverter elementConverter) throws StoreException, AccumuloElementConversionException {
        String visibilityString = "public";
        try {
            // Create table
            // (this method creates the table, removes the versioning iterator, and adds the SetOfStatisticsCombiner iterator).
            TableUtils.createTable(store);

            // Create edge
            Edge edge = new Edge(TestGroups.EDGE);
            edge.setSource("1");
            edge.setDestination("2");
            edge.setDirected(true);
            edge.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 8);

            Properties properties1 = new Properties();
            properties1.put(AccumuloPropertyNames.COUNT, 1);

            // Accumulo key
            Key key = elementConverter.getKeysFromEdge(edge).getFirst();

            // Accumulo values
            Value value1 = elementConverter.getValueFromProperties(properties1, TestGroups.EDGE);

            // Create mutation
            Mutation m1 = new Mutation(key.getRow());
            m1.put(key.getColumnFamily(), key.getColumnQualifier(), new ColumnVisibility(key.getColumnVisibility()), key.getTimestamp(), value1);

            // Write mutation
            BatchWriterConfig writerConfig = new BatchWriterConfig();
            writerConfig.setMaxMemory(1000000L);
            writerConfig.setMaxLatency(1000L, TimeUnit.MILLISECONDS);
            writerConfig.setMaxWriteThreads(1);
            BatchWriter writer = store.getMockConnector().createBatchWriter(store.getProperties().getTable(), writerConfig);
            writer.addMutation(m1);
            writer.close();

            // Read data back and check we get one merged element
            Authorizations authorizations = new Authorizations(visibilityString);
            Scanner scanner = store.getMockConnector().createScanner(store.getProperties().getTable(), authorizations);
            IteratorSetting iteratorSetting = new IteratorSettingBuilder(AccumuloStoreConstants.QUERY_TIME_AGGREGATOR_PRIORITY,
                    "KeyCombiner", CoreKeyColumnQualifierVisibilityValueAggregatorIterator.class)
                    .all()
                    .dataSchema(store.getDataSchema())
                    .storeSchema(store.getStoreSchema())
                    .keyConverter(store.getKeyPackage().getKeyConverter())
                    .build();
            scanner.addScanIterator(iteratorSetting);
            Iterator<Entry<Key, Value>> it = scanner.iterator();
            Entry<Key, Value> entry = it.next();
            Element readEdge = elementConverter.getFullElement(entry.getKey(), entry.getValue());
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
    public void testAggregatingEmptyColumnQualifier() throws StoreException, AccumuloElementConversionException {
        testAggregatingEmptyColumnQualifier(byteEntityStore, byteEntityElementConverter);
        testAggregatingEmptyColumnQualifier(gaffer1KeyStore, gaffer1ElementConverter);
    }

    public void testAggregatingEmptyColumnQualifier(final MockAccumuloStore store, final AccumuloElementConverter elementConverter) throws StoreException, AccumuloElementConversionException {
        String visibilityString = "public";
        try {
            // Create table
            // (this method creates the table, removes the versioning iterator, and adds the SetOfStatisticsCombiner iterator).
            TableUtils.createTable(store);

            // Create edge
            Edge edge = new Edge(TestGroups.EDGE);
            edge.setSource("1");
            edge.setDestination("2");
            edge.setDirected(true);
            edge.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 8);

            //THIS EDGE WILL BE REDUCED MEANING ITS CQ (columnQualifier) will only occur once because its key is equal.
            Edge edge2 = new Edge(TestGroups.EDGE);
            edge2.setSource("1");
            edge2.setDestination("2");
            edge2.setDirected(true);

            Edge edge3 = new Edge(TestGroups.EDGE);
            edge3.setSource("1");
            edge3.setDestination("2");
            edge3.setDirected(true);

            Properties properties1 = new Properties();
            properties1.put(AccumuloPropertyNames.COUNT, 1);

            Properties properties2 = new Properties();
            properties2.put(AccumuloPropertyNames.COUNT, 2);

            Properties properties3 = new Properties();
            properties3.put(AccumuloPropertyNames.COUNT, 10);

            // Accumulo key
            Key key = elementConverter.getKeysFromEdge(edge).getFirst();
            Key key2 = elementConverter.getKeysFromEdge(edge2).getFirst();
            Key key3 = elementConverter.getKeysFromEdge(edge3).getFirst();

            // Accumulo values
            Value value1 = elementConverter.getValueFromProperties(properties1, TestGroups.EDGE);
            Value value2 = elementConverter.getValueFromProperties(properties2, TestGroups.EDGE);
            Value value3 = elementConverter.getValueFromProperties(properties3, TestGroups.EDGE);

            // Create mutation
            Mutation m1 = new Mutation(key.getRow());
            m1.put(key.getColumnFamily(), key.getColumnQualifier(), new ColumnVisibility(key.getColumnVisibility()), key.getTimestamp(), value1);
            Mutation m2 = new Mutation(key.getRow());
            m2.put(key.getColumnFamily(), key.getColumnQualifier(), new ColumnVisibility(key.getColumnVisibility()), key.getTimestamp(), value2);
            Mutation m3 = new Mutation(key.getRow());
            m3.put(key.getColumnFamily(), key.getColumnQualifier(), new ColumnVisibility(key.getColumnVisibility()), key.getTimestamp(), value3);
            Mutation m4 = new Mutation(key2.getRow());
            m4.put(key2.getColumnFamily(), key2.getColumnQualifier(), new ColumnVisibility(key2.getColumnVisibility()), key2.getTimestamp(), value1);
            Mutation m5 = new Mutation(key.getRow());
            m5.put(key3.getColumnFamily(), key3.getColumnQualifier(), new ColumnVisibility(key3.getColumnVisibility()), key3.getTimestamp(), value1);

            // Write mutation
            BatchWriterConfig writerConfig = new BatchWriterConfig();
            writerConfig.setMaxMemory(1000000L);
            writerConfig.setMaxLatency(1000L, TimeUnit.MILLISECONDS);
            writerConfig.setMaxWriteThreads(1);
            BatchWriter writer = store.getMockConnector().createBatchWriter(store.getProperties().getTable(), writerConfig);
            writer.addMutation(m1);
            writer.addMutation(m2);
            writer.addMutation(m3);
            writer.addMutation(m4);
            writer.addMutation(m5);
            writer.close();

            // Read data back and check we get one merged element
            Authorizations authorizations = new Authorizations(visibilityString);
            Scanner scanner = store.getMockConnector().createScanner(store.getProperties().getTable(), authorizations);
            IteratorSetting iteratorSetting = new IteratorSettingBuilder(AccumuloStoreConstants.QUERY_TIME_AGGREGATOR_PRIORITY,
                    "KeyCombiner", CoreKeyColumnQualifierVisibilityValueAggregatorIterator.class)
                    .all()
                    .dataSchema(store.getDataSchema())
                    .storeSchema(store.getStoreSchema())
                    .keyConverter(store.getKeyPackage().getKeyConverter())
                    .build();
            scanner.addScanIterator(iteratorSetting);
            Iterator<Entry<Key, Value>> it = scanner.iterator();
            Entry<Key, Value> entry = it.next();
            Element readEdge = elementConverter.getFullElement(entry.getKey(), entry.getValue());
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