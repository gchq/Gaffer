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
package uk.gov.gchq.gaffer.accumulostore.key.core.impl;

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
import uk.gov.gchq.gaffer.accumulostore.key.exception.AccumuloElementConversionException;
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
import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static uk.gov.gchq.gaffer.accumulostore.utils.TableUtils.createTable;

public class CoreKeyGroupByAggregatorIteratorTest {
    private static AccumuloStore byteEntityStore;
    private static AccumuloStore gaffer1KeyStore;
    private static final Schema schema = Schema.fromJson(StreamUtil.schemas(CoreKeyGroupByAggregatorIteratorTest.class));
    private static final AccumuloProperties PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.storeProps(CoreKeyGroupByAggregatorIteratorTest.class));
    private static final AccumuloProperties CLASSIC_PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.openStream(CoreKeyGroupByAggregatorIteratorTest.class, "/accumuloStoreClassicKeys.properties"));

    private static AccumuloElementConverter byteEntityElementConverter;
    private static AccumuloElementConverter gaffer1ElementConverter;

    @BeforeClass
    public static void setup() throws StoreException, AccumuloException, AccumuloSecurityException, IOException {
        byteEntityStore = new SingleUseMockAccumuloStore();
        gaffer1KeyStore = new SingleUseMockAccumuloStore();
        gaffer1ElementConverter = new ClassicAccumuloElementConverter(schema);
        byteEntityElementConverter = new ByteEntityAccumuloElementConverter(schema);
    }

    @Before
    public void reInitialise() throws StoreException, TableExistsException {
        byteEntityStore.initialise(schema, PROPERTIES);
        gaffer1KeyStore.initialise(schema, CLASSIC_PROPERTIES);
        createTable(byteEntityStore);
        createTable(gaffer1KeyStore);
    }

    @AfterClass
    public static void tearDown() {
        gaffer1KeyStore = null;
        byteEntityStore = null;
    }

    @Test
    public void shouldMultiplePropertySetsAggregateInByteEntityStore() throws StoreException, AccumuloElementConversionException {
        testAggregatingMultiplePropertySets(byteEntityStore, byteEntityElementConverter);
    }

    @Test
    public void shouldMultiplePropertySetsAggregateInGafferOneStore() throws StoreException, AccumuloElementConversionException {
        testAggregatingMultiplePropertySets(gaffer1KeyStore, gaffer1ElementConverter);
    }

    private void testAggregatingMultiplePropertySets(final AccumuloStore store, final AccumuloElementConverter elementConverter) throws StoreException, AccumuloElementConversionException {
        String visibilityString = "public";
        try {

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
            edge.putProperty(AccumuloPropertyNames.COUNT, 1);

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
            edge2.putProperty(AccumuloPropertyNames.COUNT, 2);

            final Edge edge3 = new Edge(TestGroups.EDGE);
            edge3.setSource("1");
            edge3.setDestination("2");
            edge3.setDirected(true);
            edge3.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 1);
            edge3.putProperty(AccumuloPropertyNames.PROP_1, 0);
            edge3.putProperty(AccumuloPropertyNames.PROP_2, 0);
            edge3.putProperty(AccumuloPropertyNames.PROP_3, 0);
            edge3.putProperty(AccumuloPropertyNames.PROP_4, 0);
            edge3.putProperty(AccumuloPropertyNames.COUNT, 10);

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
            final BatchWriter writer = store.getConnection().createBatchWriter(store.getProperties().getTable(), writerConfig);
            writer.addMutation(m1);
            writer.addMutation(m2);
            writer.addMutation(m3);
            writer.addMutation(m4);
            writer.addMutation(m5);
            writer.close();

            // Read data back and check we get one merged element
            final Authorizations authorizations = new Authorizations(visibilityString);
            final Scanner scanner = store.getConnection().createScanner(store.getProperties().getTable(), authorizations);
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
            final Element readEdge = elementConverter.getFullElement(entry.getKey(), entry.getValue());

            final Edge expectedEdge = new Edge(TestGroups.EDGE);
            expectedEdge.setSource("1");
            expectedEdge.setDestination("2");
            expectedEdge.setDirected(true);
            expectedEdge.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 9);
            expectedEdge.putProperty(AccumuloPropertyNames.COUNT, 15);
            expectedEdge.putProperty(AccumuloPropertyNames.PROP_1, 0);
            expectedEdge.putProperty(AccumuloPropertyNames.PROP_2, 0);
            expectedEdge.putProperty(AccumuloPropertyNames.PROP_3, 0);
            expectedEdge.putProperty(AccumuloPropertyNames.PROP_4, 0);

            assertEquals(expectedEdge, readEdge);
            assertEquals(9, readEdge.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER));
            assertEquals(15, readEdge.getProperty(AccumuloPropertyNames.COUNT));
            // Check no more entries
            if (it.hasNext()) {
                fail("Additional row found.");
            }

        } catch (AccumuloException | TableNotFoundException e) {
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

    public void testAggregatingSinglePropertySet(final AccumuloStore store, final AccumuloElementConverter elementConverter) throws StoreException, AccumuloElementConversionException {
        String visibilityString = "public";
        try {
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
            final Value value1 = elementConverter.getValueFromProperties(TestGroups.EDGE, properties1);

            // Create mutation
            final Mutation m1 = new Mutation(key.getRow());
            m1.put(key.getColumnFamily(), key.getColumnQualifier(), new ColumnVisibility(key.getColumnVisibility()), key.getTimestamp(), value1);

            // Write mutation
            final BatchWriterConfig writerConfig = new BatchWriterConfig();
            writerConfig.setMaxMemory(1000000L);
            writerConfig.setMaxLatency(1000L, TimeUnit.MILLISECONDS);
            writerConfig.setMaxWriteThreads(1);
            final BatchWriter writer = store.getConnection().createBatchWriter(store.getProperties().getTable(), writerConfig);
            writer.addMutation(m1);
            writer.close();

            final Edge expectedEdge = new Edge(TestGroups.EDGE);
            expectedEdge.setSource("1");
            expectedEdge.setDestination("2");
            expectedEdge.setDirected(true);
            expectedEdge.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 8);
            expectedEdge.putProperty(AccumuloPropertyNames.COUNT, 1);

            // Read data back and check we get one merged element
            final Authorizations authorizations = new Authorizations(visibilityString);
            final Scanner scanner = store.getConnection().createScanner(store.getProperties().getTable(), authorizations);
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
            final Element readEdge = elementConverter.getFullElement(entry.getKey(), entry.getValue());
            assertEquals(expectedEdge, readEdge);
            assertEquals(8, readEdge.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER));
            assertEquals(1, readEdge.getProperty(AccumuloPropertyNames.COUNT));
            // Check no more entries
            if (it.hasNext()) {
                fail("Additional row found.");
            }

        } catch (AccumuloException | TableNotFoundException e) {
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

    public void testAggregatingEmptyColumnQualifier(final AccumuloStore store, final AccumuloElementConverter elementConverter) throws StoreException, AccumuloElementConversionException {
        final String visibilityString = "public";
        try {
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
            edge.putProperty(AccumuloPropertyNames.COUNT, 1);

            //THIS EDGE WILL BE REDUCED MEANING ITS CQ (columnQualifier) will only occur once because its key is equal.
            final Edge edge2 = new Edge(TestGroups.EDGE);
            edge2.setSource("1");
            edge2.setDestination("2");
            edge2.setDirected(true);
            edge2.putProperty(AccumuloPropertyNames.PROP_1, 0);
            edge2.putProperty(AccumuloPropertyNames.PROP_2, 0);
            edge2.putProperty(AccumuloPropertyNames.PROP_3, 0);
            edge2.putProperty(AccumuloPropertyNames.PROP_4, 0);
            edge2.putProperty(AccumuloPropertyNames.COUNT, 2);

            final Edge edge3 = new Edge(TestGroups.EDGE);
            edge3.setSource("1");
            edge3.setDestination("2");
            edge3.setDirected(true);
            edge3.putProperty(AccumuloPropertyNames.PROP_1, 0);
            edge3.putProperty(AccumuloPropertyNames.PROP_2, 0);
            edge3.putProperty(AccumuloPropertyNames.PROP_3, 0);
            edge3.putProperty(AccumuloPropertyNames.PROP_4, 0);
            edge3.putProperty(AccumuloPropertyNames.COUNT, 10);

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
            final BatchWriter writer = store.getConnection().createBatchWriter(store.getProperties().getTable(), writerConfig);
            writer.addMutation(m1);
            writer.addMutation(m2);
            writer.addMutation(m3);
            writer.addMutation(m4);
            writer.addMutation(m5);
            writer.close();

            Edge expectedEdge = new Edge(TestGroups.EDGE);
            expectedEdge.setSource("1");
            expectedEdge.setDestination("2");
            expectedEdge.setDirected(true);
            expectedEdge.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 8);
            expectedEdge.putProperty(AccumuloPropertyNames.COUNT, 15);
            expectedEdge.putProperty(AccumuloPropertyNames.PROP_1, 0);
            expectedEdge.putProperty(AccumuloPropertyNames.PROP_2, 0);
            expectedEdge.putProperty(AccumuloPropertyNames.PROP_3, 0);
            expectedEdge.putProperty(AccumuloPropertyNames.PROP_4, 0);

            // Read data back and check we get one merged element
            final Authorizations authorizations = new Authorizations(visibilityString);
            final Scanner scanner = store.getConnection().createScanner(store.getProperties().getTable(), authorizations);
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
            final Element readEdge = elementConverter.getFullElement(entry.getKey(), entry.getValue());
            assertEquals(expectedEdge, readEdge);
            assertEquals(8, readEdge.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER));
            assertEquals(15, readEdge.getProperty(AccumuloPropertyNames.COUNT));
            // Check no more entries
            if (it.hasNext()) {
                fail("Additional row found.");
            }

        } catch (AccumuloException | TableNotFoundException e) {
            fail(this.getClass().getSimpleName() + " failed with exception: " + e);
        }
    }

    @Test
    public void shouldPartiallyAggregateColumnQualifierOverCQ1GroupByInByteEntityStore() throws StoreException, AccumuloElementConversionException {
        shouldPartiallyAggregateColumnQualifierOverCQ1GroupBy(byteEntityStore, byteEntityElementConverter);
    }

    @Test
    public void shouldPartiallyAggregateColumnQualifierOverCQ1GroupByInGafferOneStore() throws StoreException, AccumuloElementConversionException {
        shouldPartiallyAggregateColumnQualifierOverCQ1GroupBy(gaffer1KeyStore, gaffer1ElementConverter);
    }

    public void shouldPartiallyAggregateColumnQualifierOverCQ1GroupBy(final AccumuloStore store, final AccumuloElementConverter elementConverter) throws StoreException, AccumuloElementConversionException {
        final String visibilityString = "public";
        try {
            // Create edge
            final Edge edge = new Edge(TestGroups.EDGE);
            edge.setSource("1");
            edge.setDestination("2");
            edge.setDirected(true);
            edge.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 1);
            edge.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER_2, 1);
            edge.putProperty(AccumuloPropertyNames.PROP_1, 0);
            edge.putProperty(AccumuloPropertyNames.PROP_2, 0);
            edge.putProperty(AccumuloPropertyNames.PROP_3, 0);
            edge.putProperty(AccumuloPropertyNames.PROP_4, 0);
            edge.putProperty(AccumuloPropertyNames.COUNT, 1);

            final Edge edge2 = new Edge(TestGroups.EDGE);
            edge2.setSource("1");
            edge2.setDestination("2");
            edge2.setDirected(true);
            edge2.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 1);
            edge2.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER_2, 2);
            edge2.putProperty(AccumuloPropertyNames.PROP_1, 0);
            edge2.putProperty(AccumuloPropertyNames.PROP_2, 0);
            edge2.putProperty(AccumuloPropertyNames.PROP_3, 0);
            edge2.putProperty(AccumuloPropertyNames.PROP_4, 0);
            edge2.putProperty(AccumuloPropertyNames.COUNT, 1);

            final Edge edge3 = new Edge(TestGroups.EDGE);
            edge3.setSource("1");
            edge3.setDestination("2");
            edge3.setDirected(true);
            edge3.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 1);
            edge3.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER_2, 3);
            edge3.putProperty(AccumuloPropertyNames.PROP_1, 0);
            edge3.putProperty(AccumuloPropertyNames.PROP_2, 0);
            edge3.putProperty(AccumuloPropertyNames.PROP_3, 0);
            edge3.putProperty(AccumuloPropertyNames.PROP_4, 0);
            edge3.putProperty(AccumuloPropertyNames.COUNT, 1);


            //THIS EDGE WILL BE REDUCED MEANING ITS CQ (columnQualifier) will only occur once because its key is equal.
            final Edge edge4 = new Edge(TestGroups.EDGE);
            edge4.setSource("1");
            edge4.setDestination("2");
            edge4.setDirected(true);
            edge4.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 2);
            edge4.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER_2, 4);
            edge4.putProperty(AccumuloPropertyNames.PROP_1, 0);
            edge4.putProperty(AccumuloPropertyNames.PROP_2, 0);
            edge4.putProperty(AccumuloPropertyNames.PROP_3, 0);
            edge4.putProperty(AccumuloPropertyNames.PROP_4, 0);
            edge4.putProperty(AccumuloPropertyNames.COUNT, 2);

            final Edge edge5 = new Edge(TestGroups.EDGE);
            edge5.setSource("1");
            edge5.setDestination("2");
            edge5.setDirected(true);
            edge5.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 3);
            edge5.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER_2, 5);
            edge5.putProperty(AccumuloPropertyNames.PROP_1, 0);
            edge5.putProperty(AccumuloPropertyNames.PROP_2, 0);
            edge5.putProperty(AccumuloPropertyNames.PROP_3, 0);
            edge5.putProperty(AccumuloPropertyNames.PROP_4, 0);
            edge5.putProperty(AccumuloPropertyNames.COUNT, 10);

            final Edge edge6 = new Edge(TestGroups.EDGE);
            edge6.setSource("1");
            edge6.setDestination("2");
            edge6.setDirected(true);
            edge6.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 4);
            edge6.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER_2, 6);
            edge6.putProperty(AccumuloPropertyNames.PROP_1, 0);
            edge6.putProperty(AccumuloPropertyNames.PROP_2, 0);
            edge6.putProperty(AccumuloPropertyNames.PROP_3, 0);
            edge6.putProperty(AccumuloPropertyNames.PROP_4, 0);
            edge6.putProperty(AccumuloPropertyNames.COUNT, 5);

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
            final BatchWriter writer = store.getConnection().createBatchWriter(store.getProperties().getTable(), writerConfig);
            writer.addMutation(m1);
            writer.addMutation(m2);
            writer.addMutation(m3);
            writer.addMutation(m4);
            writer.addMutation(m5);
            writer.addMutation(m6);
            writer.close();

            Edge expectedEdge1 = new Edge(TestGroups.EDGE);
            expectedEdge1.setSource("1");
            expectedEdge1.setDestination("2");
            expectedEdge1.setDirected(true);
            expectedEdge1.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 1);
            expectedEdge1.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER_2, 6);
            expectedEdge1.putProperty(AccumuloPropertyNames.COUNT, 3);
            expectedEdge1.putProperty(AccumuloPropertyNames.PROP_1, 0);
            expectedEdge1.putProperty(AccumuloPropertyNames.PROP_2, 0);
            expectedEdge1.putProperty(AccumuloPropertyNames.PROP_3, 0);
            expectedEdge1.putProperty(AccumuloPropertyNames.PROP_4, 0);

            Edge expectedEdge2 = new Edge(TestGroups.EDGE);
            expectedEdge2.setSource("1");
            expectedEdge2.setDestination("2");
            expectedEdge2.setDirected(true);
            expectedEdge2.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 2);
            expectedEdge2.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER_2, 4);
            expectedEdge2.putProperty(AccumuloPropertyNames.COUNT, 2);
            expectedEdge2.putProperty(AccumuloPropertyNames.PROP_1, 0);
            expectedEdge2.putProperty(AccumuloPropertyNames.PROP_2, 0);
            expectedEdge2.putProperty(AccumuloPropertyNames.PROP_3, 0);
            expectedEdge2.putProperty(AccumuloPropertyNames.PROP_4, 0);

            Edge expectedEdge3 = new Edge(TestGroups.EDGE);
            expectedEdge3.setSource("1");
            expectedEdge3.setDestination("2");
            expectedEdge3.setDirected(true);
            expectedEdge3.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 3);
            expectedEdge3.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER_2, 5);
            expectedEdge3.putProperty(AccumuloPropertyNames.COUNT, 10);
            expectedEdge3.putProperty(AccumuloPropertyNames.PROP_1, 0);
            expectedEdge3.putProperty(AccumuloPropertyNames.PROP_2, 0);
            expectedEdge3.putProperty(AccumuloPropertyNames.PROP_3, 0);
            expectedEdge3.putProperty(AccumuloPropertyNames.PROP_4, 0);

            Edge expectedEdge4 = new Edge(TestGroups.EDGE);
            expectedEdge4.setSource("1");
            expectedEdge4.setDestination("2");
            expectedEdge4.setDirected(true);
            expectedEdge4.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 4);
            expectedEdge4.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER_2, 6);
            expectedEdge4.putProperty(AccumuloPropertyNames.COUNT, 5);
            expectedEdge4.putProperty(AccumuloPropertyNames.PROP_1, 0);
            expectedEdge4.putProperty(AccumuloPropertyNames.PROP_2, 0);
            expectedEdge4.putProperty(AccumuloPropertyNames.PROP_3, 0);
            expectedEdge4.putProperty(AccumuloPropertyNames.PROP_4, 0);


            // Read data back and check we get one merged element
            final Authorizations authorizations = new Authorizations(visibilityString);
            final Scanner scanner = store.getConnection().createScanner(store.getProperties().getTable(), authorizations);
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
            Element readEdge = elementConverter.getFullElement(entry.getKey(), entry.getValue());
            assertEquals(expectedEdge1, readEdge);
            assertEquals(1, readEdge.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER));
            assertEquals(6, readEdge.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER_2));
            assertEquals(3, readEdge.getProperty(AccumuloPropertyNames.COUNT));
            entry = it.next();
            readEdge = elementConverter.getFullElement(entry.getKey(), entry.getValue());
            assertEquals(expectedEdge2, readEdge);
            assertEquals(2, readEdge.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER));
            assertEquals(4, readEdge.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER_2));
            assertEquals(2, readEdge.getProperty(AccumuloPropertyNames.COUNT));
            entry = it.next();
            readEdge = elementConverter.getFullElement(entry.getKey(), entry.getValue());
            assertEquals(expectedEdge3, readEdge);
            assertEquals(3, readEdge.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER));
            assertEquals(5, readEdge.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER_2));
            assertEquals(10, readEdge.getProperty(AccumuloPropertyNames.COUNT));
            entry = it.next();
            readEdge = elementConverter.getFullElement(entry.getKey(), entry.getValue());
            assertEquals(expectedEdge4, readEdge);
            assertEquals(4, readEdge.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER));
            assertEquals(6, readEdge.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER_2));
            assertEquals(5, readEdge.getProperty(AccumuloPropertyNames.COUNT));

            if (it.hasNext()) {
                fail("Additional row found.");
            }

        } catch (AccumuloException | TableNotFoundException e) {
            fail(this.getClass().getSimpleName() + " failed with exception: " + e);
        }
    }

    @Test
    public void shouldAggregatePropertiesOnlyWhenGroupByIsSetToCQ1CQ2InByteEntityStore() throws StoreException, AccumuloElementConversionException {
        shouldAggregatePropertiesOnlyWhenGroupByIsSetToCQ1CQ2(byteEntityStore, byteEntityElementConverter);
    }

    @Test
    public void shouldAggregatePropertiesOnlyWhenGroupByIsSetToCQ1CQ2InGafferOneStore() throws StoreException, AccumuloElementConversionException {
        shouldAggregatePropertiesOnlyWhenGroupByIsSetToCQ1CQ2(gaffer1KeyStore, gaffer1ElementConverter);
    }

    public void shouldAggregatePropertiesOnlyWhenGroupByIsSetToCQ1CQ2(final AccumuloStore store, final AccumuloElementConverter elementConverter) throws StoreException, AccumuloElementConversionException {
        final String visibilityString = "public";
        try {
            // Create edge
            final Edge edge = new Edge(TestGroups.EDGE);
            edge.setSource("1");
            edge.setDestination("2");
            edge.setDirected(true);
            edge.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 1);
            edge.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER_2, 1);
            edge.putProperty(AccumuloPropertyNames.PROP_1, 0);
            edge.putProperty(AccumuloPropertyNames.PROP_2, 0);
            edge.putProperty(AccumuloPropertyNames.PROP_3, 0);
            edge.putProperty(AccumuloPropertyNames.PROP_4, 0);
            edge.putProperty(AccumuloPropertyNames.COUNT, 1);

            final Edge edge2 = new Edge(TestGroups.EDGE);
            edge2.setSource("1");
            edge2.setDestination("2");
            edge2.setDirected(true);
            edge2.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 1);
            edge2.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER_2, 1);
            edge2.putProperty(AccumuloPropertyNames.PROP_1, 0);
            edge2.putProperty(AccumuloPropertyNames.PROP_2, 0);
            edge2.putProperty(AccumuloPropertyNames.PROP_3, 0);
            edge2.putProperty(AccumuloPropertyNames.PROP_4, 0);
            edge2.putProperty(AccumuloPropertyNames.COUNT, 1);

            final Edge edge3 = new Edge(TestGroups.EDGE);
            edge3.setSource("1");
            edge3.setDestination("2");
            edge3.setDirected(true);
            edge3.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 1);
            edge3.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER_2, 1);
            edge3.putProperty(AccumuloPropertyNames.PROP_1, 0);
            edge3.putProperty(AccumuloPropertyNames.PROP_2, 0);
            edge3.putProperty(AccumuloPropertyNames.PROP_3, 0);
            edge3.putProperty(AccumuloPropertyNames.PROP_4, 0);
            edge3.putProperty(AccumuloPropertyNames.COUNT, 1);


            //THIS EDGE WILL BE REDUCED MEANING ITS CQ (columnQualifier) will only occur once because its key is equal.
            final Edge edge4 = new Edge(TestGroups.EDGE);
            edge4.setSource("1");
            edge4.setDestination("2");
            edge4.setDirected(true);
            edge4.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 1);
            edge4.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER_2, 4);
            edge4.putProperty(AccumuloPropertyNames.PROP_1, 0);
            edge4.putProperty(AccumuloPropertyNames.PROP_2, 0);
            edge4.putProperty(AccumuloPropertyNames.PROP_3, 0);
            edge4.putProperty(AccumuloPropertyNames.PROP_4, 0);
            edge4.putProperty(AccumuloPropertyNames.COUNT, 2);

            final Edge edge5 = new Edge(TestGroups.EDGE);
            edge5.setSource("1");
            edge5.setDestination("2");
            edge5.setDirected(true);
            edge5.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 3);
            edge5.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER_2, 5);
            edge5.putProperty(AccumuloPropertyNames.PROP_1, 0);
            edge5.putProperty(AccumuloPropertyNames.PROP_2, 0);
            edge5.putProperty(AccumuloPropertyNames.PROP_3, 0);
            edge5.putProperty(AccumuloPropertyNames.PROP_4, 0);
            edge5.putProperty(AccumuloPropertyNames.COUNT, 10);

            final Edge edge6 = new Edge(TestGroups.EDGE);
            edge6.setSource("1");
            edge6.setDestination("2");
            edge6.setDirected(true);
            edge6.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 3);
            edge6.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER_2, 5);
            edge6.putProperty(AccumuloPropertyNames.PROP_1, 0);
            edge6.putProperty(AccumuloPropertyNames.PROP_2, 0);
            edge6.putProperty(AccumuloPropertyNames.PROP_3, 0);
            edge6.putProperty(AccumuloPropertyNames.PROP_4, 0);
            edge6.putProperty(AccumuloPropertyNames.COUNT, 5);

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
            final BatchWriter writer = store.getConnection().createBatchWriter(store.getProperties().getTable(), writerConfig);
            writer.addMutation(m1);
            writer.addMutation(m2);
            writer.addMutation(m3);
            writer.addMutation(m4);
            writer.addMutation(m5);
            writer.addMutation(m6);
            writer.close();

            Edge expectedEdge1 = new Edge(TestGroups.EDGE);
            expectedEdge1.setSource("1");
            expectedEdge1.setDestination("2");
            expectedEdge1.setDirected(true);
            expectedEdge1.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 1);
            expectedEdge1.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER_2, 1);
            expectedEdge1.putProperty(AccumuloPropertyNames.COUNT, 3);
            expectedEdge1.putProperty(AccumuloPropertyNames.PROP_1, 0);
            expectedEdge1.putProperty(AccumuloPropertyNames.PROP_2, 0);
            expectedEdge1.putProperty(AccumuloPropertyNames.PROP_3, 0);
            expectedEdge1.putProperty(AccumuloPropertyNames.PROP_4, 0);

            Edge expectedEdge2 = new Edge(TestGroups.EDGE);
            expectedEdge2.setSource("1");
            expectedEdge2.setDestination("2");
            expectedEdge2.setDirected(true);
            expectedEdge2.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 1);
            expectedEdge2.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER_2, 4);
            expectedEdge2.putProperty(AccumuloPropertyNames.COUNT, 2);
            expectedEdge2.putProperty(AccumuloPropertyNames.PROP_1, 0);
            expectedEdge2.putProperty(AccumuloPropertyNames.PROP_2, 0);
            expectedEdge2.putProperty(AccumuloPropertyNames.PROP_3, 0);
            expectedEdge2.putProperty(AccumuloPropertyNames.PROP_4, 0);

            Edge expectedEdge3 = new Edge(TestGroups.EDGE);
            expectedEdge3.setSource("1");
            expectedEdge3.setDestination("2");
            expectedEdge3.setDirected(true);
            expectedEdge3.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 3);
            expectedEdge3.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER_2, 5);
            expectedEdge3.putProperty(AccumuloPropertyNames.COUNT, 15);
            expectedEdge3.putProperty(AccumuloPropertyNames.PROP_1, 0);
            expectedEdge3.putProperty(AccumuloPropertyNames.PROP_2, 0);
            expectedEdge3.putProperty(AccumuloPropertyNames.PROP_3, 0);
            expectedEdge3.putProperty(AccumuloPropertyNames.PROP_4, 0);

            // Read data back and check we get one merged element
            final Authorizations authorizations = new Authorizations(visibilityString);
            final Scanner scanner = store.getConnection().createScanner(store.getProperties().getTable(), authorizations);
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
            Element readEdge = elementConverter.getFullElement(entry.getKey(), entry.getValue());
            assertEquals(expectedEdge1, readEdge);
            assertEquals(1, readEdge.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER));
            assertEquals(1, readEdge.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER_2));
            assertEquals(3, readEdge.getProperty(AccumuloPropertyNames.COUNT));
            entry = it.next();
            readEdge = elementConverter.getFullElement(entry.getKey(), entry.getValue());
            assertEquals(expectedEdge2, readEdge);
            assertEquals(1, readEdge.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER));
            assertEquals(4, readEdge.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER_2));
            assertEquals(2, readEdge.getProperty(AccumuloPropertyNames.COUNT));
            entry = it.next();
            readEdge = elementConverter.getFullElement(entry.getKey(), entry.getValue());
            assertEquals(expectedEdge3, readEdge);
            assertEquals(3, readEdge.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER));
            assertEquals(5, readEdge.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER_2));
            assertEquals(15, readEdge.getProperty(AccumuloPropertyNames.COUNT));

            if (it.hasNext()) {
                fail("Additional row found.");
            }

        } catch (AccumuloException | TableNotFoundException e) {
            fail(this.getClass().getSimpleName() + " failed with exception: " + e);
        }
    }

    @Test
    public void shouldAggregateEverythingWhenGroupByIsSetToBlankInByteEntityStore() throws StoreException, AccumuloElementConversionException {
        shouldAggregateEverythingWhenGroupByIsSetToBlank(byteEntityStore, byteEntityElementConverter);
    }

    @Test
    public void shouldAggregateEverythingWhenGroupByIsSetToBlankInGafferOneStore() throws StoreException, AccumuloElementConversionException {
        shouldAggregateEverythingWhenGroupByIsSetToBlank(gaffer1KeyStore, gaffer1ElementConverter);
    }

    public void shouldAggregateEverythingWhenGroupByIsSetToBlank(final AccumuloStore store, final AccumuloElementConverter elementConverter) throws StoreException, AccumuloElementConversionException {
        final String visibilityString = "public";
        try {
            // Create edge
            final Edge edge = new Edge(TestGroups.EDGE);
            edge.setSource("1");
            edge.setDestination("2");
            edge.setDirected(true);
            edge.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 1);
            edge.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER_2, 1);
            edge.putProperty(AccumuloPropertyNames.PROP_1, 0);
            edge.putProperty(AccumuloPropertyNames.PROP_2, 0);
            edge.putProperty(AccumuloPropertyNames.PROP_3, 0);
            edge.putProperty(AccumuloPropertyNames.PROP_4, 0);
            edge.putProperty(AccumuloPropertyNames.COUNT, 1);

            final Edge edge2 = new Edge(TestGroups.EDGE);
            edge2.setSource("1");
            edge2.setDestination("2");
            edge2.setDirected(true);
            edge2.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 1);
            edge2.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER_2, 1);
            edge2.putProperty(AccumuloPropertyNames.PROP_1, 0);
            edge2.putProperty(AccumuloPropertyNames.PROP_2, 0);
            edge2.putProperty(AccumuloPropertyNames.PROP_3, 0);
            edge2.putProperty(AccumuloPropertyNames.PROP_4, 0);
            edge2.putProperty(AccumuloPropertyNames.COUNT, 1);

            final Edge edge3 = new Edge(TestGroups.EDGE);
            edge3.setSource("1");
            edge3.setDestination("2");
            edge3.setDirected(true);
            edge3.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 1);
            edge3.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER_2, 1);
            edge3.putProperty(AccumuloPropertyNames.PROP_1, 0);
            edge3.putProperty(AccumuloPropertyNames.PROP_2, 0);
            edge3.putProperty(AccumuloPropertyNames.PROP_3, 0);
            edge3.putProperty(AccumuloPropertyNames.PROP_4, 0);
            edge3.putProperty(AccumuloPropertyNames.COUNT, 1);


            //THIS EDGE WILL BE REDUCED MEANING ITS CQ (columnQualifier) will only occur once because its key is equal.
            final Edge edge4 = new Edge(TestGroups.EDGE);
            edge4.setSource("1");
            edge4.setDestination("2");
            edge4.setDirected(true);
            edge4.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 1);
            edge4.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER_2, 4);
            edge4.putProperty(AccumuloPropertyNames.PROP_1, 0);
            edge4.putProperty(AccumuloPropertyNames.PROP_2, 0);
            edge4.putProperty(AccumuloPropertyNames.PROP_3, 0);
            edge4.putProperty(AccumuloPropertyNames.PROP_4, 0);
            edge4.putProperty(AccumuloPropertyNames.COUNT, 2);

            final Edge edge5 = new Edge(TestGroups.EDGE);
            edge5.setSource("1");
            edge5.setDestination("2");
            edge5.setDirected(true);
            edge5.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 3);
            edge5.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER_2, 5);
            edge5.putProperty(AccumuloPropertyNames.PROP_1, 0);
            edge5.putProperty(AccumuloPropertyNames.PROP_2, 0);
            edge5.putProperty(AccumuloPropertyNames.PROP_3, 0);
            edge5.putProperty(AccumuloPropertyNames.PROP_4, 0);
            edge5.putProperty(AccumuloPropertyNames.COUNT, 10);

            final Edge edge6 = new Edge(TestGroups.EDGE);
            edge6.setSource("1");
            edge6.setDestination("2");
            edge6.setDirected(true);
            edge6.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 3);
            edge6.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER_2, 5);
            edge6.putProperty(AccumuloPropertyNames.PROP_1, 0);
            edge6.putProperty(AccumuloPropertyNames.PROP_2, 0);
            edge6.putProperty(AccumuloPropertyNames.PROP_3, 0);
            edge6.putProperty(AccumuloPropertyNames.PROP_4, 0);
            edge6.putProperty(AccumuloPropertyNames.COUNT, 5);

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
            final BatchWriter writer = store.getConnection().createBatchWriter(store.getProperties().getTable(), writerConfig);
            writer.addMutation(m1);
            writer.addMutation(m2);
            writer.addMutation(m3);
            writer.addMutation(m4);
            writer.addMutation(m5);
            writer.addMutation(m6);
            writer.close();

            Edge expectedEdge1 = new Edge(TestGroups.EDGE);
            expectedEdge1.setSource("1");
            expectedEdge1.setDestination("2");
            expectedEdge1.setDirected(true);
            expectedEdge1.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 5);
            expectedEdge1.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER_2, 10);
            expectedEdge1.putProperty(AccumuloPropertyNames.COUNT, 20);
            expectedEdge1.putProperty(AccumuloPropertyNames.PROP_1, 0);
            expectedEdge1.putProperty(AccumuloPropertyNames.PROP_2, 0);
            expectedEdge1.putProperty(AccumuloPropertyNames.PROP_3, 0);
            expectedEdge1.putProperty(AccumuloPropertyNames.PROP_4, 0);

            // Read data back and check we get one merged element
            final Authorizations authorizations = new Authorizations(visibilityString);
            final Scanner scanner = store.getConnection().createScanner(store.getProperties().getTable(), authorizations);
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
            Element readEdge = elementConverter.getFullElement(entry.getKey(), entry.getValue());
            assertEquals(expectedEdge1, readEdge);
            assertEquals(5, readEdge.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER));
            assertEquals(10, readEdge.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER_2));
            assertEquals(20, readEdge.getProperty(AccumuloPropertyNames.COUNT));

            if (it.hasNext()) {
                fail("Additional row found.");
            }

        } catch (AccumuloException | TableNotFoundException e) {
            fail(this.getClass().getSimpleName() + " failed with exception: " + e);
        }
    }
}