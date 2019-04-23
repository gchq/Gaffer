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
package uk.gov.gchq.gaffer.accumulostore.key.impl;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
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
import uk.gov.gchq.gaffer.accumulostore.key.RangeFactory;
import uk.gov.gchq.gaffer.accumulostore.key.core.impl.byteEntity.ByteEntityAccumuloElementConverter;
import uk.gov.gchq.gaffer.accumulostore.key.core.impl.classic.ClassicAccumuloElementConverter;
import uk.gov.gchq.gaffer.accumulostore.key.exception.IteratorSettingException;
import uk.gov.gchq.gaffer.accumulostore.key.exception.RangeFactoryException;
import uk.gov.gchq.gaffer.accumulostore.operation.impl.SummariseGroupOverRanges;
import uk.gov.gchq.gaffer.accumulostore.utils.AccumuloPropertyNames;
import uk.gov.gchq.gaffer.accumulostore.utils.TableUtils;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static uk.gov.gchq.gaffer.accumulostore.utils.TableUtils.createTable;


public class RowIdAggregatorTest {

    private static AccumuloStore byteEntityStore;
    private static AccumuloStore gaffer1KeyStore;
    private static final Schema SCHEMA = Schema.fromJson(StreamUtil.schemas(RowIdAggregatorTest.class));
    private static final AccumuloProperties PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.storeProps(RowIdAggregatorTest.class));
    private static final AccumuloProperties CLASSIC_PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.openStream(RowIdAggregatorTest.class, "/accumuloStoreClassicKeys.properties"));

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
    public void testMultiplePropertySetsAggregateAcrossRowIDInByteEntityStore() throws StoreException, RangeFactoryException {
        testAggregatingMultiplePropertySetsAcrossRowIDRange(byteEntityStore, byteEntityElementConverter);
    }

    @Test
    public void testMultiplePropertySetsAggregateAcrossRowIDInGafferOneStore() throws StoreException, RangeFactoryException {
        testAggregatingMultiplePropertySetsAcrossRowIDRange(gaffer1KeyStore, gaffer1ElementConverter);
    }

    private void testAggregatingMultiplePropertySetsAcrossRowIDRange(final AccumuloStore store, final AccumuloElementConverter elementConverter) throws StoreException, RangeFactoryException {
        String visibilityString = "public";
        try {
            // Create table
            // (this method creates the table, removes the versioning iterator, and adds the SetOfStatisticsCombiner iterator).
            TableUtils.createTable(store);

            final Properties properties1 = new Properties();
            properties1.put(AccumuloPropertyNames.COUNT, 1);

            final Properties properties2 = new Properties();
            properties2.put(AccumuloPropertyNames.COUNT, 1);

            final Properties properties3 = new Properties();
            properties3.put(AccumuloPropertyNames.COUNT, 2);

            // Create edge
            final Edge edge = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("2")
                    .dest("1")
                    .directed(true)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 1)
                    .property(AccumuloPropertyNames.PROP_1, 0)
                    .property(AccumuloPropertyNames.PROP_2, 0)
                    .property(AccumuloPropertyNames.PROP_3, 0)
                    .property(AccumuloPropertyNames.PROP_4, 0)
                    .build();

            final Edge edge2 = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("B")
                    .dest("Z")
                    .directed(true)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 1)
                    .property(AccumuloPropertyNames.PROP_1, 1)
                    .property(AccumuloPropertyNames.PROP_2, 1)
                    .property(AccumuloPropertyNames.PROP_3, 1)
                    .property(AccumuloPropertyNames.PROP_4, 1)
                    .build();

            final Edge edge3 = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("3")
                    .dest("8")
                    .directed(true)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 1)
                    .property(AccumuloPropertyNames.PROP_1, 0)
                    .property(AccumuloPropertyNames.PROP_2, 0)
                    .property(AccumuloPropertyNames.PROP_3, 0)
                    .property(AccumuloPropertyNames.PROP_4, 0)
                    .build();

            final Edge edge6 = new Edge.Builder()
                    .group("BasicEdge2")
                    .source("1")
                    .dest("5")
                    .directed(true)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 2)
                    .property(AccumuloPropertyNames.PROP_1, 0)
                    .property(AccumuloPropertyNames.PROP_2, 0)
                    .property(AccumuloPropertyNames.PROP_3, 0)
                    .property(AccumuloPropertyNames.PROP_4, 0)
                    .build();

            final Edge edge7 = new Edge.Builder()
                    .group("BasicEdge2")
                    .source("2")
                    .dest("6")
                    .directed(true)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 1)
                    .property(AccumuloPropertyNames.PROP_1, 0)
                    .property(AccumuloPropertyNames.PROP_2, 0)
                    .property(AccumuloPropertyNames.PROP_3, 0)
                    .property(AccumuloPropertyNames.PROP_4, 0)
                    .build();

            final Edge edge8 = new Edge.Builder()
                    .group("BasicEdge2")
                    .source("4")
                    .dest("8")
                    .directed(true)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 2)
                    .property(AccumuloPropertyNames.PROP_1, 0)
                    .property(AccumuloPropertyNames.PROP_2, 0)
                    .property(AccumuloPropertyNames.PROP_3, 0)
                    .property(AccumuloPropertyNames.PROP_4, 0)
                    .build();

            final Edge edge9 = new Edge.Builder()
                    .group("BasicEdge2")
                    .source("5")
                    .dest("9")
                    .directed(true)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 2)
                    .property(AccumuloPropertyNames.PROP_1, 0)
                    .property(AccumuloPropertyNames.PROP_2, 0)
                    .property(AccumuloPropertyNames.PROP_3, 0)
                    .property(AccumuloPropertyNames.PROP_4, 0)
                    .build();

            // Accumulo key
            final Key key = elementConverter.getKeysFromEdge(edge).getFirst();
            final Key key2 = elementConverter.getKeysFromEdge(edge2).getFirst();
            final Key key3 = elementConverter.getKeysFromEdge(edge3).getFirst();
            final Key key4 = elementConverter.getKeysFromEdge(edge6).getFirst();
            final Key key5 = elementConverter.getKeysFromEdge(edge7).getFirst();
            final Key key6 = elementConverter.getKeysFromEdge(edge8).getFirst();
            final Key key7 = elementConverter.getKeysFromEdge(edge9).getFirst();

            // Accumulo values
            final Value value1 = elementConverter.getValueFromProperties(TestGroups.EDGE, properties1);
            final Value value2 = elementConverter.getValueFromProperties(TestGroups.EDGE, properties2);
            final Value value3 = elementConverter.getValueFromProperties(TestGroups.EDGE, properties3);
            final Value value4 = elementConverter.getValueFromProperties(TestGroups.EDGE_2, properties1);
            final Value value5 = elementConverter.getValueFromProperties(TestGroups.EDGE_2, properties2);

            //Create mutation
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
            final Mutation m6 = new Mutation(key4.getRow());
            m6.put(key4.getColumnFamily(), key4.getColumnQualifier(), new ColumnVisibility(key4.getColumnVisibility()), key4.getTimestamp(), value4);
            final Mutation m7 = new Mutation(key5.getRow());
            m7.put(key5.getColumnFamily(), key5.getColumnQualifier(), new ColumnVisibility(key5.getColumnVisibility()), key5.getTimestamp(), value5);
            final Mutation m8 = new Mutation(key6.getRow());
            m8.put(key6.getColumnFamily(), key6.getColumnQualifier(), new ColumnVisibility(key6.getColumnVisibility()), key6.getTimestamp(), value5);
            final Mutation m9 = new Mutation(key7.getRow());
            m9.put(key7.getColumnFamily(), key7.getColumnQualifier(), new ColumnVisibility(key7.getColumnVisibility()), key7.getTimestamp(), value5);

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
            writer.addMutation(m7);
            writer.addMutation(m8);
            writer.addMutation(m9);
            writer.close();

            // Read data back and check we get one merged element
            final Authorizations authorizations = new Authorizations(visibilityString);
            final BatchScanner scanner = store.getConnection().createBatchScanner(store.getTableName(), authorizations, 1000);
            try {
                scanner.addScanIterator(store.getKeyPackage().getIteratorFactory().getRowIDAggregatorIteratorSetting(store, "BasicEdge2"));
            } catch (final IteratorSettingException e) {
                fail(e.getMessage());
            }
            final RangeFactory rangeF = store.getKeyPackage().getRangeFactory();
            final SummariseGroupOverRanges summariseGroupOverRanges = new SummariseGroupOverRanges.Builder()
                    .view(new View.Builder()
                            .edge("BasicEdge2")
                            .entity("BasicEntity")
                            .build())
                    .build();
            final Range r = rangeF.getRangeFromPair(new Pair<>(new EntitySeed("1"), new EntitySeed("4")), summariseGroupOverRanges);
            final Range r2 = rangeF.getRangeFromPair(new Pair<>(new EntitySeed("5"), new EntitySeed("5")), summariseGroupOverRanges);
            scanner.setRanges(Arrays.asList(r, r2));
            final Iterator<Entry<Key, Value>> it = scanner.iterator();
            Entry<Key, Value> entry = it.next();
            Element readEdge = elementConverter.getFullElement(entry.getKey(), entry.getValue(), false);

            Edge expectedEdge = new Edge.Builder()
                    .group("BasicEdge2")
                    .source("4")
                    .dest("8")
                    .directed(true)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 5)
                    .build();
            expectedEdge.putProperty(AccumuloPropertyNames.COUNT, 3);

            assertEquals(expectedEdge, readEdge);
            assertEquals(5, readEdge.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER));
            assertEquals(3, readEdge.getProperty(AccumuloPropertyNames.COUNT));
            // Check we get the Result of the second provided range
            assertTrue(it.hasNext());
            entry = it.next();
            readEdge = elementConverter.getFullElement(entry.getKey(), entry.getValue(), false);
            expectedEdge = new Edge.Builder()
                    .group("BasicEdge2")
                    .source("5")
                    .dest("9")
                    .directed(true)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 2)
                    .property(AccumuloPropertyNames.COUNT, 1)
                    .build();
            assertEquals(expectedEdge, readEdge);
            //Check no additional rows are found. (For a table of this size we shouldn't see this)
            if (it.hasNext()) {
                fail("Additional row found.");
            }
        } catch (final AccumuloException | TableExistsException | TableNotFoundException e) {
            fail(this.getClass().getSimpleName() + " failed with exception: " + e);
        }
    }

}
