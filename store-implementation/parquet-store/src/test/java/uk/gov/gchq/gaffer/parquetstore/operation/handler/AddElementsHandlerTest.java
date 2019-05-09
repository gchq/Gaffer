/*
 * Copyright 2018 Crown Copyright
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

package uk.gov.gchq.gaffer.parquetstore.operation.handler;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import scala.collection.JavaConversions$;
import scala.collection.mutable.WrappedArray;

import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.parquetstore.ParquetStore;
import uk.gov.gchq.gaffer.parquetstore.ParquetStoreProperties;
import uk.gov.gchq.gaffer.parquetstore.testutils.TestUtils;
import uk.gov.gchq.gaffer.parquetstore.utils.AggregateAndSortDataTest;
import uk.gov.gchq.gaffer.parquetstore.utils.WriteUnsortedDataTest;
import uk.gov.gchq.gaffer.spark.SparkSessionProvider;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.types.FreqMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AddElementsHandlerTest {
    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    @Before
    public void setUp() {
        Logger.getRootLogger().setLevel(Level.INFO);
    }

    @Test
    public void testOnePartitionOneGroup() throws OperationException, IOException, StoreException {
        // Given
        final List<Element> elementsToAdd = new ArrayList<>();
        elementsToAdd.addAll(AggregateAndSortDataTest.generateData());
        elementsToAdd.addAll(AggregateAndSortDataTest.generateData());
        final AddElements add = new AddElements.Builder()
                .input(elementsToAdd)
                .build();
        final Context context = new Context();
        final Schema schema = TestUtils.gafferSchema("schemaUsingLongVertexType");
        final ParquetStoreProperties storeProperties = new ParquetStoreProperties();
        final String testDir = testFolder.newFolder().getPath();
        storeProperties.setDataDir(testDir + "/data");
        storeProperties.setTempFilesDir(testDir + "/tmpdata");
        final ParquetStore store = (ParquetStore) ParquetStore.createStore("graphId", schema, storeProperties);
        final FileSystem fs = FileSystem.get(new Configuration());
        final SparkSession sparkSession = SparkSessionProvider.getSparkSession();

        // When
        new AddElementsHandler().doOperation(add, context, store);

        // Then
        // - New snapshot directory should have been created.
        final long snapshotId = store.getLatestSnapshot();
        final Path snapshotPath = new Path(testDir + "/data", ParquetStore.getSnapshotPath(snapshotId));
        assertTrue(fs.exists(snapshotPath));
        // - There should be 1 file named partition-0.parquet (and an associated .crc file) in the "group=BasicEntity"
        //   directory.
        assertTrue(fs.exists(new Path(snapshotPath, ParquetStore.getGroupSubDir(TestGroups.ENTITY, false) + "/" + ParquetStore.getFile(0))));
        assertTrue(fs.exists(new Path(snapshotPath, ParquetStore.getGroupSubDir(TestGroups.ENTITY, false) + "/." + ParquetStore.getFile(0) + ".crc")));
        // - The files should contain the data sorted by vertex and date.
        final Row[] results = (Row[]) sparkSession
                .read()
                .parquet(new Path(snapshotPath, ParquetStore.getGroupSubDir(TestGroups.ENTITY, false) + "/" + ParquetStore.getFile(0)).toString())
                .collect();
        assertEquals(40, results.length);
        for (int i = 0; i < 40; i++) {
            assertEquals((long) i / 2, (long) results[i].getAs(ParquetStore.VERTEX));
            assertEquals(i % 2 == 0 ? 'b' : 'a', ((byte[]) results[i].getAs("byte"))[0]);
            assertEquals(i % 2 == 0 ? 8f : 6f, results[i].getAs("float"), 0.01f);
            assertEquals(11L * 2 * (i / 2), (long) results[i].getAs("long"));
            assertEquals(i % 2 == 0 ? 14 : 12, (int) results[i].getAs("short"));
            assertEquals(i % 2 == 0 ? 100000L : 200000L, (long) results[i].getAs("date"));
            assertEquals(2, (int) results[i].getAs("count"));
            assertArrayEquals(i % 2 == 0 ? new String[]{"A", "C"} : new String[]{"A", "B"},
                    (String[]) ((WrappedArray<String>) results[i].getAs("treeSet")).array());
            final FreqMap mergedFreqMap1 = new FreqMap();
            mergedFreqMap1.put("A", 2L);
            mergedFreqMap1.put("B", 2L);
            final FreqMap mergedFreqMap2 = new FreqMap();
            mergedFreqMap2.put("A", 2L);
            mergedFreqMap2.put("C", 2L);
            assertEquals(JavaConversions$.MODULE$.mapAsScalaMap(i % 2 == 0 ? mergedFreqMap2 : mergedFreqMap1),
                    results[i].getAs("freqMap"));
        }
    }

    @Test
    public void testOnePartitionAllGroups() throws IOException, OperationException, StoreException {
        // Given
        final List<Element> elementsToAdd = new ArrayList<>();
        //  - Data for TestGroups.ENTITY
        elementsToAdd.addAll(AggregateAndSortDataTest.generateData());
        elementsToAdd.addAll(AggregateAndSortDataTest.generateData());
        //  - Data for TestGroups.ENTITY_2
        elementsToAdd.add(WriteUnsortedDataTest.createEntityForEntityGroup_2(10000L));
        elementsToAdd.add(WriteUnsortedDataTest.createEntityForEntityGroup_2(100L));
        elementsToAdd.add(WriteUnsortedDataTest.createEntityForEntityGroup_2(10L));
        elementsToAdd.add(WriteUnsortedDataTest.createEntityForEntityGroup_2(1L));
        //  - Data for TestGroups.EDGE
        elementsToAdd.add(WriteUnsortedDataTest.createEdgeForEdgeGroup(10000L, 1000L, true, new Date(100L)));
        elementsToAdd.add(WriteUnsortedDataTest.createEdgeForEdgeGroup(100L, 100000L, false, new Date(200L)));
        elementsToAdd.add(WriteUnsortedDataTest.createEdgeForEdgeGroup(1L, 10L, true, new Date(300L)));
        elementsToAdd.add(WriteUnsortedDataTest.createEdgeForEdgeGroup(1L, 10L, true, new Date(400L)));
        elementsToAdd.add(WriteUnsortedDataTest.createEdgeForEdgeGroup(1L, 10L, false, new Date(400L)));
        elementsToAdd.add(WriteUnsortedDataTest.createEdgeForEdgeGroup(1L, 2L, false, new Date(400L)));
        //  - Data for TestGroups.EDGE_2
        elementsToAdd.add(WriteUnsortedDataTest.createEdgeForEdgeGroup_2(10000L, 20L, true));
        elementsToAdd.add(WriteUnsortedDataTest.createEdgeForEdgeGroup_2(100L, 200L, false));
        elementsToAdd.add(WriteUnsortedDataTest.createEdgeForEdgeGroup_2(10L, 50L, true));
        elementsToAdd.add(WriteUnsortedDataTest.createEdgeForEdgeGroup_2(1L, 2000L, false));
        //  - Shuffle the list so that the order is random
        Collections.shuffle(elementsToAdd);
        final AddElements add = new AddElements.Builder()
                .input(elementsToAdd)
                .build();
        final Context context = new Context();
        final Schema schema = TestUtils.gafferSchema("schemaUsingLongVertexType");
        final ParquetStoreProperties storeProperties = new ParquetStoreProperties();
        final String testDir = testFolder.newFolder().getPath();
        storeProperties.setDataDir(testDir + "/data");
        storeProperties.setTempFilesDir(testDir + "/tmpdata");
        final ParquetStore store = (ParquetStore) ParquetStore.createStore("graphId", schema, storeProperties);
        final FileSystem fs = FileSystem.get(new Configuration());
        final SparkSession sparkSession = SparkSessionProvider.getSparkSession();

        // When
        new AddElementsHandler().doOperation(add, context, store);

        // Then
        // - New snapshot directory should have been created.
        final long snapshotId = store.getLatestSnapshot();
        final Path snapshotPath = new Path(testDir + "/data", ParquetStore.getSnapshotPath(snapshotId));
        assertTrue(fs.exists(snapshotPath));
        // - There should be 1 file named partition-0.parquet (and an associated .crc file) in the "group=BasicEntity"
        //   directory.
        assertTrue(fs.exists(new Path(snapshotPath, ParquetStore.getGroupSubDir(TestGroups.ENTITY, false) + "/" + ParquetStore.getFile(0))));
        assertTrue(fs.exists(new Path(snapshotPath, ParquetStore.getGroupSubDir(TestGroups.ENTITY, false) + "/." + ParquetStore.getFile(0) + ".crc")));
        // - The files should contain the data sorted by vertex and date.
        Row[] results = (Row[]) sparkSession
                .read()
                .parquet(new Path(snapshotPath, ParquetStore.getGroupSubDir(TestGroups.ENTITY, false) + "/" + ParquetStore.getFile(0)).toString())
                .collect();
        assertEquals(40, results.length);
        for (int i = 0; i < 40; i++) {
            assertEquals((long) i / 2, (long) results[i].getAs(ParquetStore.VERTEX));
            assertEquals(i % 2 == 0 ? 'b' : 'a', ((byte[]) results[i].getAs("byte"))[0]);
            assertEquals(i % 2 == 0 ? 8f : 6f, results[i].getAs("float"), 0.01f);
            assertEquals(11L * 2 * (i / 2), (long) results[i].getAs("long"));
            assertEquals(i % 2 == 0 ? 14 : 12, (int) results[i].getAs("short"));
            assertEquals(i % 2 == 0 ? 100000L : 200000L, (long) results[i].getAs("date"));
            assertEquals(2, (int) results[i].getAs("count"));
            assertArrayEquals(i % 2 == 0 ? new String[]{"A", "C"} : new String[]{"A", "B"},
                    (String[]) ((WrappedArray<String>) results[i].getAs("treeSet")).array());
            final FreqMap mergedFreqMap1 = new FreqMap();
            mergedFreqMap1.put("A", 2L);
            mergedFreqMap1.put("B", 2L);
            final FreqMap mergedFreqMap2 = new FreqMap();
            mergedFreqMap2.put("A", 2L);
            mergedFreqMap2.put("C", 2L);
            assertEquals(JavaConversions$.MODULE$.mapAsScalaMap(i % 2 == 0 ? mergedFreqMap2 : mergedFreqMap1),
                    results[i].getAs("freqMap"));
        }
        // - There should be 1 file named partition-0.parquet (and an associated .crc file) in the "group=BasicEntity2"
        //   directory.
        assertTrue(fs.exists(new Path(snapshotPath, ParquetStore.getGroupSubDir(TestGroups.ENTITY_2, false) + "/" + ParquetStore.getFile(0))));
        assertTrue(fs.exists(new Path(snapshotPath, ParquetStore.getGroupSubDir(TestGroups.ENTITY_2, false) + "/." + ParquetStore.getFile(0) + ".crc")));
        // - The files should contain the data sorted by vertex.
        results = (Row[]) sparkSession
                .read()
                .parquet(new Path(snapshotPath, "graph/group=BasicEntity2/" + ParquetStore.getFile(0)).toString())
                .collect();
        assertEquals(4, results.length);
        checkEntityGroup2(WriteUnsortedDataTest.createEntityForEntityGroup_2(1L), results[0]);
        checkEntityGroup2(WriteUnsortedDataTest.createEntityForEntityGroup_2(10L), results[1]);
        checkEntityGroup2(WriteUnsortedDataTest.createEntityForEntityGroup_2(100L), results[2]);
        checkEntityGroup2(WriteUnsortedDataTest.createEntityForEntityGroup_2(10000L), results[3]);
        // - There should be 1 file named partition-0.parquet (and an associated .crc file) in the "group=BasicEdge"
        //   directory and in the "reversed-group=BasicEdge" directory.
        assertTrue(fs.exists(new Path(snapshotPath, ParquetStore.getGroupSubDir(TestGroups.EDGE, false) + "/" + ParquetStore.getFile(0))));
        assertTrue(fs.exists(new Path(snapshotPath, ParquetStore.getGroupSubDir(TestGroups.EDGE, false) + "/." + ParquetStore.getFile(0) + ".crc")));
        assertTrue(fs.exists(new Path(snapshotPath, ParquetStore.getGroupSubDir(TestGroups.EDGE, true) + "/" + ParquetStore.getFile(0))));
        assertTrue(fs.exists(new Path(snapshotPath, ParquetStore.getGroupSubDir(TestGroups.EDGE, true) + "/." + ParquetStore.getFile(0) + ".crc")));
        // - The files should contain the data sorted by source, destination, directed, date
        results = (Row[]) sparkSession
                .read()
                .parquet(new Path(snapshotPath, ParquetStore.getGroupSubDir(TestGroups.EDGE, false) + "/" + ParquetStore.getFile(0)).toString())
                .collect();
        assertEquals(6, results.length);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup(1L, 2L, false, new Date(400L)),
                results[0]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup(1L, 10L, false, new Date(400L)),
                results[1]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup(1L, 10L, true, new Date(300L)),
                results[2]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup(1L, 10L, true, new Date(400L)),
                results[3]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup(100L, 100000L, false, new Date(200L)),
                results[4]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup(10000L, 1000L, true, new Date(100L)),
                results[5]);
        results = (Row[]) sparkSession
                .read()
                .parquet(new Path(snapshotPath, ParquetStore.getGroupSubDir(TestGroups.EDGE, true) + "/" + ParquetStore.getFile(0)).toString())
                .collect();
        assertEquals(6, results.length);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup(1L, 2L, false, new Date(400L)),
                results[0]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup(1L, 10L, false, new Date(400L)),
                results[1]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup(1L, 10L, true, new Date(300L)),
                results[2]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup(1L, 10L, true, new Date(400L)),
                results[3]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup(10000L, 1000L, true, new Date(100L)),
                results[4]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup(100L, 100000L, false, new Date(200L)),
                results[5]);

        // - There should be 1 file named partition-0.parquet (and an associated .crc file) in the "group=BasicEdge2"
        //   directory and in the "reversed-group=BasicEdge2" directory.
        assertTrue(fs.exists(new Path(snapshotPath, ParquetStore.getGroupSubDir(TestGroups.EDGE_2, false) + "/" + ParquetStore.getFile(0))));
        assertTrue(fs.exists(new Path(snapshotPath, ParquetStore.getGroupSubDir(TestGroups.EDGE_2, false) + "/." + ParquetStore.getFile(0) + ".crc")));
        // - The files should contain the data sorted by source, destination, directed
        results = (Row[]) sparkSession
                .read()
                .parquet(new Path(snapshotPath, ParquetStore.getGroupSubDir(TestGroups.EDGE_2, false) + "/" + ParquetStore.getFile(0)).toString())
                .collect();
        assertEquals(4, results.length);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup_2(1L, 2000L, false), results[0]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup_2(10L, 50L, true), results[1]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup_2(100L, 200L, false), results[2]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup_2(10000L, 20L, true), results[3]);
        results = (Row[]) sparkSession
                .read()
                .parquet(new Path(snapshotPath, ParquetStore.getGroupSubDir(TestGroups.EDGE_2, true) + "/" + ParquetStore.getFile(0)).toString())
                .collect();
        assertEquals(4, results.length);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup_2(10000L, 20L, true), results[0]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup_2(10L, 50L, true), results[1]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup_2(100L, 200L, false), results[2]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup_2(1L, 2000L, false), results[3]);
    }

    private void checkEdge(final Edge edge, final Row row) {
        assertEquals(edge.getSource(), row.getAs(ParquetStore.SOURCE));
        assertEquals(edge.getDestination(), row.getAs(ParquetStore.DESTINATION));
        assertEquals(edge.isDirected(), row.getAs(ParquetStore.DIRECTED));
        assertEquals(edge.getProperty("byte"), ((byte[]) row.getAs("byte"))[0]);
        assertEquals((float) edge.getProperty("float"), row.getAs("float"), 0.01F);
        assertArrayEquals(((TreeSet<String>) edge.getProperty("treeSet")).toArray(),
                (String[]) ((WrappedArray<String>) row.getAs("treeSet")).array());
        assertEquals(edge.getProperty("long"), row.getAs("long"));
        assertEquals(edge.getProperty("short").toString(), row.getAs("short").toString());
        assertEquals(((Date) edge.getProperty("date")).getTime(), (long) row.getAs("date"));
        assertEquals(JavaConversions$.MODULE$.mapAsScalaMap((Map<?, ?>) edge.getProperty("freqMap")),
                row.getAs("freqMap"));
        assertEquals(edge.getProperty("count"), (int) row.getAs("count"));
    }

    private void checkEntityGroup2(final Entity entity, final Row row) {
        assertEquals(entity.getVertex(), row.getAs(ParquetStore.VERTEX));
        assertEquals(entity.getProperty("byte"), ((byte[]) row.getAs("byte"))[0]);
        assertEquals((float) entity.getProperty("float"), row.getAs("float"), 0.01F);
        assertArrayEquals(((TreeSet<String>) entity.getProperty("treeSet")).toArray(),
                (String[]) ((WrappedArray<String>) row.getAs("treeSet")).array());
        assertEquals(entity.getProperty("long"), row.getAs("long"));
        assertEquals((short) entity.getProperty("short"), (int) row.getAs("short"));
        assertEquals(((Date) entity.getProperty("date")).getTime(), (long) row.getAs("date"));
        assertEquals(JavaConversions$.MODULE$.mapAsScalaMap((Map<?, ?>) entity.getProperty("freqMap")),
                row.getAs("freqMap"));
        assertEquals(entity.getProperty("count"), (int) row.getAs("count"));
    }

    @Test
    public void testMultiplePartitionsOneGroup() {
        // TODO
    }

    @Test
    public void testMultiplePartitionsAllGroups() {
        // TODO
    }

    @Test
    public void testRepeatedCallsOfAddElementsHandler() throws IOException, OperationException, StoreException {
        // Given
        final List<Element> elementsToAdd = new ArrayList<>();
        //  - Data for TestGroups.ENTITY
        elementsToAdd.addAll(AggregateAndSortDataTest.generateData());
        elementsToAdd.addAll(AggregateAndSortDataTest.generateData());
        //  - Data for TestGroups.ENTITY_2
        elementsToAdd.add(WriteUnsortedDataTest.createEntityForEntityGroup_2(10000L));
        elementsToAdd.add(WriteUnsortedDataTest.createEntityForEntityGroup_2(100L));
        elementsToAdd.add(WriteUnsortedDataTest.createEntityForEntityGroup_2(10L));
        elementsToAdd.add(WriteUnsortedDataTest.createEntityForEntityGroup_2(1L));
        //  - Data for TestGroups.EDGE
        elementsToAdd.add(WriteUnsortedDataTest.createEdgeForEdgeGroup(10000L, 1000L, true, new Date(100L)));
        elementsToAdd.add(WriteUnsortedDataTest.createEdgeForEdgeGroup(100L, 100000L, false, new Date(200L)));
        elementsToAdd.add(WriteUnsortedDataTest.createEdgeForEdgeGroup(1L, 10L, true, new Date(300L)));
        elementsToAdd.add(WriteUnsortedDataTest.createEdgeForEdgeGroup(1L, 10L, true, new Date(400L)));
        elementsToAdd.add(WriteUnsortedDataTest.createEdgeForEdgeGroup(1L, 10L, false, new Date(400L)));
        elementsToAdd.add(WriteUnsortedDataTest.createEdgeForEdgeGroup(1L, 2L, false, new Date(400L)));
        //  - Data for TestGroups.EDGE_2
        elementsToAdd.add(WriteUnsortedDataTest.createEdgeForEdgeGroup_2(10000L, 20L, true));
        elementsToAdd.add(WriteUnsortedDataTest.createEdgeForEdgeGroup_2(100L, 200L, false));
        elementsToAdd.add(WriteUnsortedDataTest.createEdgeForEdgeGroup_2(10L, 50L, true));
        elementsToAdd.add(WriteUnsortedDataTest.createEdgeForEdgeGroup_2(1L, 2000L, false));
        //  - Shuffle the list so that the order is random
        Collections.shuffle(elementsToAdd);
        final AddElements add = new AddElements.Builder()
                .input(elementsToAdd)
                .build();
        final Context context = new Context();
        final Schema schema = TestUtils.gafferSchema("schemaUsingLongVertexType");
        final ParquetStoreProperties storeProperties = new ParquetStoreProperties();
        final String testDir = testFolder.newFolder().getPath();
        storeProperties.setDataDir(testDir + "/data");
        storeProperties.setTempFilesDir(testDir + "/tmpdata");
        final ParquetStore store = (ParquetStore) ParquetStore.createStore("graphId", schema, storeProperties);
        final FileSystem fs = FileSystem.get(new Configuration());
        final SparkSession sparkSession = SparkSessionProvider.getSparkSession();

        // When1 - Add elementsToAdd twice
        new AddElementsHandler().doOperation(add, context, store);
        new AddElementsHandler().doOperation(add, context, store);

        // Then1
        // - New snapshot directory should have been created.
        long snapshotId = store.getLatestSnapshot();
        Path snapshotPath = new Path(testDir + "/data", ParquetStore.getSnapshotPath(snapshotId));
        assertTrue(fs.exists(snapshotPath));
        // - There should be 1 file named partition-0.parquet (and an associated .crc file) in the "group=BasicEntity"
        //   directory.
        assertTrue(fs.exists(new Path(snapshotPath, ParquetStore.getGroupSubDir(TestGroups.ENTITY, false) + "/" + ParquetStore.getFile(0))));
        assertTrue(fs.exists(new Path(snapshotPath, ParquetStore.getGroupSubDir(TestGroups.ENTITY, false) + "/." + ParquetStore.getFile(0) + ".crc")));
        // - The files should contain the data sorted by vertex and date.
        Row[] results = (Row[]) sparkSession
                .read()
                .parquet(new Path(snapshotPath, ParquetStore.getGroupSubDir(TestGroups.ENTITY, false) + "/" + ParquetStore.getFile(0)).toString())
                .collect();
        assertEquals(40, results.length);
        for (int i = 0; i < 40; i++) {
            assertEquals((long) i / 2, (long) results[i].getAs(ParquetStore.VERTEX));
            assertEquals(i % 2 == 0 ? 'b' : 'a', ((byte[]) results[i].getAs("byte"))[0]);
            assertEquals(i % 2 == 0 ? 16f : 12f, results[i].getAs("float"), 0.01f);
            assertEquals(11L * 2 * 2 * (i / 2), (long) results[i].getAs("long"));
            assertEquals(i % 2 == 0 ? 28 : 24, (int) results[i].getAs("short"));
            assertEquals(i % 2 == 0 ? 100000L : 200000L, (long) results[i].getAs("date"));
            assertEquals(4, (int) results[i].getAs("count"));
            assertArrayEquals(i % 2 == 0 ? new String[]{"A", "C"} : new String[]{"A", "B"},
                    (String[]) ((WrappedArray<String>) results[i].getAs("treeSet")).array());
            final FreqMap mergedFreqMap1 = new FreqMap();
            mergedFreqMap1.put("A", 4L);
            mergedFreqMap1.put("B", 4L);
            final FreqMap mergedFreqMap2 = new FreqMap();
            mergedFreqMap2.put("A", 4L);
            mergedFreqMap2.put("C", 4L);
            assertEquals(JavaConversions$.MODULE$.mapAsScalaMap(i % 2 == 0 ? mergedFreqMap2 : mergedFreqMap1),
                    results[i].getAs("freqMap"));
        }
        // - There should be 1 file named partition-0.parquet (and an associated .crc file) in the "group=BasicEntity2"
        //   directory.
        assertTrue(fs.exists(new Path(snapshotPath, ParquetStore.getGroupSubDir(TestGroups.ENTITY_2, false) + "/" + ParquetStore.getFile(0))));
        assertTrue(fs.exists(new Path(snapshotPath, ParquetStore.getGroupSubDir(TestGroups.ENTITY_2, false) + "/." + ParquetStore.getFile(0) + ".crc")));
        // - The files should contain the data sorted by vertex.
        results = (Row[]) sparkSession
                .read()
                .parquet(new Path(snapshotPath, ParquetStore.getGroupSubDir(TestGroups.ENTITY_2, false) + "/" + ParquetStore.getFile(0)).toString())
                .collect();
        assertEquals(8, results.length);
        checkEntityGroup2(WriteUnsortedDataTest.createEntityForEntityGroup_2(1L), results[0]);
        checkEntityGroup2(WriteUnsortedDataTest.createEntityForEntityGroup_2(1L), results[1]);
        checkEntityGroup2(WriteUnsortedDataTest.createEntityForEntityGroup_2(10L), results[2]);
        checkEntityGroup2(WriteUnsortedDataTest.createEntityForEntityGroup_2(10L), results[3]);
        checkEntityGroup2(WriteUnsortedDataTest.createEntityForEntityGroup_2(100L), results[4]);
        checkEntityGroup2(WriteUnsortedDataTest.createEntityForEntityGroup_2(100L), results[5]);
        checkEntityGroup2(WriteUnsortedDataTest.createEntityForEntityGroup_2(10000L), results[6]);
        checkEntityGroup2(WriteUnsortedDataTest.createEntityForEntityGroup_2(10000L), results[7]);
        // - There should be 1 file named partition-0.parquet (and an associated .crc file) in the "group=BasicEdge"
        //   directory and in the "reversed-group=BasicEdge" directory.
        assertTrue(fs.exists(new Path(snapshotPath, ParquetStore.getGroupSubDir(TestGroups.EDGE, false) + "/" + ParquetStore.getFile(0))));
        assertTrue(fs.exists(new Path(snapshotPath, ParquetStore.getGroupSubDir(TestGroups.EDGE, false) + "/." + ParquetStore.getFile(0) + ".crc")));
        // - The files should contain the data sorted by source, destination, directed, date
        results = (Row[]) sparkSession
                .read()
                .parquet(new Path(snapshotPath, ParquetStore.getGroupSubDir(TestGroups.EDGE, false) + "/" + ParquetStore.getFile(0)).toString())
                .collect();
        assertEquals(6, results.length);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup(1L, 2L, false, new Date(400L), (short) 2),
                results[0]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup(1L, 10L, false, new Date(400L), (short) 2),
                results[1]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup(1L, 10L, true, new Date(300L), (short) 2),
                results[2]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup(1L, 10L, true, new Date(400L), (short) 2),
                results[3]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup(100L, 100000L, false, new Date(200L), (short) 2),
                results[4]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup(10000L, 1000L, true, new Date(100L), (short) 2),
                results[5]);
        results = (Row[]) sparkSession
                .read()
                .parquet(new Path(snapshotPath, ParquetStore.getGroupSubDir(TestGroups.EDGE, true) + "/" + ParquetStore.getFile(0)).toString())
                .collect();
        assertEquals(6, results.length);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup(1L, 2L, false, new Date(400L), (short) 2),
                results[0]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup(1L, 10L, false, new Date(400L), (short) 2),
                results[1]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup(1L, 10L, true, new Date(300L), (short) 2),
                results[2]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup(1L, 10L, true, new Date(400L), (short) 2),
                results[3]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup(10000L, 1000L, true, new Date(100L), (short) 2),
                results[4]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup(100L, 100000L, false, new Date(200L), (short) 2),
                results[5]);
        // - There should be 1 file named partition-0.parquet (and an associated .crc file) in the "group=BasicEdge2"
        //   directory and in the "reversed-group=BasicEdge2" directory.
        assertTrue(fs.exists(new Path(snapshotPath, ParquetStore.getGroupSubDir(TestGroups.EDGE_2, false) + "/" + ParquetStore.getFile(0))));
        assertTrue(fs.exists(new Path(snapshotPath, ParquetStore.getGroupSubDir(TestGroups.EDGE_2, false) + "/." + ParquetStore.getFile(0) + ".crc")));
        // - The files should contain the data sorted by source, destination, directed
        results = (Row[]) sparkSession
                .read()
                .parquet(new Path(snapshotPath, ParquetStore.getGroupSubDir(TestGroups.EDGE_2, false) + "/" + ParquetStore.getFile(0)).toString())
                .collect();
        assertEquals(8, results.length);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup_2(1L, 2000L, false), results[0]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup_2(1L, 2000L, false), results[1]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup_2(10L, 50L, true), results[2]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup_2(10L, 50L, true), results[3]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup_2(100L, 200L, false), results[4]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup_2(100L, 200L, false), results[5]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup_2(10000L, 20L, true), results[6]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup_2(10000L, 20L, true), results[7]);
        results = (Row[]) sparkSession
                .read()
                .parquet(new Path(snapshotPath, ParquetStore.getGroupSubDir(TestGroups.EDGE_2, true) + "/" + ParquetStore.getFile(0)).toString())
                .collect();
        assertEquals(8, results.length);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup_2(10000L, 20L, true), results[0]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup_2(10000L, 20L, true), results[1]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup_2(10L, 50L, true), results[2]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup_2(10L, 50L, true), results[3]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup_2(100L, 200L, false), results[4]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup_2(100L, 200L, false), results[5]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup_2(1L, 2000L, false), results[6]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup_2(1L, 2000L, false), results[7]);

        // When2 - Add some elements from only TestGroups.ENTITY (this tests that groups that are unchanged after
        // an AddElements operation are correctly copied through to the new snapshot).
        elementsToAdd.clear();
        elementsToAdd.add(WriteUnsortedDataTest.createEntityForEntityGroup_2(10000L));
        elementsToAdd.add(WriteUnsortedDataTest.createEntityForEntityGroup_2(100L));
        elementsToAdd.add(WriteUnsortedDataTest.createEntityForEntityGroup_2(10L));
        elementsToAdd.add(WriteUnsortedDataTest.createEntityForEntityGroup_2(1L));
        new AddElementsHandler().doOperation(add, context, store);
        // Then1
        // - New snapshot directory should have been created.
        snapshotId = store.getLatestSnapshot();
        snapshotPath = new Path(testDir + "/data", ParquetStore.getSnapshotPath(snapshotId));
        assertTrue(fs.exists(snapshotPath));
        // - There should be 1 file named partition-0.parquet (and an associated .crc file) in the "group=BasicEntity"
        //   directory.
        assertTrue(fs.exists(new Path(snapshotPath, ParquetStore.getGroupSubDir(TestGroups.ENTITY, false) + "/" + ParquetStore.getFile(0))));
        assertTrue(fs.exists(new Path(snapshotPath, ParquetStore.getGroupSubDir(TestGroups.ENTITY, false) + "/." + ParquetStore.getFile(0) + ".crc")));
        // - The files should contain the data sorted by vertex and date.
        results = (Row[]) sparkSession
                .read()
                .parquet(new Path(snapshotPath, ParquetStore.getGroupSubDir(TestGroups.ENTITY, false) + "/" + ParquetStore.getFile(0)).toString())
                .collect();
        assertEquals(40, results.length);
        for (int i = 0; i < 40; i++) {
            assertEquals((long) i / 2, (long) results[i].getAs(ParquetStore.VERTEX));
            assertEquals(i % 2 == 0 ? 'b' : 'a', ((byte[]) results[i].getAs("byte"))[0]);
            assertEquals(i % 2 == 0 ? 16f : 12f, results[i].getAs("float"), 0.01f);
            assertEquals(11L * 2 * 2 * (i / 2), (long) results[i].getAs("long"));
            assertEquals(i % 2 == 0 ? 28 : 24, (int) results[i].getAs("short"));
            assertEquals(i % 2 == 0 ? 100000L : 200000L, (long) results[i].getAs("date"));
            assertEquals(4, (int) results[i].getAs("count"));
            assertArrayEquals(i % 2 == 0 ? new String[]{"A", "C"} : new String[]{"A", "B"},
                    (String[]) ((WrappedArray<String>) results[i].getAs("treeSet")).array());
            final FreqMap mergedFreqMap1 = new FreqMap();
            mergedFreqMap1.put("A", 4L);
            mergedFreqMap1.put("B", 4L);
            final FreqMap mergedFreqMap2 = new FreqMap();
            mergedFreqMap2.put("A", 4L);
            mergedFreqMap2.put("C", 4L);
            assertEquals(JavaConversions$.MODULE$.mapAsScalaMap(i % 2 == 0 ? mergedFreqMap2 : mergedFreqMap1),
                    results[i].getAs("freqMap"));
        }
        // - There should be 1 file named partition-0.parquet (and an associated .crc file) in the "group=BasicEntity2"
        //   directory.
        assertTrue(fs.exists(new Path(snapshotPath, ParquetStore.getGroupSubDir(TestGroups.ENTITY_2, false) + "/" + ParquetStore.getFile(0))));
        assertTrue(fs.exists(new Path(snapshotPath, ParquetStore.getGroupSubDir(TestGroups.ENTITY_2, false) + "/." + ParquetStore.getFile(0) + ".crc")));
        // - The files should contain the data sorted by vertex.
        results = (Row[]) sparkSession
                .read()
                .parquet(new Path(snapshotPath, ParquetStore.getGroupSubDir(TestGroups.ENTITY_2, false) + "/" + ParquetStore.getFile(0)).toString())
                .collect();
        assertEquals(12, results.length);
        checkEntityGroup2(WriteUnsortedDataTest.createEntityForEntityGroup_2(1L), results[0]);
        checkEntityGroup2(WriteUnsortedDataTest.createEntityForEntityGroup_2(1L), results[1]);
        checkEntityGroup2(WriteUnsortedDataTest.createEntityForEntityGroup_2(1L), results[2]);
        checkEntityGroup2(WriteUnsortedDataTest.createEntityForEntityGroup_2(10L), results[3]);
        checkEntityGroup2(WriteUnsortedDataTest.createEntityForEntityGroup_2(10L), results[4]);
        checkEntityGroup2(WriteUnsortedDataTest.createEntityForEntityGroup_2(10L), results[5]);
        checkEntityGroup2(WriteUnsortedDataTest.createEntityForEntityGroup_2(100L), results[6]);
        checkEntityGroup2(WriteUnsortedDataTest.createEntityForEntityGroup_2(100L), results[7]);
        checkEntityGroup2(WriteUnsortedDataTest.createEntityForEntityGroup_2(100L), results[8]);
        checkEntityGroup2(WriteUnsortedDataTest.createEntityForEntityGroup_2(10000L), results[9]);
        checkEntityGroup2(WriteUnsortedDataTest.createEntityForEntityGroup_2(10000L), results[10]);
        checkEntityGroup2(WriteUnsortedDataTest.createEntityForEntityGroup_2(10000L), results[11]);
        // - There should be 1 file named partition-0.parquet (and an associated .crc file) in the "group=BasicEdge"
        //   directory and in the "reversed-group=BasicEdge" directory.
        assertTrue(fs.exists(new Path(snapshotPath, ParquetStore.getGroupSubDir(TestGroups.EDGE, false) + "/" + ParquetStore.getFile(0))));
        assertTrue(fs.exists(new Path(snapshotPath, ParquetStore.getGroupSubDir(TestGroups.EDGE, false) + "/." + ParquetStore.getFile(0) + ".crc")));
        // - The files should contain the data sorted by source, destination, directed, date
        results = (Row[]) sparkSession
                .read()
                .parquet(new Path(snapshotPath, ParquetStore.getGroupSubDir(TestGroups.EDGE, false) + "/" + ParquetStore.getFile(0)).toString())
                .collect();
        assertEquals(6, results.length);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup(1L, 2L, false, new Date(400L), (short) 2),
                results[0]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup(1L, 10L, false, new Date(400L), (short) 2),
                results[1]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup(1L, 10L, true, new Date(300L), (short) 2),
                results[2]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup(1L, 10L, true, new Date(400L), (short) 2),
                results[3]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup(100L, 100000L, false, new Date(200L), (short) 2),
                results[4]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup(10000L, 1000L, true, new Date(100L), (short) 2),
                results[5]);
        results = (Row[]) sparkSession
                .read()
                .parquet(new Path(snapshotPath, ParquetStore.getGroupSubDir(TestGroups.EDGE, true) + "/" + ParquetStore.getFile(0)).toString())
                .collect();
        assertEquals(6, results.length);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup(1L, 2L, false, new Date(400L), (short) 2),
                results[0]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup(1L, 10L, false, new Date(400L), (short) 2),
                results[1]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup(1L, 10L, true, new Date(300L), (short) 2),
                results[2]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup(1L, 10L, true, new Date(400L), (short) 2),
                results[3]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup(10000L, 1000L, true, new Date(100L), (short) 2),
                results[4]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup(100L, 100000L, false, new Date(200L), (short) 2),
                results[5]);
        // - There should be 1 file named partition-0.parquet (and an associated .crc file) in the "group=BasicEdge2"
        //   directory and in the "reversed-group=BasicEdge2" directory.
        assertTrue(fs.exists(new Path(snapshotPath, ParquetStore.getGroupSubDir(TestGroups.EDGE_2, false) + "/" + ParquetStore.getFile(0))));
        assertTrue(fs.exists(new Path(snapshotPath, ParquetStore.getGroupSubDir(TestGroups.EDGE_2, false) + "/." + ParquetStore.getFile(0) + ".crc")));
        // - The files should contain the data sorted by source, destination, directed
        results = (Row[]) sparkSession
                .read()
                .parquet(new Path(snapshotPath, ParquetStore.getGroupSubDir(TestGroups.EDGE_2, false) + "/" + ParquetStore.getFile(0)).toString())
                .collect();
        assertEquals(8, results.length);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup_2(1L, 2000L, false), results[0]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup_2(1L, 2000L, false), results[1]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup_2(10L, 50L, true), results[2]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup_2(10L, 50L, true), results[3]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup_2(100L, 200L, false), results[4]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup_2(100L, 200L, false), results[5]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup_2(10000L, 20L, true), results[6]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup_2(10000L, 20L, true), results[7]);
        results = (Row[]) sparkSession
                .read()
                .parquet(new Path(snapshotPath, ParquetStore.getGroupSubDir(TestGroups.EDGE_2, true) + "/" + ParquetStore.getFile(0)).toString())
                .collect();
        assertEquals(8, results.length);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup_2(10000L, 20L, true), results[0]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup_2(10000L, 20L, true), results[1]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup_2(10L, 50L, true), results[2]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup_2(10L, 50L, true), results[3]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup_2(100L, 200L, false), results[4]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup_2(100L, 200L, false), results[5]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup_2(1L, 2000L, false), results[6]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup_2(1L, 2000L, false), results[7]);

        // - There should be 1 file named partition-0.parquet (and an associated .crc file) in the "group=BasicEdge2"
        //   directory and in the "reversed-group=BasicEdge2" directory.
        assertTrue(fs.exists(new Path(snapshotPath, ParquetStore.getGroupSubDir(TestGroups.EDGE_2, false) + "/" + ParquetStore.getFile(0))));
        assertTrue(fs.exists(new Path(snapshotPath, ParquetStore.getGroupSubDir(TestGroups.EDGE_2, false) + "/." + ParquetStore.getFile(0) + ".crc")));
        // - The files should contain the data sorted by source, destination, directed
        results = (Row[]) sparkSession
                .read()
                .parquet(new Path(snapshotPath, ParquetStore.getGroupSubDir(TestGroups.EDGE_2, false) + "/" + ParquetStore.getFile(0)).toString())
                .collect();
        assertEquals(8, results.length);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup_2(1L, 2000L, false), results[0]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup_2(1L, 2000L, false), results[1]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup_2(10L, 50L, true), results[2]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup_2(10L, 50L, true), results[3]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup_2(100L, 200L, false), results[4]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup_2(100L, 200L, false), results[5]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup_2(10000L, 20L, true), results[6]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup_2(10000L, 20L, true), results[7]);
        results = (Row[]) sparkSession
                .read()
                .parquet(new Path(snapshotPath, ParquetStore.getGroupSubDir(TestGroups.EDGE_2, true) + "/" + ParquetStore.getFile(0)).toString())
                .collect();
        assertEquals(8, results.length);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup_2(10000L, 20L, true), results[0]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup_2(10000L, 20L, true), results[1]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup_2(10L, 50L, true), results[2]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup_2(10L, 50L, true), results[3]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup_2(100L, 200L, false), results[4]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup_2(100L, 200L, false), results[5]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup_2(1L, 2000L, false), results[6]);
        checkEdge(WriteUnsortedDataTest.createEdgeForEdgeGroup_2(1L, 2000L, false), results[7]);
    }

    @Test
    public void testWhenInputIsEmpty() {
        // TODO
    }
}
