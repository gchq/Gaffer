/*
 * Copyright 2017. Crown Copyright
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

package uk.gov.gchq.gaffer.parquetstore.operation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestTypes;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterator;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.parquetstore.ParquetStoreProperties;
import uk.gov.gchq.gaffer.parquetstore.testutils.DataGen;
import uk.gov.gchq.gaffer.parquetstore.testutils.TestUtils;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.types.FreqMap;
import uk.gov.gchq.gaffer.user.User;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class EdgeCasesTest {
    private static User USER = new User();

    @AfterClass
    public static void cleanUp() throws IOException {
        try (final FileSystem fs = FileSystem.get(new Configuration())) {
            final ParquetStoreProperties parquetStoreProperties = getParquetStoreProperties();
            deleteFolder(parquetStoreProperties.getDataDir(), fs);
        }
    }

    private static void deleteFolder(final String path, final FileSystem fs) throws IOException {
        Path dataDir = new Path(path);
        if (fs.exists(dataDir)) {
            fs.delete(dataDir, true);
            while (fs.listStatus(dataDir.getParent()).length == 0) {
                dataDir = dataDir.getParent();
                fs.delete(dataDir, true);
            }
        }
    }

    private static ParquetStoreProperties getParquetStoreProperties() {
        return TestUtils.getParquetStoreProperties();
    }

    @Test
    public void addElementsToExistingFolderTest() throws StoreException, OperationException, IOException {
        final Schema gafferSchema = TestUtils.gafferSchema("schemaUsingStringVertexType");
        final ParquetStoreProperties parquetStoreProperties = getParquetStoreProperties();
        parquetStoreProperties.setSampleRate(1);
        parquetStoreProperties.setAddElementsOutputFilesPerGroup(1);
        Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("addElementsToExistingFolderTest")
                        .build())
                .addSchemas(gafferSchema)
                .storeProperties(parquetStoreProperties)
                .build();
        final FreqMap f2 = new FreqMap();
        f2.upsert("A", 2L);
        f2.upsert("B", 2L);
        final ArrayList<Element> elements = new ArrayList<>(1);
        elements.add(DataGen.getEntity(TestGroups.ENTITY, "vertex", (byte) 'a', 0.2, 3f, TestUtils.getTreeSet1(), 5L, (short) 6,
                TestUtils.DATE, TestUtils.getFreqMap1(), 1, null));
        graph.execute(new AddElements.Builder().input(elements).build(), USER);
        graph.execute(new AddElements.Builder().input(elements).build(), USER);
        CloseableIterator<? extends Element> results = graph.execute(new GetAllElements.Builder().build(), USER).iterator();
        final Entity expected = DataGen.getEntity(TestGroups.ENTITY, "vertex", (byte) 'a', 0.4, 6f, TestUtils.getTreeSet1(), 10L, (short) 12,
                TestUtils.DATE, f2, 2, "");
        assertTrue(results.hasNext());
        assertEquals(expected, results.next());
        assertFalse(results.hasNext());
    }

    @Test
    public void readElementsWithZeroElementFiles() throws IOException, OperationException, StoreException {
        final List<Element> elements = new ArrayList<>(2);
        elements.add(DataGen.getEntity(TestGroups.ENTITY, "vert1", null, null, null, null, null, null, null, null, 1, ""));
        elements.add(DataGen.getEntity(TestGroups.ENTITY, "vert2", null, null, null, null, null, null, null, null, 1, ""));

        final Schema gafferSchema = Schema.fromJson(StreamUtil.openStreams(EdgeCasesTest.class, "schemaUsingStringVertexType"));
        ParquetStoreProperties parquetStoreProperties = getParquetStoreProperties();
        parquetStoreProperties.setAddElementsOutputFilesPerGroup(3);
        parquetStoreProperties.setSampleRate(1);
        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("readElementsWithZeroElementFiles")
                        .build())
                .addSchema(gafferSchema)
                .storeProperties(parquetStoreProperties)
                .build();
        graph.execute(new AddElements.Builder().input(elements).build(), USER);
        final List<Element> retrievedElements = new ArrayList<>();
        final CloseableIterator<? extends Element> iter = graph.execute(new GetAllElements(), USER).iterator();
        assertTrue(iter.hasNext());
        while (iter.hasNext()) {
            retrievedElements.add(iter.next());
        }
        assertThat(elements, containsInAnyOrder(retrievedElements.toArray()));
    }

    @Test
    public void indexOutOfRangeTest() throws IOException, StoreException, OperationException {
        final Schema gafferSchema = TestUtils.gafferSchema("schemaUsingStringVertexType");
        final ParquetStoreProperties parquetStoreProperties = getParquetStoreProperties();
        parquetStoreProperties.setAddElementsOutputFilesPerGroup(1);
        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("indexOutOfRangeTest")
                        .build())
                .addSchemas(gafferSchema)
                .storeProperties(parquetStoreProperties)
                .build();

        final ArrayList<Element> elements = new ArrayList<>(2);
        final Edge A2B = new Edge(TestGroups.EDGE, "A", "B", false);
        A2B.putProperty("count", 1);
        final Edge B2A = new Edge(TestGroups.EDGE, "B", "A", false);
        B2A.putProperty("count", 1);
        elements.add(A2B);
        elements.add(B2A);
        graph.execute(new AddElements.Builder().input(elements).build(), USER);

        final List<EntitySeed> entitySeeds = new ArrayList<>(1);
        entitySeeds.add(new EntitySeed("0"));
        Iterable<? extends Element> results = graph.execute(new GetElements.Builder().input(entitySeeds).build(), USER);
        Iterator<? extends Element> iter = results.iterator();
        assertFalse(iter.hasNext());

        entitySeeds.clear();
        entitySeeds.add(new EntitySeed("a"));
        results = graph.execute(new GetElements.Builder().input(entitySeeds).build(), USER);
        iter = results.iterator();
        assertFalse(iter.hasNext());
    }

    @Test
    public void deduplicateEdgeWhenSrcAndDstAreEqual() throws OperationException {
        final Schema gafferSchema = Schema.fromJson(StreamUtil.openStreams(EdgeCasesTest.class, "schemaUsingStringVertexType"));
        ParquetStoreProperties parquetStoreProperties = getParquetStoreProperties();
        parquetStoreProperties.setSampleRate(1);
        parquetStoreProperties.setAddElementsOutputFilesPerGroup(1);
        Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("deduplicateEdgeWhenSrcAndDstAreEqual")
                        .build())
                .addSchemas(gafferSchema)
                .storeProperties(parquetStoreProperties)
                .build();

        final ArrayList<Element> elements = new ArrayList<>(1);
        final Edge A2A = new Edge(TestGroups.EDGE, "A", "A", false);
        A2A.putProperty("count", 1);
        A2A.putProperty(TestTypes.VISIBILITY, "");
        elements.add(A2A);
        graph.execute(new AddElements.Builder().input(elements).build(), USER);

        final List<EntitySeed> entitySeeds = new ArrayList<>(1);
        entitySeeds.add(new EntitySeed("A"));
        Iterable<? extends Element> results = graph.execute(new GetElements.Builder().input(entitySeeds).build(), USER);
        Iterator<? extends Element> iter = results.iterator();
        assertTrue(iter.hasNext());
        assertEquals(A2A, iter.next());
        assertFalse(iter.hasNext());

        results = graph.execute(new GetAllElements.Builder().build(), USER);
        iter = results.iterator();
        assertTrue(iter.hasNext());
        assertEquals(A2A, iter.next());
        assertFalse(iter.hasNext());
    }

    @Test
    public void changingNumberOfFilesOutput() throws OperationException {
        final Schema gafferSchema = Schema.fromJson(StreamUtil.openStreams(EdgeCasesTest.class, "schemaUsingStringVertexType"));
        ParquetStoreProperties parquetStoreProperties = getParquetStoreProperties();
        parquetStoreProperties.setAddElementsOutputFilesPerGroup(4);
        Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("changingNumberOfFilesOutput")
                        .build())
                .addSchemas(gafferSchema)
                .storeProperties(parquetStoreProperties)
                .build();
        final List<Element> input = DataGen.generate300StringElementsWithNullProperties(false);
        graph.execute(new AddElements.Builder().input(input).build(), USER);
        CloseableIterable<? extends Element> data = graph.execute(new GetAllElements.Builder().build(), USER);
        checkData(data, 1);
        data.close();
        parquetStoreProperties.setAddElementsOutputFilesPerGroup(2);
        graph.execute(new AddElements.Builder().input(input).build(), USER);
        data = graph.execute(new GetAllElements.Builder().build(), USER);
        checkData(data, 2);
        data.close();
        parquetStoreProperties.setAddElementsOutputFilesPerGroup(10);
        graph.execute(new AddElements.Builder().input(input).build(), USER);
        data = graph.execute(new GetAllElements.Builder().build(), USER);
        checkData(data, 3);
        data.close();
    }

    private void checkData(final CloseableIterable<? extends Element> data, final int iteration) {
        final List<Element> expected = new ArrayList<>(150);
        final List<Element> actual = new ArrayList<>(150);
        final Iterator<? extends Element> dataIter = data.iterator();
        assertTrue(dataIter.hasNext());
        while (dataIter.hasNext()) {
            actual.add(dataIter.next());
        }
        for (int i = 0; i < 25; i++) {
            expected.add(DataGen.getEdge(TestGroups.EDGE, "src" + i, "dst" + i, true, null, null, null, null, null, null, null, null, 2 * iteration, ""));
            expected.add(DataGen.getEdge(TestGroups.EDGE, "src" + i, "dst" + i, false, null, null, null, null, null, null, null, null, 2 * iteration, ""));

            expected.add(DataGen.getEdge(TestGroups.EDGE_2, "src" + i, "dst" + i, true, null, null, null, null, null, null, null, null, 2 * iteration, ""));
            expected.add(DataGen.getEdge(TestGroups.EDGE_2, "src" + i, "dst" + i, false, null, null, null, null, null, null, null, null, 2 * iteration, ""));

            expected.add(DataGen.getEntity(TestGroups.ENTITY, "vert" + i, null, null, null, null, null, null, null, null, 2 * iteration, ""));
            expected.add(DataGen.getEntity(TestGroups.ENTITY_2, "vert" + i, null, null, null, null, null, null, null, null, 2 * iteration, ""));
        }
        assertThat(expected, containsInAnyOrder(actual.toArray()));
    }
}
