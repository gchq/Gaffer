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

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterator;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.parquetstore.ParquetStore;
import uk.gov.gchq.gaffer.parquetstore.ParquetStoreProperties;
import uk.gov.gchq.gaffer.parquetstore.data.DataGen;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 *
 */
public class EdgeCasesTest {

    private static Logger LOGGER = LoggerFactory.getLogger(EdgeCasesTest.class);

    private static User USER = new User();

    @BeforeClass
    public static void setup() {
        org.apache.log4j.Logger.getRootLogger().setLevel(Level.INFO);
    }

    @AfterClass
    public static void cleanUp() throws IOException {
        final FileSystem fs = FileSystem.get(new Configuration());
        final ParquetStoreProperties props = new ParquetStoreProperties();
        Path dataDir = new Path(props.getDataDir());
        fs.delete(dataDir, true);
        while (fs.listStatus(dataDir.getParent()).length == 0) {
            dataDir = dataDir.getParent();
            fs.delete(dataDir, true);
        }
    }

    private static ParquetStoreProperties getParquetStoreProperties() {
        return (ParquetStoreProperties) StoreProperties.loadStoreProperties(
                StreamUtil.storeProps(EdgeCasesTest.class));
    }

    @Test
    public void addElementsToExistingFolderTest() throws StoreException, OperationException, IOException {
        LOGGER.info("Starting addElementsToExistingFolderTest");
        final Schema gafferSchema = Schema.fromJson(EdgeCasesTest.class.getResourceAsStream("/schemaUsingStringVertexType/dataSchema.json"),
                EdgeCasesTest.class.getResourceAsStream("/schemaUsingStringVertexType/dataTypes.json"),
                EdgeCasesTest.class.getResourceAsStream("/schemaUsingStringVertexType/storeSchema.json"),
                EdgeCasesTest.class.getResourceAsStream("/schemaUsingStringVertexType/storeTypes.json"));
        final ParquetStoreProperties parquetProperties = getParquetStoreProperties();
        Graph graph = new Graph.Builder()
                .addSchemas(gafferSchema)
                .storeProperties(parquetProperties)
                .build();

        final ArrayList<Element> elements = new ArrayList<>(1);
        final HyperLogLogPlus h = new HyperLogLogPlus(5, 5);
        h.offer("A");
        h.offer("B");
        elements.add(DataGen.getEntity("BasicEntity", "vertex", (byte) 'a', 0.2, 3f, h, 5L, (short) 6,
                new java.util.Date()));
        graph.execute(new AddElements.Builder().input(elements).build(), USER);
        graph.execute(new AddElements.Builder().input(elements).build(), USER);
        CloseableIterator<? extends Element> results = graph.execute(new GetAllElements.Builder().build(), USER).iterator();
        assertTrue(results.hasNext());
        Entity entity = (Entity) results.next();
        assertEquals(2, entity.getProperty("count"));
        assertFalse(results.hasNext());
    }

    @Test
    public void readElementsWithZeroElementFiles() throws IOException, OperationException, StoreException {
        LOGGER.info("Starting readElementsWithZeroElementFiles");
        final Iterable<? extends Element> elements = DataGen.generate300StringElementsWithNullProperties();

        final Schema gafferSchema = Schema.fromJson(EdgeCasesTest.class.getResourceAsStream("/schemaUsingStringVertexType/dataSchema.json"),
                EdgeCasesTest.class.getResourceAsStream("/schemaUsingStringVertexType/dataTypes.json"),
                EdgeCasesTest.class.getResourceAsStream("/schemaUsingStringVertexType/storeSchema.json"),
                EdgeCasesTest.class.getResourceAsStream("/schemaUsingStringVertexType/storeTypes.json"));
        ParquetStoreProperties parquetProperties = getParquetStoreProperties();
        parquetProperties.setAddElementsOutputFilesPerGroup(200);
        parquetProperties.setAddElementsBatchSize(1024);
        ParquetStore store = new ParquetStore(gafferSchema, parquetProperties);
        Graph graph = new Graph.Builder()
                .store(store)
                .build();
        graph.execute(new AddElements.Builder().input(elements).build(), USER);

        assertEquals(4, new File(parquetProperties.getDataDir() + "/" + store.getCurrentSnapshot() + "/graph").listFiles().length);
        final Iterable<? extends Element> retrievedElements = graph.execute(new GetAllElements(), USER);
        final Iterator<? extends Element> iter = retrievedElements.iterator();
        assertTrue(iter.hasNext());
        Edge edge = (Edge) iter.next();
        assertEquals("BasicEdge", edge.getGroup());
        assertEquals("src0", edge.getSource());
        assertEquals("dst0", edge.getDestination());
        assertEquals(false, edge.isDirected());
        assertEquals(2, (int) edge.getProperty("count"));
        edge = (Edge) iter.next();
        assertEquals("src0", edge.getSource());
        assertEquals("dst0", edge.getDestination());
        assertEquals(true, edge.isDirected());
        assertEquals(2, (int) edge.getProperty("count"));

        for (int i = 0; i <98 ; i++) {
            iter.next();
        }

        Entity e = (Entity) iter.next();
        assertEquals("BasicEntity", e.getGroup());
        assertEquals("vert0", e.getVertex());
        assertEquals(2, (int) e.getProperty("count"));
        e = (Entity) iter.next();
        assertEquals("BasicEntity", e.getGroup());
        assertEquals("vert1", e.getVertex());
        assertEquals(2, (int) e.getProperty("count"));

        for (int i = 0; i <48 ; i++) {
            iter.next();
        }

        e = (Entity) iter.next();
        assertEquals("BasicEntity2", e.getGroup());
        assertEquals("vert9", e.getVertex());
        assertEquals(2, (int) e.getProperty("count"));
        assertFalse(iter.hasNext());
    }

    @Test
    public void indexOutOfRangeTest() throws IOException, StoreException, OperationException {
        LOGGER.info("Starting indexOutOfRangeTest");
        final Schema gafferSchema = Schema.fromJson(EdgeCasesTest.class.getResourceAsStream("/schemaUsingStringVertexType/dataSchema.json"),
                EdgeCasesTest.class.getResourceAsStream("/schemaUsingStringVertexType/dataTypes.json"),
                EdgeCasesTest.class.getResourceAsStream("/schemaUsingStringVertexType/storeSchema.json"),
                EdgeCasesTest.class.getResourceAsStream("/schemaUsingStringVertexType/storeTypes.json"));
        ParquetStoreProperties parquetProperties = getParquetStoreProperties();
        parquetProperties.setAddElementsOutputFilesPerGroup(1);
        Graph graph = new Graph.Builder()
                .addSchemas(gafferSchema)
                .storeProperties(parquetProperties)
                .build();

        final ArrayList<Element> elements = new ArrayList<>(2);
        final Edge A2B = new Edge("BasicEdge", "A", "B", false);
        A2B.putProperty("count", 1);
        final Edge B2A = new Edge("BasicEdge", "B", "A", false);
        B2A.putProperty("count", 1);
        elements.add(A2B);
        elements.add(B2A);
        graph.execute(new AddElements.Builder().input(elements).build(), USER);

        ArrayList<EntitySeed> entitySeeds = new ArrayList<>(1);
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
        LOGGER.info("Starting deduplicateEdgeWhenSrcAndDstAreEqual");
        final Schema gafferSchema = Schema.fromJson(EdgeCasesTest.class.getResourceAsStream("/schemaUsingStringVertexType/dataSchema.json"),
                EdgeCasesTest.class.getResourceAsStream("/schemaUsingStringVertexType/dataTypes.json"),
                EdgeCasesTest.class.getResourceAsStream("/schemaUsingStringVertexType/storeSchema.json"),
                EdgeCasesTest.class.getResourceAsStream("/schemaUsingStringVertexType/storeTypes.json"));
        ParquetStoreProperties parquetProperties = getParquetStoreProperties();
        parquetProperties.setAddElementsOutputFilesPerGroup(1);
        Graph graph = new Graph.Builder()
                .addSchemas(gafferSchema)
                .storeProperties(parquetProperties)
                .build();

        final ArrayList<Element> elements = new ArrayList<>(2);
        final Edge A2A = new Edge("BasicEdge", "A", "A", false);
        A2A.putProperty("count", 1);
        elements.add(A2A);
        graph.execute(new AddElements.Builder().input(elements).build(), USER);

        ArrayList<EntitySeed> entitySeeds = new ArrayList<>(1);
        entitySeeds.add(new EntitySeed("A"));
        Iterable<? extends Element> results = graph.execute(new GetElements.Builder().input(entitySeeds).build(), USER);
        Iterator<? extends Element> iter = results.iterator();
        assertTrue(iter.hasNext());
        Edge edge = (Edge) iter.next();
        assertEquals("BasicEdge", edge.getGroup());
        assertEquals("A", edge.getSource());
        assertEquals("A", edge.getDestination());
        assertEquals(false, edge.isDirected());
        assertEquals(1, edge.getProperty("count"));
        assertFalse(iter.hasNext());

        results = graph.execute(new GetAllElements.Builder().build(), USER);
        iter = results.iterator();
        assertTrue(iter.hasNext());
        edge = (Edge) iter.next();
        assertEquals("BasicEdge", edge.getGroup());
        assertEquals("A", edge.getSource());
        assertEquals("A", edge.getDestination());
        assertEquals(false, edge.isDirected());
        assertEquals(1, edge.getProperty("count"));
        assertFalse(iter.hasNext());


    }
}
