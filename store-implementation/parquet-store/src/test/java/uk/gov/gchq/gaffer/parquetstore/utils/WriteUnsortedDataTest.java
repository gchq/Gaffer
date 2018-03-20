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
package uk.gov.gchq.gaffer.parquetstore.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Row;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.parquetstore.ParquetStoreProperties;
import uk.gov.gchq.gaffer.parquetstore.operation.addelements.impl.CalculateSplitPointsFromIterable;
import uk.gov.gchq.gaffer.parquetstore.operation.addelements.impl.WriteUnsortedData;
import uk.gov.gchq.gaffer.parquetstore.testutils.DataGen;
import uk.gov.gchq.gaffer.parquetstore.testutils.TestUtils;
import uk.gov.gchq.gaffer.store.StoreException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WriteUnsortedDataTest {

    @BeforeClass
    public static void setUp() throws StoreException, IOException {
        Logger.getRootLogger().setLevel(Level.WARN);
    }

    @Test
    public void writeIterableOfEntities() throws OperationException, IOException {
        final SchemaUtils schemaUtils = new SchemaUtils(TestUtils.gafferSchema("schemaUsingLongVertexType"));
        final FileSystem fs = FileSystem.get(new Configuration());
        final ParquetStoreProperties props = TestUtils.getParquetStoreProperties();
        final List<Element> data = new ArrayList<>();
        for (long i = 0; i < 12; i++) {
            data.add(DataGen.getEntity(TestGroups.ENTITY, i, null, null, null, null, null, null, null, null, 1, null));
            data.add(DataGen.getEntity(TestGroups.ENTITY_2, i + 5, null, null, null, null, null, null, null, null, 1, null));
        }
        final Map<String, Map<Object, Integer>> splitPoints = new HashMap<>(2);
        splitPoints.put(TestGroups.ENTITY, new CalculateSplitPointsFromIterable(2, 2, data, TestGroups.ENTITY, true).call()._2);
        splitPoints.put(TestGroups.ENTITY_2, new CalculateSplitPointsFromIterable(2, 2, data, TestGroups.ENTITY_2, true).call()._2);
        final WriteUnsortedData writeUnsortedData = new WriteUnsortedData(props.getTempFilesDir() + "/WriteUnsortedDataTest", schemaUtils, splitPoints);
        writeUnsortedData.writeElements(data.iterator());
        final String entity1Split0 = props.getTempFilesDir() + "/WriteUnsortedDataTest/graph/GROUP=" + TestGroups.ENTITY + "/raw/split0";
        final String entity1Split1 = props.getTempFilesDir() + "/WriteUnsortedDataTest/graph/GROUP=" + TestGroups.ENTITY + "/raw/split1";
        final String entity2Split0 = props.getTempFilesDir() + "/WriteUnsortedDataTest/graph/GROUP=" + TestGroups.ENTITY_2 + "/raw/split0";
        final String entity2Split1 = props.getTempFilesDir() + "/WriteUnsortedDataTest/graph/GROUP=" + TestGroups.ENTITY_2 + "/raw/split1";
        Assert.assertTrue(fs.exists(new Path(props.getTempFilesDir())));
        Assert.assertTrue(fs.exists(new Path(props.getTempFilesDir() + "/WriteUnsortedDataTest/graph/GROUP=" + TestGroups.ENTITY)));
        Assert.assertTrue(fs.exists(new Path(props.getTempFilesDir() + "/WriteUnsortedDataTest/graph/GROUP=" + TestGroups.ENTITY_2)));
        Assert.assertTrue(fs.exists(new Path(entity1Split0)));
        Assert.assertTrue(fs.exists(new Path(entity1Split1)));
        Assert.assertTrue(fs.exists(new Path(entity2Split0)));
        Assert.assertTrue(fs.exists(new Path(entity2Split1)));
        Row[] results = (Row[]) TestUtils.spark.read().parquet(entity1Split0).select(ParquetStoreConstants.VERTEX).collect();
        for (int i = 0; i < 6; i++) {
            Assert.assertEquals((long) i, results[i].get(0));
        }
        results = (Row[]) TestUtils.spark.read().parquet(entity1Split1).select(ParquetStoreConstants.VERTEX).collect();
        for (int i = 6; i < 12; i++) {
            Assert.assertEquals((long) i, results[i - 6].get(0));
        }
        results = (Row[]) TestUtils.spark.read().parquet(entity2Split0).select(ParquetStoreConstants.VERTEX).collect();
        for (int i = 0; i < 6; i++) {
            Assert.assertEquals((long) i + 5, results[i].get(0));
        }
        results = (Row[]) TestUtils.spark.read().parquet(entity2Split1).select(ParquetStoreConstants.VERTEX).collect();
        for (int i = 6; i < 12; i++) {
            Assert.assertEquals((long) i + 5, results[i - 6].get(0));
        }
    }

    @Test
    public void writeIterableOfEdges() throws OperationException, IOException {
        final SchemaUtils schemaUtils = new SchemaUtils(TestUtils.gafferSchema("schemaUsingLongVertexType"));
        final FileSystem fs = FileSystem.get(new Configuration());
        final ParquetStoreProperties props = TestUtils.getParquetStoreProperties();
        final List<Element> data = new ArrayList<>();
        for (long i = 0; i < 12; i++) {
            data.add(DataGen.getEdge(TestGroups.EDGE, i, i + 2, true, null, null, null, null, null, null, null, null, 1, null));
            data.add(DataGen.getEdge(TestGroups.EDGE_2, i + 5, i + 8, false, null, null, null, null, null, null, null, null, 1, null));
        }
        final Map<String, Map<Object, Integer>> splitPoints = new HashMap<>(2);
        splitPoints.put(TestGroups.EDGE, new CalculateSplitPointsFromIterable(2, 2, data, TestGroups.EDGE, false).call()._2);
        splitPoints.put(TestGroups.EDGE_2, new CalculateSplitPointsFromIterable(2, 2, data, TestGroups.EDGE_2, false).call()._2);
        final WriteUnsortedData writeUnsortedData = new WriteUnsortedData(props.getTempFilesDir() + "/WriteUnsortedDataTest", schemaUtils, splitPoints);
        writeUnsortedData.writeElements(data.iterator());
        final String edge1Split0 = props.getTempFilesDir() + "/WriteUnsortedDataTest/graph/GROUP=" + TestGroups.EDGE + "/raw/split0";
        final String edge1Split1 = props.getTempFilesDir() + "/WriteUnsortedDataTest/graph/GROUP=" + TestGroups.EDGE + "/raw/split1";
        final String edge2Split0 = props.getTempFilesDir() + "/WriteUnsortedDataTest/graph/GROUP=" + TestGroups.EDGE_2 + "/raw/split0";
        final String edge2Split1 = props.getTempFilesDir() + "/WriteUnsortedDataTest/graph/GROUP=" + TestGroups.EDGE_2 + "/raw/split1";
        Assert.assertTrue(fs.exists(new Path(props.getTempFilesDir())));
        Assert.assertTrue(fs.exists(new Path(props.getTempFilesDir() + "/WriteUnsortedDataTest/graph/GROUP=" + TestGroups.EDGE)));
        Assert.assertTrue(fs.exists(new Path(props.getTempFilesDir() + "/WriteUnsortedDataTest/graph/GROUP=" + TestGroups.EDGE_2)));
        Assert.assertTrue(fs.exists(new Path(edge1Split0)));
        Assert.assertTrue(fs.exists(new Path(edge1Split1)));
        Assert.assertTrue(fs.exists(new Path(edge2Split0)));
        Assert.assertTrue(fs.exists(new Path(edge2Split1)));
        Row[] results = (Row[]) TestUtils.spark.read().parquet(edge1Split0).select(ParquetStoreConstants.SOURCE).collect();
        for (int i = 0; i < 6; i++) {
            Assert.assertEquals((long) i, results[i].get(0));
        }
        results = (Row[]) TestUtils.spark.read().parquet(edge1Split1).select(ParquetStoreConstants.SOURCE).collect();
        for (int i = 6; i < 12; i++) {
            Assert.assertEquals((long) i, results[i - 6].get(0));
        }
        results = (Row[]) TestUtils.spark.read().parquet(edge2Split0).select(ParquetStoreConstants.SOURCE).collect();
        for (int i = 0; i < 6; i++) {
            Assert.assertEquals((long) i + 5, results[i].get(0));
        }
        results = (Row[]) TestUtils.spark.read().parquet(edge2Split1).select(ParquetStoreConstants.SOURCE).collect();
        for (int i = 6; i < 12; i++) {
            Assert.assertEquals((long) i + 5, results[i - 6].get(0));
        }
    }

    @AfterClass
    public static void cleanUp() throws IOException {
        final ParquetStoreProperties props = TestUtils.getParquetStoreProperties();
        deleteFolder(props.getTempFilesDir() + "/WriteUnsortedDataTest", FileSystem.get(new Configuration()));
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

}
