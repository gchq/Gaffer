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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.OperationException;
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

    private SchemaUtils schemaUtils;
    private FileSystem fs;

    @Before
    public void setUp() throws StoreException, IOException {
        Logger.getRootLogger().setLevel(Level.WARN);
        schemaUtils = new SchemaUtils(TestUtils.gafferSchema("schemaUsingLongVertexType"));
        fs = FileSystem.get(new Configuration());
    }

    @Test
    public void writeIterableOfEntities() throws OperationException, IOException {
        final List<Element> data = new ArrayList<>();
        for (long i = 0; i < 12; i++) {
            data.add(DataGen.getEntity(TestGroups.ENTITY, i, null, null, null, null, null, null, null, null, 1));
            data.add(DataGen.getEntity(TestGroups.ENTITY_2, i + 5, null, null, null, null, null, null, null, null, 1));
        }
        final Map<String, Map<Integer, Object>> splitPoints = new HashMap<>(2);
        splitPoints.put(TestGroups.ENTITY, new CalculateSplitPointsFromIterable(2, 2).calculateSplitsForGroup(data, TestGroups.ENTITY, true));
        splitPoints.put(TestGroups.ENTITY_2, new CalculateSplitPointsFromIterable(2, 2).calculateSplitsForGroup(data, TestGroups.ENTITY_2, true));
        final WriteUnsortedData writeUnsortedData = new WriteUnsortedData(".WriteUnsortedDataTest", schemaUtils, splitPoints);
        writeUnsortedData.writeElements(data.iterator());
        final String Entity1Split0 = ".WriteUnsortedDataTest/graph/GROUP=" + TestGroups.ENTITY + "/raw/split0";
        final String Entity1Split1 = ".WriteUnsortedDataTest/graph/GROUP=" + TestGroups.ENTITY + "/raw/split1";
        final String Entity2Split0 = ".WriteUnsortedDataTest/graph/GROUP=" + TestGroups.ENTITY_2 + "/raw/split0";
        final String Entity2Split1 = ".WriteUnsortedDataTest/graph/GROUP=" + TestGroups.ENTITY_2 + "/raw/split1";
        Assert.assertTrue(fs.exists(new Path(".WriteUnsortedDataTest")));
        Assert.assertTrue(fs.exists(new Path(".WriteUnsortedDataTest/graph/GROUP=" + TestGroups.ENTITY)));
        Assert.assertTrue(fs.exists(new Path(".WriteUnsortedDataTest/graph/GROUP=" + TestGroups.ENTITY_2)));
        Assert.assertTrue(fs.exists(new Path(Entity1Split0)));
        Assert.assertTrue(fs.exists(new Path(Entity1Split1)));
        Assert.assertTrue(fs.exists(new Path(Entity2Split0)));
        Assert.assertTrue(fs.exists(new Path(Entity2Split1)));
        Row[] results = (Row[]) TestUtils.spark.read().parquet(Entity1Split0).select(ParquetStoreConstants.VERTEX).collect();
        for (int i = 0; i < 6; i++) {
            Assert.assertEquals((long) i, results[i].get(0));
        }
        results = (Row[]) TestUtils.spark.read().parquet(Entity1Split1).select(ParquetStoreConstants.VERTEX).collect();
        for (int i = 6; i < 12; i++) {
            Assert.assertEquals((long) i, results[i - 6].get(0));
        }
        results = (Row[]) TestUtils.spark.read().parquet(Entity2Split0).select(ParquetStoreConstants.VERTEX).collect();
        for (int i = 0; i < 6; i++) {
            Assert.assertEquals((long) i + 5, results[i].get(0));
        }
        results = (Row[]) TestUtils.spark.read().parquet(Entity2Split1).select(ParquetStoreConstants.VERTEX).collect();
        for (int i = 6; i < 12; i++) {
            Assert.assertEquals((long) i + 5, results[i - 6].get(0));
        }
    }

    @Test
    public void writeIterableOfEdges() throws OperationException, IOException {
        final List<Element> data = new ArrayList<>();
        for (long i = 0; i < 12; i++) {
            data.add(DataGen.getEdge(TestGroups.EDGE, i, i + 2, true, null, null, null, null, null, null, null, null, 1));
            data.add(DataGen.getEdge(TestGroups.EDGE_2, i + 5, i + 8, false, null, null, null, null, null, null, null, null,1));
        }
        final Map<String, Map<Integer, Object>> splitPoints = new HashMap<>(2);
        splitPoints.put(TestGroups.EDGE, new CalculateSplitPointsFromIterable(2, 2).calculateSplitsForGroup(data, TestGroups.EDGE, false));
        splitPoints.put(TestGroups.EDGE_2, new CalculateSplitPointsFromIterable(2, 2).calculateSplitsForGroup(data, TestGroups.EDGE_2, false));
        final WriteUnsortedData writeUnsortedData = new WriteUnsortedData(".WriteUnsortedDataTest", schemaUtils, splitPoints);
        writeUnsortedData.writeElements(data.iterator());
        final String Edge1Split0 = ".WriteUnsortedDataTest/graph/GROUP=" + TestGroups.EDGE + "/raw/split0";
        final String Edge1Split1 = ".WriteUnsortedDataTest/graph/GROUP=" + TestGroups.EDGE + "/raw/split1";
        final String Edge2Split0 = ".WriteUnsortedDataTest/graph/GROUP=" + TestGroups.EDGE_2 + "/raw/split0";
        final String Edge2Split1 = ".WriteUnsortedDataTest/graph/GROUP=" + TestGroups.EDGE_2 + "/raw/split1";
        Assert.assertTrue(fs.exists(new Path(".WriteUnsortedDataTest")));
        Assert.assertTrue(fs.exists(new Path(".WriteUnsortedDataTest/graph/GROUP=" + TestGroups.EDGE)));
        Assert.assertTrue(fs.exists(new Path(".WriteUnsortedDataTest/graph/GROUP=" + TestGroups.EDGE_2)));
        Assert.assertTrue(fs.exists(new Path(Edge1Split0)));
        Assert.assertTrue(fs.exists(new Path(Edge1Split1)));
        Assert.assertTrue(fs.exists(new Path(Edge2Split0)));
        Assert.assertTrue(fs.exists(new Path(Edge2Split1)));
        Row[] results = (Row[]) TestUtils.spark.read().parquet(Edge1Split0).select(ParquetStoreConstants.SOURCE).collect();
        for (int i = 0; i < 6; i++) {
            Assert.assertEquals((long) i, results[i].get(0));
        }
        results = (Row[]) TestUtils.spark.read().parquet(Edge1Split1).select(ParquetStoreConstants.SOURCE).collect();
        for (int i = 6; i < 12; i++) {
            Assert.assertEquals((long) i, results[i - 6].get(0));
        }
        results = (Row[]) TestUtils.spark.read().parquet(Edge2Split0).select(ParquetStoreConstants.SOURCE).collect();
        for (int i = 0; i < 6; i++) {
            Assert.assertEquals((long) i + 5, results[i].get(0));
        }
        results = (Row[]) TestUtils.spark.read().parquet(Edge2Split1).select(ParquetStoreConstants.SOURCE).collect();
        for (int i = 6; i < 12; i++) {
            Assert.assertEquals((long) i + 5, results[i - 6].get(0));
        }
    }

    @After
    public void cleanUpData() throws IOException {
        deleteFolder(".WriteUnsortedDataTest", fs);
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