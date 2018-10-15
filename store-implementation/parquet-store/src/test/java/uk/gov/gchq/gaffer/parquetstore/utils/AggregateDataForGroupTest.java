/*
 * Copyright 2017-2018. Crown Copyright
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

import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import scala.collection.JavaConversions$;
import scala.collection.mutable.WrappedArray;

import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestTypes;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.parquetstore.ParquetStore;
import uk.gov.gchq.gaffer.parquetstore.io.writer.ParquetElementWriter;
import uk.gov.gchq.gaffer.parquetstore.operation.handler.utilities.AggregateDataForGroup;
import uk.gov.gchq.gaffer.parquetstore.testutils.DataGen;
import uk.gov.gchq.gaffer.parquetstore.testutils.TestUtils;
import uk.gov.gchq.gaffer.spark.SparkSessionProvider;
import uk.gov.gchq.gaffer.types.FreqMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class AggregateDataForGroupTest {
    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    @Before
    public void setUp() {
        Logger.getRootLogger().setLevel(Level.WARN);
    }

    private static void generateData(final String file, final SchemaUtils schemaUtils) throws IOException {
        final ParquetWriter<Element> writer = new ParquetElementWriter
                .Builder(new Path(file))
                .withSparkSchema(schemaUtils.getSparkSchema(TestGroups.ENTITY))
                .withType(schemaUtils.getParquetSchema(TestGroups.ENTITY))
                .usingConverter(schemaUtils.getConverter(TestGroups.ENTITY))
                .build();
        for (int i = 19; i >= 0; i--) {
            writer.write(DataGen.getEntity(TestGroups.ENTITY, (long) i, (byte) 'a', 0.1, 3f,
                    TestUtils.getTreeSet1(), 5L * i, (short) 6, TestUtils.DATE, TestUtils.getFreqMap1(), 1, "A"));
            writer.write(DataGen.getEntity(TestGroups.ENTITY, (long) i, (byte) 'b', 0.2, 4f,
                    TestUtils.getTreeSet2(), 6L * i, (short) 7, TestUtils.DATE, TestUtils.getFreqMap2(), 1, "A"));
        }
        writer.close();
    }

    @Test
    public void aggregateDataForGroupTest() throws Exception {
        // Given
        final SchemaUtils schemaUtils = new SchemaUtils(TestUtils.gafferSchema("schemaUsingLongVertexType"));
        final String file1 = testFolder.newFolder().getAbsolutePath() + "/inputdata1.parquet";
        final String file2 = testFolder.newFolder().getAbsolutePath() + "/inputdata2.parquet";
        generateData(file1, schemaUtils);
        generateData(file2, schemaUtils);
        final SparkSession sparkSession = SparkSessionProvider.getSparkSession();
        final List<String> inputFiles = new ArrayList<>(Sets.newHashSet(file1, file2));
        final String outputFolder = testFolder.newFolder().getAbsolutePath() + "/aggregated";
        final AggregateDataForGroup aggregator = new AggregateDataForGroup(FileSystem.get(new Configuration()), schemaUtils, TestGroups.ENTITY,
                inputFiles, outputFolder, sparkSession);

        // When
        aggregator.call();

        // Then
        final FileSystem fs = FileSystem.get(new Configuration());
        Assert.assertTrue(fs.exists(new Path(outputFolder)));
        final Row[] results = (Row[]) sparkSession
                .read()
                .parquet(outputFolder)
                .sort(ParquetStore.VERTEX)
                .collect();
        for (int i = 0; i < 20; i++) {
            assertEquals((long) i, (long) results[i].getAs(ParquetStore.VERTEX));
            assertEquals('b', ((byte[]) results[i].getAs("byte"))[0]);
            assertEquals(0.6, results[i].getAs("double"), 0.01);
            assertEquals(14f, results[i].getAs("float"), 0.01f);
            assertEquals(11L * 2 * i, (long) results[i].getAs("long"));
            assertEquals(26, (int) results[i].getAs("short"));
            assertEquals(TestUtils.DATE.getTime(), (long) results[i].getAs("date"));
            assertEquals(4, (int) results[i].getAs("count"));
            assertArrayEquals(new String[]{"A", "B", "C"},
                    (String[]) ((WrappedArray<String>) results[i].getAs("treeSet")).array());
            final FreqMap mergedFreqMap = new FreqMap();
            mergedFreqMap.put("A", 4L);
            mergedFreqMap.put("B", 2L);
            mergedFreqMap.put("C", 2L);
            assertEquals(JavaConversions$.MODULE$.mapAsScalaMap(mergedFreqMap), results[i].getAs("freqMap"));
        }
    }
}
