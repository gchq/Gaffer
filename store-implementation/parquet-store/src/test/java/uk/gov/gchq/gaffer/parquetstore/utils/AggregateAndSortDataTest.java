/*
 * Copyright 2018. Crown Copyright
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
import uk.gov.gchq.gaffer.parquetstore.operation.handler.utilities.AggregateAndSortData;
import uk.gov.gchq.gaffer.parquetstore.testutils.DataGen;
import uk.gov.gchq.gaffer.parquetstore.testutils.TestUtils;
import uk.gov.gchq.gaffer.spark.SparkSessionProvider;
import uk.gov.gchq.gaffer.types.FreqMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * TODO: Tests for edges
 * TODO: Test that visibility is implicitly included as a group-by property?
 */
public class AggregateAndSortDataTest {
    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    @Before
    public void setUp() {
        Logger.getRootLogger().setLevel(Level.INFO);
    }

    public static void writeData(final String file, final SchemaUtils schemaUtils) throws IOException {
        final ParquetWriter<Element> writer = new ParquetElementWriter
                .Builder(new Path(file))
                .withSparkSchema(schemaUtils.getSparkSchema(TestGroups.ENTITY))
                .withType(schemaUtils.getParquetSchema(TestGroups.ENTITY))
                .usingConverter(schemaUtils.getConverter(TestGroups.ENTITY))
                .build();
        for (final Element element : generateData()) {
            writer.write(element);
        }
        writer.close();
    }

    public static List<Element> generateData() {
        final List<Element> data = new ArrayList<>();
        for (int i = 19; i >= 0; i--) {
            data.add(DataGen.getEntity(TestGroups.ENTITY, (long) i, (byte) 'a', 0.1, 3f,
                    TestUtils.getTreeSet1(), 11L * i, (short) 6, new Date(200000L), TestUtils.getFreqMap1(), 1, null));
            data.add(DataGen.getEntity(TestGroups.ENTITY, (long) i, (byte) 'b', 0.2, 4f,
                    TestUtils.getTreeSet2(), 11L * i, (short) 7, new Date(100000L), TestUtils.getFreqMap2(), 1, null));
        }
        return data;
    }

    @Test
    public void test() throws Exception {
        // Given
        final FileSystem fs = FileSystem.get(new Configuration());
        final SchemaUtils schemaUtils = new SchemaUtils(TestUtils.gafferSchema("schemaUsingLongVertexType"));
        final String file1 = testFolder.newFolder().getAbsolutePath() + "/inputdata1.parquet";
        final String file2 = testFolder.newFolder().getAbsolutePath() + "/inputdata2.parquet";
        writeData(file1, schemaUtils);
        writeData(file2, schemaUtils);
        final SparkSession sparkSession = SparkSessionProvider.getSparkSession();
        final List<String> inputFiles = new ArrayList<>(Sets.newHashSet(file1, file2));
        final String outputFolder = testFolder.newFolder().getAbsolutePath() + "/aggregated";

        // When
        new AggregateAndSortData(schemaUtils, fs, inputFiles, outputFolder, TestGroups.ENTITY, "test",false, sparkSession)
                .call();

        // Then
        Assert.assertTrue(fs.exists(new Path(outputFolder)));
        final Row[] results = (Row[]) sparkSession
                .read()
                .option("mergeSchema", true)
                .parquet(outputFolder)
                .collect();
        // Should be sorted by vertex and date
        for (int i = 0; i < 40; i++) {
            assertEquals((long) i / 2, (long) results[i].getAs(ParquetStore.VERTEX));
            assertEquals(i % 2 == 0 ? 'b' : 'a', ((byte[]) results[i].getAs("byte"))[0]);
            assertEquals(i % 2 == 0 ? 0.4 : 0.2, results[i].getAs("double"), 0.01);
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
}
