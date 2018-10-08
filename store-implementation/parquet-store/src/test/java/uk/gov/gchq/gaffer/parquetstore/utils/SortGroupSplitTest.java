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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
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
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.parquetstore.ParquetStore;
import uk.gov.gchq.gaffer.parquetstore.io.writer.ParquetElementWriter;
import uk.gov.gchq.gaffer.parquetstore.operation.handler.utilities.SortGroupSplit;
import uk.gov.gchq.gaffer.parquetstore.testutils.DataGen;
import uk.gov.gchq.gaffer.parquetstore.testutils.TestUtils;
import uk.gov.gchq.gaffer.spark.SparkSessionProvider;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SortGroupSplitTest {

    @Before
    public void setUp() {
        Logger.getRootLogger().setLevel(Level.WARN);
    }

    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    private void generateDate(final String inputDir) throws IOException {
        final SchemaUtils schemaUtils = new SchemaUtils(TestUtils.gafferSchema("schemaUsingLongVertexType"));
        ParquetWriter<Element> writer = new ParquetElementWriter
                .Builder(new Path(inputDir + "/0.parquet"))
                .withSparkSchema(schemaUtils.getSparkSchema(TestGroups.ENTITY))
                .withType(schemaUtils.getParquetSchema(TestGroups.ENTITY))
                .usingConverter(schemaUtils.getConverter(TestGroups.ENTITY))
                .build();
        for (int i = 9; i >= 0; i--) {
            writer.write(DataGen.getEntity(TestGroups.ENTITY, (long) i, (byte) 'b', 0.5, 7f,
                    TestUtils.MERGED_TREESET, 11L * i, (short) 13, new Date(200000L), TestUtils.MERGED_FREQMAP,
                    2, null));
            writer.write(DataGen.getEntity(TestGroups.ENTITY, (long) i, (byte) 'b', 0.5, 7f,
                    TestUtils.MERGED_TREESET, 11L * i, (short) 13, new Date(100000L), TestUtils.MERGED_FREQMAP,
                    2, null));
        }
        writer.close();
        writer = new ParquetElementWriter
                .Builder(new Path(inputDir + "/1.parquet"))
                .withSparkSchema(schemaUtils.getSparkSchema(TestGroups.ENTITY))
                .withType(schemaUtils.getParquetSchema(TestGroups.ENTITY))
                .usingConverter(schemaUtils.getConverter(TestGroups.ENTITY))
                .build();
        for (int i = 19; i >= 10; i--) {
            writer.write(DataGen.getEntity(TestGroups.ENTITY, (long) i, (byte) 'b', 0.5, 7f,
                    TestUtils.MERGED_TREESET, 11L * i, (short) 13, new Date(200000L), TestUtils.MERGED_FREQMAP,
                    2, null));
            writer.write(DataGen.getEntity(TestGroups.ENTITY, (long) i, (byte) 'b', 0.5, 7f,
                    TestUtils.MERGED_TREESET, 11L * i, (short) 13, new Date(100000L), TestUtils.MERGED_FREQMAP,
                    2, null));
        }
        writer.close();
    }

    @Test
    public void sortTest() throws IOException {
        // Given
        final FileSystem fs = FileSystem.get(new Configuration());
        final SparkSession sparkSession = SparkSessionProvider.getSparkSession();
        final String inputDir = testFolder.newFolder().getAbsolutePath();
        final String outputDir = testFolder.newFolder().getAbsolutePath() + "/output";
        generateDate(inputDir);
        final List<String> sortColumns = new ArrayList<>();
        sortColumns.add(ParquetStore.VERTEX);
        sortColumns.add("date");

        // When
        new SortGroupSplit(fs, sparkSession, sortColumns, inputDir, outputDir).call();

        // Then
        //  - Check output directory exists and contains one Parquet file
        assertTrue(fs.exists(new Path(outputDir)));
        final FileStatus[] outputFiles = fs.listStatus(new Path(outputDir), path1 -> path1.getName().endsWith(".parquet"));
        assertEquals(1, outputFiles.length);
        //  - Read results and check in correct order
        final Row[] results = (Row[]) sparkSession
                .read()
                .parquet(outputFiles[0].getPath().toString())
                .collect();
        for (int i = 0; i < 40; i++) {
            assertEquals((long) i / 2, (long) results[i].getAs(ParquetStore.VERTEX));
            assertEquals('b', ((byte[]) results[i].getAs("byte"))[0]);
            assertEquals(0.5, results[i].getAs("double"), 0.01);
            assertEquals(7f, results[i].getAs("float"), 0.01f);
            assertEquals(11L * (i / 2), (long) results[i].getAs("long"));
            assertEquals(13, (int) results[i].getAs("short"));
            if (i % 2 == 0) {
                assertEquals(new Date(100000L).getTime(), (long) results[i].getAs("date"));
            } else {
                assertEquals(new Date(200000L).getTime(), (long) results[i].getAs("date"));
            }
            assertEquals(2, (int) results[i].getAs("count"));
            Assert.assertArrayEquals(new String[]{"A", "B", "C"}, (String[]) ((WrappedArray<String>) results[i].getAs("treeSet")).array());
            assertEquals(JavaConversions$.MODULE$.mapAsScalaMap(TestUtils.MERGED_FREQMAP), results[i].getAs("freqMap"));
        }
    }
}
