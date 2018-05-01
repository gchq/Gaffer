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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import scala.collection.JavaConversions$;
import scala.collection.mutable.WrappedArray;

import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.parquetstore.ParquetStore;
import uk.gov.gchq.gaffer.parquetstore.ParquetStoreProperties;
import uk.gov.gchq.gaffer.parquetstore.io.writer.ParquetElementWriter;
import uk.gov.gchq.gaffer.parquetstore.operation.handler.utilities.SortGroupSplit;
import uk.gov.gchq.gaffer.parquetstore.testutils.DataGen;
import uk.gov.gchq.gaffer.parquetstore.testutils.TestUtils;
import uk.gov.gchq.gaffer.spark.SparkSessionProvider;
import uk.gov.gchq.gaffer.store.StoreException;

import java.io.IOException;

public class SortDataTest {
    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    private ParquetStoreProperties generatePreAggregatedData() throws IOException {
        final ParquetStoreProperties properties = TestUtils.getParquetStoreProperties(testFolder);
        final SchemaUtils schemaUtils = new SchemaUtils(TestUtils.gafferSchema("schemaUsingLongVertexType"));

        ParquetWriter<Element> writer = new ParquetElementWriter
                .Builder(new Path(properties.getTempFilesDir() + "/SortDataTest/graph/GROUP=" + TestGroups.ENTITY + "/aggregated/split0/part-0.parquet"))
                .isEntity(true)
                .withSparkSchema(schemaUtils.getSparkSchema(TestGroups.ENTITY))
                .withType(schemaUtils.getParquetSchema(TestGroups.ENTITY))
                .usingConverter(schemaUtils.getConverter(TestGroups.ENTITY))
                .build();
        for (int i = 9; i >= 0; i--) {
            writer.write(DataGen.getEntity(TestGroups.ENTITY, (long) i, (byte) 'b', 0.5, 7f, TestUtils.MERGED_TREESET, 11L * i, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2, null));
        }
        writer.close();
        writer = new ParquetElementWriter
                .Builder(new Path(properties.getTempFilesDir() + "/SortDataTest/graph/GROUP=" + TestGroups.ENTITY + "/aggregated/split1/part-0.parquet"))
                .isEntity(true)
                .withSparkSchema(schemaUtils.getSparkSchema(TestGroups.ENTITY))
                .withType(schemaUtils.getParquetSchema(TestGroups.ENTITY))
                .usingConverter(schemaUtils.getConverter(TestGroups.ENTITY))
                .build();
        for (int i = 19; i >= 10; i--) {
            writer.write(DataGen.getEntity(TestGroups.ENTITY, (long) i, (byte) 'b', 0.5, 7f, TestUtils.MERGED_TREESET, 11L * i, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2, null));
        }
        writer.close();
        return properties;
    }

    @Test
    public void sortGroup() throws StoreException, IOException {
        final ParquetStoreProperties properties = generatePreAggregatedData();
        properties.setAddElementsOutputFilesPerGroup(2);
        final ParquetStore store = new ParquetStore();
        store.initialise("SortDataTest",
                TestUtils.gafferSchema("schemaUsingLongVertexType"), properties);
        final SparkSession sparkSession = SparkSessionProvider.getSparkSession();
        new SortGroupSplit(TestGroups.ENTITY, ParquetStoreConstants.VERTEX, store,
                sparkSession, 0).call();
        new SortGroupSplit(TestGroups.ENTITY, ParquetStoreConstants.VERTEX, store,
                sparkSession, 1).call();
        final FileSystem fs = FileSystem.get(new Configuration());
        final String entityGroup = properties.getTempFilesDir() + "/SortDataTest/graph/GROUP=" + TestGroups.ENTITY + "/sorted";
        Assert.assertTrue(fs.exists(new Path(entityGroup)));
        Row[] results = (Row[]) sparkSession.read().parquet(entityGroup + "/split0/part-00000-*").collect();
        for (int i = 0; i < 10; i++) {
            Assert.assertEquals((long) i, (long) results[i].getAs(ParquetStoreConstants.VERTEX));
            Assert.assertEquals('b', ((byte[]) results[i].getAs("byte"))[0]);
            Assert.assertEquals(0.5, results[i].getAs("double"), 0.01);
            Assert.assertEquals(7f, results[i].getAs("float"), 0.01f);
            Assert.assertEquals(11L * i, (long) results[i].getAs("long"));
            Assert.assertEquals(13, (int) results[i].getAs("short"));
            Assert.assertEquals(TestUtils.DATE.getTime(), (long) results[i].getAs("date"));
            Assert.assertEquals(2, (int) results[i].getAs("count"));
            Assert.assertArrayEquals(new String[]{"A", "B", "C"}, (String[]) ((WrappedArray<String>) results[i].getAs("treeSet")).array());
            Assert.assertEquals(JavaConversions$.MODULE$.mapAsScalaMap(TestUtils.MERGED_FREQMAP), results[i].getAs("freqMap"));
        }
        results = (Row[]) sparkSession.read().parquet(entityGroup + "/split1/part-00000-*").collect();
        for (int i = 0; i < 10; i++) {
            Assert.assertEquals((long) i + 10, (long) results[i].getAs(ParquetStoreConstants.VERTEX));
            Assert.assertEquals('b', ((byte[]) results[i].getAs("byte"))[0]);
            Assert.assertEquals(0.5, results[i].getAs("double"), 0.01);
            Assert.assertEquals(7f, results[i].getAs("float"), 0.01f);
            Assert.assertEquals(11L * (i + 10), (long) results[i].getAs("long"));
            Assert.assertEquals(13, (int) results[i].getAs("short"));
            Assert.assertEquals(TestUtils.DATE.getTime(), (long) results[i].getAs("date"));
            Assert.assertEquals(2, (int) results[i].getAs("count"));
            Assert.assertArrayEquals(new String[]{"A", "B", "C"}, (String[]) ((WrappedArray<String>) results[i].getAs("treeSet")).array());
            Assert.assertEquals(JavaConversions$.MODULE$.mapAsScalaMap(TestUtils.MERGED_FREQMAP), results[i].getAs("freqMap"));
        }
    }
}
