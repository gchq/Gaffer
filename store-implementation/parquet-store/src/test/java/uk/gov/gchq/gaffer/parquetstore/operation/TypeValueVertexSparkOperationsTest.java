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
 * limitations under the License
 */

package uk.gov.gchq.gaffer.parquetstore.operation;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Before;
import org.junit.BeforeClass;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.parquetstore.testutils.DataGen;
import uk.gov.gchq.gaffer.parquetstore.testutils.TestUtils;
import uk.gov.gchq.gaffer.parquetstore.utils.ParquetStoreConstants;
import uk.gov.gchq.gaffer.spark.operation.javardd.ImportJavaRDDOfElements;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.types.TypeValue;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertThat;

public class TypeValueVertexSparkOperationsTest extends AbstractSparkOperationsTest {

    @BeforeClass
    public static void genData() throws OperationException, StoreException {
        Logger.getRootLogger().setLevel(Level.WARN);
        getGraph(getSchema(), getParquetStoreProperties())
                .execute(new ImportJavaRDDOfElements.Builder()
                        .input(getElements(javaSparkContext))
                        .javaSparkContext(javaSparkContext)
                        .build(), USER);
    }

    @Before
    public void setup() throws StoreException {
        graph = getGraph(getSchema(), getParquetStoreProperties());
    }

    protected static Schema getSchema() {
        return Schema.fromJson(StreamUtil.openStreams(TypeValueVertexSparkOperationsTest.class, "schemaUsingTypeValueVertexType"));
    }

    private static JavaRDD<Element> getElements(final JavaSparkContext spark) {
        return DataGen.generate300TypeValueElementsRDD(spark);
    }

    @Override
    void checkGetDataFrameOfElements(final Dataset<Row> data) {
        // check all columns are present
        final String[] actualColumns = data.columns();
        final List<String> expectedColumns = new ArrayList<>(17);
        expectedColumns.add(ParquetStoreConstants.GROUP);
        expectedColumns.add(ParquetStoreConstants.VERTEX + "_type");
        expectedColumns.add(ParquetStoreConstants.VERTEX + "_value");
        expectedColumns.add(ParquetStoreConstants.SOURCE + "_type");
        expectedColumns.add(ParquetStoreConstants.SOURCE + "_value");
        expectedColumns.add(ParquetStoreConstants.DESTINATION + "_type");
        expectedColumns.add(ParquetStoreConstants.DESTINATION + "_value");
        expectedColumns.add(ParquetStoreConstants.DIRECTED);
        expectedColumns.add("byte");
        expectedColumns.add("double");
        expectedColumns.add("float");
        expectedColumns.add("treeSet");
        expectedColumns.add("long");
        expectedColumns.add("short");
        expectedColumns.add("date");
        expectedColumns.add("freqMap");
        expectedColumns.add("count");

        assertThat(expectedColumns, containsInAnyOrder(actualColumns));

        //check returned elements are correct
        final List<Element> expected = new ArrayList<>(175);
        final List<Element> actual = TestUtils.convertTypeValueRowsToElements(data);
        for (int x = 0 ; x < 25; x++) {
            final String type = "type" + (x % 5);
            final TypeValue src = new TypeValue(type, "src" + x);
            final TypeValue dst = new TypeValue(type, "dst" + (x + 1));
            final TypeValue vrt = new TypeValue(type, "vrt" + x);
            expected.add(DataGen.getEdge(TestGroups.EDGE, src, dst, true, (byte) 'b', (0.2 * x) + 0.3, 6f, TestUtils.MERGED_TREESET, (6L * x) + 5L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));
            expected.add(DataGen.getEdge(TestGroups.EDGE, src, dst, false, (byte) 'a', 0.2 * x, 2f, TestUtils.getTreeSet1(), 5L, (short) 6, TestUtils.DATE, TestUtils.getFreqMap1(), 1));
            expected.add(DataGen.getEdge(TestGroups.EDGE, src, dst, false, (byte) 'b', 0.3, 4f, TestUtils.getTreeSet2(), 6L * x, (short) 7, TestUtils.DATE1, TestUtils.getFreqMap2(), 1));

            expected.add(DataGen.getEdge(TestGroups.EDGE_2, src, dst, true, (byte) 'b', (0.2 * x) + 0.3, 6f, TestUtils.MERGED_TREESET, (6L * x) + 5L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));
            expected.add(DataGen.getEdge(TestGroups.EDGE_2, src, dst, false, (byte) 'b', (0.2 * x) + 0.3, 6f, TestUtils.MERGED_TREESET, (6L * x) + 5L, (short) 13, TestUtils.DATE1, TestUtils.MERGED_FREQMAP, 2));

            expected.add(DataGen.getEntity(TestGroups.ENTITY, vrt, (byte) 'b', 0.5, 7f, TestUtils.MERGED_TREESET, (5L * x) + (6L * x), (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));
            expected.add(DataGen.getEntity(TestGroups.ENTITY_2, vrt, (byte) 'b', 0.5, 7f, TestUtils.MERGED_TREESET, (5L * x) + (6L * x), (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2));
        }
        assertThat(expected, containsInAnyOrder(actual.toArray()));
    }
}
