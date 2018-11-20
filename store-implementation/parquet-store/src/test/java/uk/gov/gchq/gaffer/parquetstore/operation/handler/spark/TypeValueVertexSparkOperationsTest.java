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
 * limitations under the License
 */

package uk.gov.gchq.gaffer.parquetstore.operation.handler.spark;

import org.apache.spark.rdd.RDD;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;
import uk.gov.gchq.gaffer.parquetstore.operation.handler.TypeValueVertexOperationsTest;
import uk.gov.gchq.gaffer.parquetstore.testutils.TestUtils;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.List;

import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertThat;

public class TypeValueVertexSparkOperationsTest extends AbstractSparkOperationsTest {
    private final TypeValueVertexOperationsTest tvvot = new TypeValueVertexOperationsTest();

    @Override
    protected Schema getSchema() {
        return TestUtils.gafferSchema("schemaUsingTypeValueVertexType");
    }

    @Override
    protected RDD<Element> getInputDataForGetAllElementsTest() {
        final List<Element> elements = tvvot.getInputDataForGetAllElementsTest();
        return TestUtils.getJavaSparkContext().parallelize(elements).rdd();
    }

    @Override
    protected int getNumberOfItemsInInputDataForGetAllElementsTest() {
        return tvvot.getInputDataForGetAllElementsTest().size();
    }

    @Override
    protected List<Element> getResultsForGetAllElementsTest() {
        return tvvot.getResultsForGetAllElementsTest();
    }

    @Override
    protected List<ElementSeed> getSeeds() {
        return tvvot.getSeeds();
    }

    @Override
    protected List<Element> getResultsForGetElementsWithSeedsRelatedTest() {
        return tvvot.getResultsForGetElementsWithSeedsRelatedTest();
    }

    /**
    @Override
    protected Graph genData(final boolean withVisibilities) throws IOException, OperationException, StoreException {
        final ParquetStoreProperties properties = TestUtils.getParquetStoreProperties(testFolder);
        final Graph graph = getGraph(getSchema(), properties, "TypeValueVertexSparkOperationsTest");
        graph.execute(new ImportJavaRDDOfElements.Builder()
                .input(getElements(TestUtils.getJavaSparkContext(), withVisibilities))
                .build(), USER);
        return graph;
    }

    @Override
    protected JavaRDD<Element> getElements(final JavaSparkContext spark, final boolean withVisibilities) {
        return DataGen.generate300TypeValueElementsRDD(spark, withVisibilities);
    }

    @Override
    protected void checkGetDataFrameOfElements(final Dataset<Row> data, final boolean withVisibilities) {
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
        expectedColumns.add(TestTypes.VISIBILITY);

        assertThat(expectedColumns, containsInAnyOrder(actualColumns));

        String visibility = null;
        if (withVisibilities) {
            visibility = "A";
        }

        //check returned elements are correct
        final List<Element> expected = new ArrayList<>(175);
        final List<Element> actual = TestUtils.convertTypeValueRowsToElements(data);
        for (int x = 0; x < 25; x++) {
            final String type = "type" + (x % 5);
            final TypeValue src = new TypeValue(type, "src" + x);
            final TypeValue dst = new TypeValue(type, "dst" + (x + 1));
            final TypeValue vrt = new TypeValue(type, "vrt" + x);
            expected.add(DataGen.getEdge(TestGroups.EDGE, src, dst, true, (byte) 'b', (0.2 * x) + 0.3, 6f, TestUtils.MERGED_TREESET, (6L * x) + 5L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2, visibility));
            expected.add(DataGen.getEdge(TestGroups.EDGE, src, dst, false, (byte) 'a', 0.2 * x, 2f, TestUtils.getTreeSet1(), 5L, (short) 6, TestUtils.DATE, TestUtils.getFreqMap1(), 1, visibility));
            expected.add(DataGen.getEdge(TestGroups.EDGE, src, dst, false, (byte) 'b', 0.3, 4f, TestUtils.getTreeSet2(), 6L * x, (short) 7, TestUtils.DATE1, TestUtils.getFreqMap2(), 1, visibility));

            expected.add(DataGen.getEdge(TestGroups.EDGE_2, src, dst, true, (byte) 'b', (0.2 * x) + 0.3, 6f, TestUtils.MERGED_TREESET, (6L * x) + 5L, (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2, visibility));
            expected.add(DataGen.getEdge(TestGroups.EDGE_2, src, dst, false, (byte) 'b', (0.2 * x) + 0.3, 6f, TestUtils.MERGED_TREESET, (6L * x) + 5L, (short) 13, TestUtils.DATE1, TestUtils.MERGED_FREQMAP, 2, visibility));

            expected.add(DataGen.getEntity(TestGroups.ENTITY, vrt, (byte) 'b', 0.5, 7f, TestUtils.MERGED_TREESET, (5L * x) + (6L * x), (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2, visibility));
            expected.add(DataGen.getEntity(TestGroups.ENTITY_2, vrt, (byte) 'b', 0.5, 7f, TestUtils.MERGED_TREESET, (5L * x) + (6L * x), (short) 13, TestUtils.DATE, TestUtils.MERGED_FREQMAP, 2, visibility));
        }
        assertThat(expected, containsInAnyOrder(actual.toArray()));
    }*/
}
