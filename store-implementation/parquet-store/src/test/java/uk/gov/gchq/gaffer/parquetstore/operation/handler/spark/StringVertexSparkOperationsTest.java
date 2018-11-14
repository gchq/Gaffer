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
import uk.gov.gchq.gaffer.parquetstore.operation.handler.StringVertexOperationsTest;
import uk.gov.gchq.gaffer.parquetstore.testutils.TestUtils;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.List;

public class StringVertexSparkOperationsTest extends AbstractSparkOperationsTest {
    private final StringVertexOperationsTest svot = new StringVertexOperationsTest();

    @Override
    protected Schema getSchema() {
        return TestUtils.gafferSchema("schemaUsingStringVertexType");
    }

    @Override
    protected RDD<Element> getInputDataForGetAllElementsTest() {
        final List<Element> elements = svot.getInputDataForGetAllElementsTest();
        return TestUtils.getJavaSparkContext().parallelize(elements).rdd();
    }

    @Override
    protected int getNumberOfItemsInInputDataForGetAllElementsTest() {
        return svot.getInputDataForGetAllElementsTest().size();
    }

    @Override
    protected List<Element> getResultsForGetAllElementsTest() {
        return svot.getResultsForGetAllElementsTest();
    }

    @Override
    protected List<ElementSeed> getSeeds() {
        return svot.getSeeds();
    }

    @Override
    protected List<Element> getResultsForGetElementsWithSeedsRelatedTest() {
        return svot.getResultsForGetElementsWithSeedsRelatedTest();
    }

    /**
    @Override
    protected Graph genData(final boolean withVisibilities) throws IOException, OperationException, StoreException {
        final ParquetStoreProperties properties = TestUtils.getParquetStoreProperties(testFolder);
        final Graph graph = getGraph(getSchema(), properties, "StringVertexSparkOperationsTest");
        graph.execute(new ImportJavaRDDOfElements.Builder()
                .input(getElements(TestUtils.getJavaSparkContext(), withVisibilities))
                .build(), USER);
        return graph;
    }



    @Override
    protected JavaRDD<Element> getElements(final JavaSparkContext spark, final boolean withVisibilities) {
        return DataGen.generate300StringElementsWithNullPropertiesRDD(spark, withVisibilities);
    }

    @Override
    protected void checkGetDataFrameOfElements(final Dataset<Row> data, final boolean withVisibilities) {
        // check all columns are present
        final String[] actualColumns = data.columns();
        final List<String> expectedColumns = new ArrayList<>(14);
        expectedColumns.add(ParquetStoreConstants.GROUP);
        expectedColumns.add(ParquetStoreConstants.VERTEX);
        expectedColumns.add(ParquetStoreConstants.SOURCE);
        expectedColumns.add(ParquetStoreConstants.DESTINATION);
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
        final List<Element> actual = TestUtils.convertStringRowsToElements(data);
        for (long i = 0; i < 25; i++) {
            expected.add(DataGen.getEdge(TestGroups.EDGE, "src" + i, "dst" + i, true, null, null, null, null, null, null, null, null, 2, visibility));
            expected.add(DataGen.getEdge(TestGroups.EDGE, "src" + i, "dst" + i, false, null, null, null, null, null, null, null, null, 2, visibility));

            expected.add(DataGen.getEdge(TestGroups.EDGE_2, "src" + i, "dst" + i, true, null, null, null, null, null, null, null, null, 2, visibility));
            expected.add(DataGen.getEdge(TestGroups.EDGE_2, "src" + i, "dst" + i, false, null, null, null, null, null, null, null, null, 2, visibility));

            expected.add(DataGen.getEntity(TestGroups.ENTITY, "vert" + i, null, null, null, null, null, null, null, null, 2, visibility));
            expected.add(DataGen.getEntity(TestGroups.ENTITY_2, "vert" + i, null, null, null, null, null, null, null, null, 2, visibility));
        }
        assertThat(expected, containsInAnyOrder(actual.toArray()));
    }*/
}
