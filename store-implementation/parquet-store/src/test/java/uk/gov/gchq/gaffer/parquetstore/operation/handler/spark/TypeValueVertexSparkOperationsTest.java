/*
 * Copyright 2017-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.parquetstore.operation.handler.spark;

import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;
import uk.gov.gchq.gaffer.parquetstore.ParquetStore;
import uk.gov.gchq.gaffer.parquetstore.operation.handler.TypeValueVertexOperationsTest;
import uk.gov.gchq.gaffer.parquetstore.testutils.TestUtils;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.types.FreqMap;
import uk.gov.gchq.gaffer.types.TypeValue;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TreeSet;

public class TypeValueVertexSparkOperationsTest extends AbstractSparkOperationsTest {
    private final TypeValueVertexOperationsTest tvvot = new TypeValueVertexOperationsTest();

    @Override
    protected Schema createSchema() {
        return TestUtils.gafferSchema("schemaUsingTypeValueVertexType");
    }

    @Override
    protected RDD<Element> getInputDataForGetAllElementsTest() {
        final List<Element> elements = tvvot.getInputDataForGetAllElementsTest();
        return TestUtils.getJavaSparkContext().parallelize(elements).rdd();
    }

    @Override
    protected List<Element> getInputDataForGetAllElementsTestAsList() {
        return tvvot.getInputDataForGetAllElementsTest();
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

    @Override
    protected List<Element> convertRowsToElements(final List<Row> rows) {
        final List<Element> elementList = new ArrayList<>();
        for (final Row row : rows) {
            final Element element;
            final String group = row.getAs(ParquetStore.GROUP);
            switch (group) {
                case TestGroups.ENTITY:
                    // Fall through to ENTITY_2
                case TestGroups.ENTITY_2:
                    element = new Entity(group, getTypeValue(row, ParquetStore.VERTEX));
                    break;
                case TestGroups.EDGE:
                    // Fall through to EDGE_2
                case TestGroups.EDGE_2:
                    element = new Edge(group, getTypeValue(row, ParquetStore.SOURCE), getTypeValue(row, ParquetStore.DESTINATION), row.getAs(ParquetStore.DIRECTED));
                    break;
                default:
                    throw new IllegalArgumentException("Unexpected group of " + group);
            }
            addProperties(element, row);
            elementList.add(element);
        }
        return elementList;
    }

    private static void addProperties(final Element element, final Row row) {
        element.putProperty("byte", ((byte[]) row.getAs("byte"))[0]);
        element.putProperty("double", row.getAs("double"));
        element.putProperty("float", row.getAs("float"));
        element.putProperty("treeSet", new TreeSet<String>(row.getList(row.fieldIndex("treeSet"))));
        element.putProperty("long", row.getAs("long"));
        element.putProperty("short", ((Integer) row.getAs("short")).shortValue());
        element.putProperty("date", new Date(row.getLong(row.fieldIndex("date"))));
        element.putProperty("freqMap", new FreqMap(row.getJavaMap(row.fieldIndex("freqMap"))));
        element.putProperty("count", row.getAs("count"));
    }

    private static TypeValue getTypeValue(final Row row, final String field) {
        final String type = row.getString(row.fieldIndex(field + "_type"));
        final String value = row.getString(row.fieldIndex(field + "_value"));
        return new TypeValue(type, value);
    }

    /*
    @Override
    protected Graph genData(final boolean withVisibilities) throws IOException, OperationException, StoreException {
        final ParquetStoreProperties properties = TestUtils.getParquetStoreProperties(testFolder);
        final Graph graph = getGraph(getSchema(), properties, "TypeValueVertexSparkOperationsTest");
        graph.execute(new ImportJavaRDDOfElements.Builder()
                .input(getElements(TestUtils.getJavaSparkContext(), withVisibilities))
                .build(), user);
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
