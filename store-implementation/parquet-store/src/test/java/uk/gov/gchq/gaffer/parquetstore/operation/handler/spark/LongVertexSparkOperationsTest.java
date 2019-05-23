/*
 * Copyright 2017-2019 Crown Copyright
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
import uk.gov.gchq.gaffer.parquetstore.operation.handler.LongVertexOperationsTest;
import uk.gov.gchq.gaffer.parquetstore.testutils.TestUtils;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.types.FreqMap;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TreeSet;

public class LongVertexSparkOperationsTest extends AbstractSparkOperationsTest {
    private final LongVertexOperationsTest lvot = new LongVertexOperationsTest();

    @Override
    protected Schema createSchema() {
        return TestUtils.gafferSchema("schemaUsingLongVertexType");
    }

    @Override
    protected RDD<Element> getInputDataForGetAllElementsTest() {
        final List<Element> elements = lvot.getInputDataForGetAllElementsTest();
        return TestUtils.getJavaSparkContext().parallelize(elements).rdd();
    }

    @Override
    protected List<Element> getInputDataForGetAllElementsTestAsList() {
        return lvot.getInputDataForGetAllElementsTest();
    }

    @Override
    protected int getNumberOfItemsInInputDataForGetAllElementsTest() {
        return lvot.getInputDataForGetAllElementsTest().size();
    }

    @Override
    protected List<Element> getResultsForGetAllElementsTest() {
        return lvot.getResultsForGetAllElementsTest();
    }

    @Override
    protected List<ElementSeed> getSeeds() {
        return new LongVertexOperationsTest().getSeeds();
    }

    @Override
    protected List<Element> getResultsForGetElementsWithSeedsRelatedTest() {
        return lvot.getResultsForGetElementsWithSeedsRelatedTest();
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
                    element = new Entity(group, row.getAs(ParquetStore.VERTEX));
                    break;
                case TestGroups.EDGE:
                    // Fall through to EDGE_2
                case TestGroups.EDGE_2:
                    element = new Edge(group, row.getAs(ParquetStore.SOURCE), row.getAs(ParquetStore.DESTINATION), row.getAs(ParquetStore.DIRECTED));
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
}
