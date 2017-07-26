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
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.BeforeClass;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.parquetstore.testutils.DataGen;
import uk.gov.gchq.gaffer.parquetstore.testutils.TestUtils;
import uk.gov.gchq.gaffer.parquetstore.utils.ParquetStoreConstants;
import uk.gov.gchq.gaffer.spark.operation.scalardd.ImportRDDOfElements;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertThat;

public class StringVertexSparkOperationsTest extends AbstractSparkOperationsTest {

    @BeforeClass
    public static void genData() throws OperationException, StoreException {
        Logger.getRootLogger().setLevel(Level.WARN);
        getGraph(getSchema(), getParquetStoreProperties())
                .execute(new ImportRDDOfElements.Builder()
                        .input(getElements(spark))
                        .sparkContext(spark.sparkContext())
                        .build(), USER);
    }

    @Before
    public void setup() throws StoreException {
        graph = getGraph(getSchema(), getParquetStoreProperties());
    }

    protected static Schema getSchema() {
        return Schema.fromJson(
                StringVertexSparkOperationsTest.class.getResourceAsStream("/schemaUsingStringVertexType/dataSchema.json"),
                StringVertexSparkOperationsTest.class.getResourceAsStream("/schemaUsingStringVertexType/dataTypes.json"),
                StringVertexSparkOperationsTest.class.getResourceAsStream("/schemaUsingStringVertexType/storeSchema.json"),
                StringVertexSparkOperationsTest.class.getResourceAsStream("/schemaUsingStringVertexType/storeTypes.json"));
    }

    private static RDD<Element> getElements(final SparkSession spark) {
        return DataGen.generate300StringElementsWithNullPropertiesRDD(spark);
    }

    @Override
    void checkGetDataFrameOfElements(final Dataset<Row> data) {
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

        assertThat(expectedColumns, containsInAnyOrder(actualColumns));

        //check returned elements are correct
        final List<Element> expected = new ArrayList<>(175);
        final List<Element> actual = TestUtils.convertStringRowsToElements(data);
        for (long i = 0 ; i < 25; i++) {
            expected.add(DataGen.getEdge("BasicEdge", "src" + i, "dst" + i, true, null, null, null, null, null, null, null, null, 2));
            expected.add(DataGen.getEdge("BasicEdge", "src" + i, "dst" + i, false, null, null, null, null, null, null, null, null, 2));

            expected.add(DataGen.getEdge("BasicEdge2", "src" + i, "dst" + i, true, null, null, null, null, null, null, null, null, 2));
            expected.add(DataGen.getEdge("BasicEdge2", "src" + i, "dst" + i, false, null, null, null, null, null, null, null, null, 2));

            expected.add(DataGen.getEntity("BasicEntity", "vert" + i, null, null, null, null, null, null, null, null, 2));
            expected.add(DataGen.getEntity("BasicEntity2", "vert" + i, null, null, null, null, null, null, null, null, 2));
        }
        assertThat(expected, containsInAnyOrder(actual.toArray()));
    }
}
