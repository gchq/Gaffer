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
import uk.gov.gchq.gaffer.parquetstore.ParquetStore;
import uk.gov.gchq.gaffer.parquetstore.data.DataGen;
import uk.gov.gchq.gaffer.spark.operation.scalardd.ImportRDDOfElements;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.schema.Schema;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class StringVertexSparkOperationsTest extends AbstractSparkOperationsTest {

    @BeforeClass
    public static void genData() throws OperationException, StoreException {
        Logger.getRootLogger().setLevel(Level.WARN);
        getGraph(getSchema(), getParquetStoreProperties()).execute(new ImportRDDOfElements.Builder().input(getElements(spark)).build(), USER);
    }

    @Before
    public void setup() throws StoreException {
        this.graph = getGraph(getSchema(), getParquetStoreProperties());
    }

    protected static Schema getSchema() {
        return Schema.fromJson(StringVertexSparkOperationsTest.class.getResourceAsStream("/schemaUsingStringVertexType/dataSchema.json"),
                StringVertexSparkOperationsTest.class.getResourceAsStream("/schemaUsingStringVertexType/dataTypes.json"),
                StringVertexSparkOperationsTest.class.getResourceAsStream("/schemaUsingStringVertexType/storeSchema.json"),
                StringVertexSparkOperationsTest.class.getResourceAsStream("/schemaUsingStringVertexType/storeTypes.json"));
    }

    private static RDD<Element> getElements(SparkSession spark) {
        return DataGen.generate300StringElementsWithNullPropertiesRDD(spark);
    }

    @Override
    void checkGetDataFrameOfElements(Dataset<Row> data) {
        assertEquals(15, data.columns().length);
        assertEquals(150L, data.count());
    }
}
