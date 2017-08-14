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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.parquetstore.ParquetStoreProperties;
import uk.gov.gchq.gaffer.spark.SparkConstants;
import uk.gov.gchq.gaffer.spark.SparkUser;
import uk.gov.gchq.gaffer.spark.operation.dataframe.GetDataFrameOfElements;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.impl.predicate.IsEqual;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public abstract class AbstractSparkOperationsTest {

    abstract void checkGetDataFrameOfElements(Dataset<Row> data);

    static SparkSession spark = SparkSession.builder()
            .appName("Parquet Gaffer Store tests")
            .master(getParquetStoreProperties().getSparkMaster())
            .config(SparkConstants.DRIVER_ALLOW_MULTIPLE_CONTEXTS, "true")
            .config(SparkConstants.SERIALIZER, SparkConstants.DEFAULT_SERIALIZER)
            .config(SparkConstants.KRYO_REGISTRATOR, SparkConstants.DEFAULT_KRYO_REGISTRATOR)
            .getOrCreate();
    static JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
    static User USER = new SparkUser(new User(), spark);
    Graph graph;

    static ParquetStoreProperties getParquetStoreProperties() {
        return (ParquetStoreProperties) StoreProperties.loadStoreProperties(
                AbstractSparkOperationsTest.class.getResourceAsStream("/multiUseStore.properties"));
    }

    static Graph getGraph(final Schema schema, final ParquetStoreProperties properties) throws StoreException {
        return new Graph.Builder()
                .addSchema(schema)
                .storeProperties(properties)
                .graphId("test")
                .build();
    }

    @AfterClass
    public static void cleanUpData() throws IOException {
        try (final FileSystem fs = FileSystem.get(new Configuration())) {
            final ParquetStoreProperties props = (ParquetStoreProperties) StoreProperties.loadStoreProperties(
                    StreamUtil.storeProps(AbstractSparkOperationsTest.class));
            deleteFolder(props.getDataDir(), fs);
        }
    }

    private static void deleteFolder(final String path, final FileSystem fs) throws IOException {
        Path dataDir = new Path(path);
        if (fs.exists(dataDir)) {
            fs.delete(dataDir, true);
            while (fs.listStatus(dataDir.getParent()).length == 0) {
                dataDir = dataDir.getParent();
                fs.delete(dataDir, true);
            }
        }
    }

    @Test
    public void getDataFrameOfElementsTest() throws OperationException {
        final Dataset<Row> data = graph.execute(new GetDataFrameOfElements.Builder()
                .sqlContext(spark.sqlContext())
                .build(), USER);
        checkGetDataFrameOfElements(data);
    }

    @Test
    public void getDataFrameOfElementsWithViewTest() throws OperationException {
        final View view = new View.Builder()
                .entity(TestGroups.ENTITY,
                        new ViewElementDefinition.Builder().preAggregationFilter(
                                new ElementFilter.Builder().select("double").execute(new IsEqual(0.2)).build()
                        ).build())
                .build();
        try {
            graph.execute(new GetDataFrameOfElements.Builder()
                    .sqlContext(spark.sqlContext())
                    .view(view).build(), USER);
            fail();
        } catch (final OperationException e) {
            assertEquals("Views are not supported by this operation yet", e.getMessage());
        } catch (final Exception e) {
            fail();
        }
    }
}
