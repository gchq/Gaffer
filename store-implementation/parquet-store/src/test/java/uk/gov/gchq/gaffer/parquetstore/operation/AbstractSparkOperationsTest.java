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
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.spark.SparkUser;
import uk.gov.gchq.koryphe.impl.predicate.IsEqual;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.parquetstore.ParquetStore;
import uk.gov.gchq.gaffer.parquetstore.ParquetStoreProperties;
import uk.gov.gchq.gaffer.spark.operation.dataframe.GetDataFrameOfElements;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.user.User;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 *
 */
public abstract class AbstractSparkOperationsTest {

    abstract void checkGetDataFrameOfElements(Dataset<Row> data);

    private static Logger LOGGER = LoggerFactory.getLogger(AbstractSparkOperationsTest.class);
    static SparkSession spark = SparkSession.builder()
            .appName("Parquet Gaffer Store tests")
            .master(getParquetStoreProperties().getSparkMaster())
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.kryo.registrator", "uk.gov.gchq.gaffer.spark.serialisation.kryo.Registrator")
            .getOrCreate();
    static User USER = new SparkUser(new User(), spark);
    Graph graph;

    static ParquetStoreProperties getParquetStoreProperties() {
        return (ParquetStoreProperties) StoreProperties.loadStoreProperties(
                AbstractSparkOperationsTest.class.getResourceAsStream("/multiUseStore.properties"));
    }

    static Graph getGraph(final ParquetStore store) throws StoreException {
        return new Graph.Builder()
                .store(store)
                .build();
    }

    @AfterClass
    public static void cleanUpData() throws IOException {
        LOGGER.info("Cleaning up the data");
        final FileSystem fs = FileSystem.get(new Configuration());
        final ParquetStoreProperties props = (ParquetStoreProperties) StoreProperties.loadStoreProperties(
                StreamUtil.storeProps(AbstractSparkOperationsTest.class));
        deleteFolder(props.getDataDir(), fs);
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
        Dataset<Row> data = this.graph.execute(new GetDataFrameOfElements.Builder().build(), USER);
        checkGetDataFrameOfElements(data);
    }

    @Test
    public void getDataFrameOfElementsWithViewTest() throws OperationException {
        View view = new View.Builder()
                .entity("BasicEntity",
                        new ViewElementDefinition.Builder().preAggregationFilter(
                                new ElementFilter.Builder().select("property2").execute(new IsEqual(0.2)).build()
                        ).build())
                .build();
        try {
            this.graph.execute(new GetDataFrameOfElements.Builder().view(view).build(), USER);
            fail();
        } catch (OperationException e){
            assertEquals("Views are not supported by this operation yet", e.getMessage());
        } catch (Exception e) {
            fail();
        }
    }
}
