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
import uk.gov.gchq.gaffer.parquetstore.serialisation.impl.BooleanParquetSerialiser;
import uk.gov.gchq.gaffer.parquetstore.serialisation.impl.ByteParquetSerialiser;
import uk.gov.gchq.gaffer.parquetstore.serialisation.impl.DateParquetSerialiser;
import uk.gov.gchq.gaffer.parquetstore.serialisation.impl.DoubleParquetSerialiser;
import uk.gov.gchq.gaffer.parquetstore.serialisation.impl.FloatParquetSerialiser;
import uk.gov.gchq.gaffer.parquetstore.serialisation.impl.InLineHyperLogLogPlusParquetSerialiser;
import uk.gov.gchq.gaffer.parquetstore.serialisation.impl.IntegerParquetSerialiser;
import uk.gov.gchq.gaffer.parquetstore.serialisation.impl.LongParquetSerialiser;
import uk.gov.gchq.gaffer.parquetstore.serialisation.impl.ShortParquetSerialiser;
import uk.gov.gchq.gaffer.parquetstore.serialisation.impl.StringParquetSerialiser;
import uk.gov.gchq.gaffer.parquetstore.serialisation.impl.TypeValueParquetSerialiser;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.JavaSerialiser;
import uk.gov.gchq.gaffer.spark.SparkUser;
import uk.gov.gchq.gaffer.store.SerialisationFactory;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaOptimiser;
import uk.gov.gchq.koryphe.impl.predicate.IsEqual;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.parquetstore.ParquetStoreProperties;
import uk.gov.gchq.gaffer.spark.operation.dataframe.GetDataFrameOfElements;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.user.User;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

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
    private static final Serialiser[] SERIALISERS = new Serialiser[] {
            new StringParquetSerialiser(),
            new ByteParquetSerialiser(),
            new IntegerParquetSerialiser(),
            new LongParquetSerialiser(),
            new BooleanParquetSerialiser(),
            new DateParquetSerialiser(),
            new DoubleParquetSerialiser(),
            new FloatParquetSerialiser(),
            new InLineHyperLogLogPlusParquetSerialiser(),
            new ShortParquetSerialiser(),
            new TypeValueParquetSerialiser(),
            new JavaSerialiser()};

    static ParquetStoreProperties getParquetStoreProperties() {
        return (ParquetStoreProperties) StoreProperties.loadStoreProperties(
                AbstractSparkOperationsTest.class.getResourceAsStream("/multiUseStore.properties"));
    }

    static Graph getGraph(final Schema schema, final ParquetStoreProperties properties) throws StoreException {
        final SchemaOptimiser optimiser = new SchemaOptimiser(new SerialisationFactory(SERIALISERS));
        return new Graph.Builder()
                .addSchema(optimiser.optimise(schema, true))
                .storeProperties(properties)
                .build();
    }

    @AfterClass
    public static void cleanUpData() throws IOException {
        LOGGER.info("Cleaning up the data");
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
        Dataset<Row> data = graph.execute(new GetDataFrameOfElements.Builder().build(), USER);
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
            graph.execute(new GetDataFrameOfElements.Builder().view(view).build(), USER);
            fail();
        } catch (final OperationException e) {
            assertEquals("Views are not supported by this operation yet", e.getMessage());
        } catch (final Exception e) {
            fail();
        }
    }
}
