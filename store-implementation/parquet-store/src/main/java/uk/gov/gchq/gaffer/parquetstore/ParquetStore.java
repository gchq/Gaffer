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
 * limitations under the License.
 */
package uk.gov.gchq.gaffer.parquetstore;

import com.google.common.collect.Sets;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.parquetstore.operation.addelements.handler.AddElementsFromRDDHandler;
import uk.gov.gchq.gaffer.parquetstore.operation.addelements.handler.AddElementsHandler;
import uk.gov.gchq.gaffer.parquetstore.operation.getelements.handler.GetAdjacentIdsHandler;
import uk.gov.gchq.gaffer.parquetstore.operation.getelements.handler.GetAllElementsHandler;
import uk.gov.gchq.gaffer.parquetstore.operation.getelements.handler.GetDataframeOfElementsHandler;
import uk.gov.gchq.gaffer.parquetstore.operation.getelements.handler.GetElementsHandler;
import uk.gov.gchq.gaffer.parquetstore.utils.ParquetStoreConstants;
import uk.gov.gchq.gaffer.parquetstore.utils.SchemaUtils;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.spark.SparkUser;
import uk.gov.gchq.gaffer.spark.operation.dataframe.GetDataFrameOfElements;
import uk.gov.gchq.gaffer.spark.operation.scalardd.ImportRDDOfElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.SerialisationFactory;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaOptimiser;
import uk.gov.gchq.gaffer.user.User;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import static uk.gov.gchq.gaffer.store.StoreTrait.INGEST_AGGREGATION;
import static uk.gov.gchq.gaffer.store.StoreTrait.ORDERED;
import static uk.gov.gchq.gaffer.store.StoreTrait.PRE_AGGREGATION_FILTERING;

public class ParquetStore extends Store {

    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetStore.class);
    private static final Set<StoreTrait> TRAITS =
            Collections.unmodifiableSet(Sets.newHashSet(
                    ORDERED,
                    INGEST_AGGREGATION,
                    PRE_AGGREGATION_FILTERING
            ));
    private Index index;
    private SchemaUtils schemaUtils;
    private FileSystem fs;

    @Override
    protected SchemaOptimiser createSchemaOptimiser() {
        return new SchemaOptimiser(new SerialisationFactory(ParquetStoreConstants.SERIALISERS));
    }

    @Override
    public void initialise(final String graphId, final Schema schema, final StoreProperties properties) throws StoreException {
        LOGGER.info("Initialising ParquetStore");
        super.initialise(graphId, schema, properties);
        try {
            fs = FileSystem.get(new Configuration());
        } catch (final IOException e) {
            throw new StoreException("Could not connect to the file system", e);
        }
        schemaUtils = new SchemaUtils(getSchema());
        index = new Index();
        index.loadIndices(fs, this);
    }

    public FileSystem getFS() throws StoreException {
        return fs;
    }

    public SchemaUtils getSchemaUtils() {
        return schemaUtils;
    }

    public Set<StoreTrait> getTraits() {
        return TRAITS;
    }

    public boolean isValidationRequired() {
        return false;
    }

    @Override
    protected Context createContext(final User user) {
        final SparkUser sparkUser;
        if (user instanceof SparkUser) {
            sparkUser = (SparkUser) user;
            final SparkConf conf = sparkUser.getSparkSession().sparkContext().getConf();
            final String sparkSerialiser = conf.get("spark.serializer", null);
            if (sparkSerialiser == null || !"org.apache.spark.serializer.KryoSerializer".equals(sparkSerialiser)) {
                LOGGER.warn("For the best performance you should set the spark config 'spark.serializer' = 'org.apache.spark.serializer.KryoSerializer'");
            }
            final String sparkKryoRegistrator = conf.get("spark.kryo.registrator", null);
            if (sparkKryoRegistrator == null || !"uk.gov.gchq.gaffer.spark.serialisation.kryo.Registrator".equals(sparkKryoRegistrator)) {
                LOGGER.warn("For the best performance you should set the spark config 'spark.kryo.registrator' = 'uk.gov.gchq.gaffer.spark.serialisation.kryo.Registrator'");
            }

        } else {
            LOGGER.info("Setting up the Spark session using " + getProperties().getSparkMaster() + " as the master URL");
            final SparkSession spark = SparkSession.builder()
                    .appName("Gaffer Parquet Store")
                    .master(getProperties().getSparkMaster())
                    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                    .config("spark.kryo.registrator", "uk.gov.gchq.gaffer.spark.serialisation.kryo.Registrator")
                    .getOrCreate();
            sparkUser = new SparkUser(user, spark);
        }
        return super.createContext(sparkUser);
    }

    protected void addAdditionalOperationHandlers() {
        addOperationHandler(GetDataFrameOfElements.class, new GetDataframeOfElementsHandler());
        addOperationHandler(ImportRDDOfElements.class, new AddElementsFromRDDHandler());
    }

    @Override
    protected OutputOperationHandler<GetElements, CloseableIterable<? extends Element>> getGetElementsHandler() {
        return new GetElementsHandler();
    }

    @Override
    protected GetAllElementsHandler getGetAllElementsHandler() {
        return new GetAllElementsHandler();
    }

    @Override
    protected GetAdjacentIdsHandler getAdjacentIdsHandler() {
        return new GetAdjacentIdsHandler();
    }

    protected OperationHandler<? extends AddElements> getAddElementsHandler() {
        return new AddElementsHandler();
    }

    @Override
    protected Class<? extends Serialiser> getRequiredParentSerialiserClass() {
        return Serialiser.class;
    }

    @Override
    protected Object doUnhandledOperation(final Operation operation, final Context context) {
        throw new UnsupportedOperationException("Operation " + operation.getClass() + " is not getSnapshotTimestampsupported by the ParquetStore.");
    }

    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST_OF_RETURN_VALUE", justification = "The properties should always be ParquetStoreProperties")
    @Override
    public ParquetStoreProperties getProperties() {
        return (ParquetStoreProperties) super.getProperties();
    }

    public void setIndex(final Index index) {
        this.index = index;
    }

    public Index getIndex() {
        return index;
    }

    public static String getGroupDirectory(final String group, final String identifier, final String rootDir) {
        if (ParquetStoreConstants.DESTINATION.equals(identifier)) {
            return rootDir + "/" + ParquetStoreConstants.REVERSE_EDGES + "/" + ParquetStoreConstants.GROUP + "=" + group;
        } else {
            return rootDir + "/" + ParquetStoreConstants.GRAPH + "/" + ParquetStoreConstants.GROUP + "=" + group;
        }
    }
}
