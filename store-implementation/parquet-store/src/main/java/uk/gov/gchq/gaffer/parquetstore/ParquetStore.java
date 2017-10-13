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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.parquetstore.index.GraphIndex;
import uk.gov.gchq.gaffer.parquetstore.operation.addelements.handler.AddElementsHandler;
import uk.gov.gchq.gaffer.parquetstore.operation.addelements.handler.ImportJavaRDDOfElementsHandler;
import uk.gov.gchq.gaffer.parquetstore.operation.addelements.handler.ImportRDDOfElementsHandler;
import uk.gov.gchq.gaffer.parquetstore.operation.getelements.handler.GetAdjacentIdsHandler;
import uk.gov.gchq.gaffer.parquetstore.operation.getelements.handler.GetAllElementsHandler;
import uk.gov.gchq.gaffer.parquetstore.operation.getelements.handler.GetDataframeOfElementsHandler;
import uk.gov.gchq.gaffer.parquetstore.operation.getelements.handler.GetElementsHandler;
import uk.gov.gchq.gaffer.parquetstore.utils.ParquetStoreConstants;
import uk.gov.gchq.gaffer.parquetstore.utils.SchemaUtils;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.spark.operation.dataframe.GetDataFrameOfElements;
import uk.gov.gchq.gaffer.spark.operation.javardd.ImportJavaRDDOfElements;
import uk.gov.gchq.gaffer.spark.operation.scalardd.ImportRDDOfElements;
import uk.gov.gchq.gaffer.store.SerialisationFactory;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaElementDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaOptimiser;
import uk.gov.gchq.koryphe.ValidationResult;

import java.io.IOException;
import java.util.Collections;
import java.util.Map.Entry;
import java.util.Set;

import static uk.gov.gchq.gaffer.store.StoreTrait.INGEST_AGGREGATION;
import static uk.gov.gchq.gaffer.store.StoreTrait.ORDERED;
import static uk.gov.gchq.gaffer.store.StoreTrait.PRE_AGGREGATION_FILTERING;
import static uk.gov.gchq.gaffer.store.StoreTrait.STORE_VALIDATION;
import static uk.gov.gchq.gaffer.store.StoreTrait.VISIBILITY;

/**
 * An implementation of {@link Store} that uses Parquet files to store the {@link Element}s.
 * <p>
 * It is designed to make the most of the Parquet file types by serialising the {@link Element}s using
 * {@link uk.gov.gchq.gaffer.parquetstore.serialisation.ParquetSerialiser}'s which also allows for Gaffer objects to be
 * stored as multiple or nested columns of primitive types.
 */
public class ParquetStore extends Store {
    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetStore.class);
    private static final Set<StoreTrait> TRAITS =
            Collections.unmodifiableSet(Sets.newHashSet(
                    ORDERED,
                    VISIBILITY,
                    INGEST_AGGREGATION,
                    PRE_AGGREGATION_FILTERING,
                    STORE_VALIDATION
            ));

    private GraphIndex graphIndex;
    private SchemaUtils schemaUtils;
    private FileSystem fs;

    @Override
    public void initialise(final String graphId, final Schema schema, final StoreProperties properties) throws StoreException {
        super.initialise(graphId, schema, properties);
        try {
            fs = FileSystem.get(new Configuration());
        } catch (final IOException e) {
            throw new StoreException("Could not connect to the file system", e);
        }
        schemaUtils = new SchemaUtils(getSchema());
        loadIndex();
    }

    public FileSystem getFS() {
        return fs;
    }

    public SchemaUtils getSchemaUtils() {
        return schemaUtils;
    }

    @Override
    public Set<StoreTrait> getTraits() {
        return TRAITS;
    }

    public String getDataDir() {
        return getProperties().getDataDir() + "/" + getGraphId();
    }

    public String getTempFilesDir() {
        return getProperties().getTempFilesDir() + "/" + getGraphId();
    }

    public void setGraphIndex(final GraphIndex graphIndex) {
        this.graphIndex = graphIndex;
    }

    public GraphIndex getGraphIndex() {
        return graphIndex;
    }

    public static String getGroupDirectory(final String group, final String column, final String rootDir) {
        if (ParquetStoreConstants.VERTEX.equals(column) || ParquetStoreConstants.SOURCE.equals(column)) {
            return rootDir + "/" + ParquetStoreConstants.GRAPH + "/" + ParquetStoreConstants.GROUP + "=" + group;
        } else {
            return rootDir + "/sortedBy=" + column + "/" + ParquetStoreConstants.GROUP + "=" + group;
        }
    }

    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST_OF_RETURN_VALUE", justification = "The properties should always be ParquetStoreProperties")
    @Override
    public ParquetStoreProperties getProperties() {
        return (ParquetStoreProperties) super.getProperties();
    }

    @Override
    protected Class<ParquetStoreProperties> getPropertiesClass() {
        return ParquetStoreProperties.class;
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

    @Override
    protected OperationHandler<? extends AddElements> getAddElementsHandler() {
        return new AddElementsHandler();
    }

    @Override
    protected void addAdditionalOperationHandlers() {
        addOperationHandler(GetDataFrameOfElements.class, new GetDataframeOfElementsHandler());
        addOperationHandler(ImportJavaRDDOfElements.class, new ImportJavaRDDOfElementsHandler());
        addOperationHandler(ImportRDDOfElements.class, new ImportRDDOfElementsHandler());

    }

    @Override
    protected Class<? extends Serialiser> getRequiredParentSerialiserClass() {
        return Serialiser.class;
    }

    @Override
    protected SchemaOptimiser createSchemaOptimiser() {
        return new SchemaOptimiser(new SerialisationFactory(ParquetStoreConstants.SERIALISERS));
    }

    @Override
    public void validateSchemas() {
        super.validateSchemas();
        validateConsistentVertex();
    }

    @Override
    protected void validateSchemaElementDefinition(final Entry<String, SchemaElementDefinition> schemaElementDefinitionEntry, final ValidationResult validationResult) {
        super.validateSchemaElementDefinition(schemaElementDefinitionEntry, validationResult);
        validateConsistentGroupByProperties(schemaElementDefinitionEntry, validationResult);
    }

    private void loadIndex() throws StoreException {
        final String rootDir = getDataDir();
        try {
            if (fs.exists(new Path(rootDir))) {
                graphIndex = new GraphIndex();
                final long snapshot = getLatestSnapshot(rootDir);
                graphIndex.readGroups(schemaUtils, rootDir + "/" + snapshot, fs);
                graphIndex.setSnapshotTimestamp(snapshot);
            }
        } catch (final IOException e) {
            throw new StoreException(e.getMessage());
        }
    }

    private long getLatestSnapshot(final String rootDir) throws StoreException {
        long latestSnapshot = 0L;
        try {
            for (final FileStatus status : fs.listStatus(new Path(rootDir))) {
                final long currentSnapshot = Long.parseLong(status.getPath().getName());
                if (latestSnapshot < currentSnapshot) {
                    latestSnapshot = currentSnapshot;
                }
            }
        } catch (final IOException e) {
            throw new StoreException(e.getMessage());
        }
        return latestSnapshot;
    }

    private void checkForOptimisedConfig(final SparkConf conf, final String key, final Class<?> optimalClass) {
        final String value = conf.get(key, null);
        if (null == value || !optimalClass.getName().equals(value)) {
            LOGGER.warn("For the best performance you should set the spark config '{}' = '{}'", key, optimalClass.getName());
        }
    }
}
