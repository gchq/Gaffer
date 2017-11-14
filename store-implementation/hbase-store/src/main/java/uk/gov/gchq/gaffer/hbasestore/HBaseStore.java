/*
 * Copyright 2016-2017 Crown Copyright
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

package uk.gov.gchq.gaffer.hbasestore;

import com.google.common.collect.Sets;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.commonutil.CloseableUtil;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.hbasestore.operation.handler.AddElementsHandler;
import uk.gov.gchq.gaffer.hbasestore.operation.handler.GetAdjacentIdsHandler;
import uk.gov.gchq.gaffer.hbasestore.operation.handler.GetAllElementsHandler;
import uk.gov.gchq.gaffer.hbasestore.operation.handler.GetElementsHandler;
import uk.gov.gchq.gaffer.hbasestore.operation.handler.SampleElementsForSplitPointsHandler;
import uk.gov.gchq.gaffer.hbasestore.operation.handler.SplitStoreFromIterableHandler;
import uk.gov.gchq.gaffer.hbasestore.operation.hdfs.handler.AddElementsFromHdfsHandler;
import uk.gov.gchq.gaffer.hbasestore.retriever.HBaseRetriever;
import uk.gov.gchq.gaffer.hbasestore.utils.TableUtils;
import uk.gov.gchq.gaffer.hdfs.operation.AddElementsFromHdfs;
import uk.gov.gchq.gaffer.hdfs.operation.handler.HdfsSplitStoreFromFileHandler;
import uk.gov.gchq.gaffer.operation.graph.GraphFilters;
import uk.gov.gchq.gaffer.operation.impl.SampleElementsForSplitPoints;
import uk.gov.gchq.gaffer.operation.impl.SplitStoreFromFile;
import uk.gov.gchq.gaffer.operation.impl.SplitStoreFromIterable;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaElementDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaOptimiser;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.ValidationResult;

import java.io.IOException;
import java.util.Collections;
import java.util.Map.Entry;
import java.util.Set;

import static uk.gov.gchq.gaffer.store.StoreTrait.INGEST_AGGREGATION;
import static uk.gov.gchq.gaffer.store.StoreTrait.ORDERED;
import static uk.gov.gchq.gaffer.store.StoreTrait.POST_AGGREGATION_FILTERING;
import static uk.gov.gchq.gaffer.store.StoreTrait.POST_TRANSFORMATION_FILTERING;
import static uk.gov.gchq.gaffer.store.StoreTrait.PRE_AGGREGATION_FILTERING;
import static uk.gov.gchq.gaffer.store.StoreTrait.QUERY_AGGREGATION;
import static uk.gov.gchq.gaffer.store.StoreTrait.STORE_VALIDATION;
import static uk.gov.gchq.gaffer.store.StoreTrait.TRANSFORMATION;
import static uk.gov.gchq.gaffer.store.StoreTrait.VISIBILITY;

/**
 * An HBase implementation of a Gaffer {@link Store}
 * <p>
 * The key detail of the HBase implementation is that any Edge inserted by a
 * user is inserted into the hbase table twice, once with the source object
 * being put first in the rowId and once with the destination bring put first in
 * the rowId. This is to enable an edge to be found in a Range scan when providing
 * only one end of the edge.
 */
public class HBaseStore extends Store {
    public static final Set<StoreTrait> TRAITS =
            Collections.unmodifiableSet(Sets.newHashSet(
                    ORDERED,
                    VISIBILITY,
                    PRE_AGGREGATION_FILTERING,
                    POST_AGGREGATION_FILTERING,
                    POST_TRANSFORMATION_FILTERING,
                    TRANSFORMATION,
                    INGEST_AGGREGATION,
                    QUERY_AGGREGATION,
                    STORE_VALIDATION
            ));
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseStore.class);
    private Connection connection;

    @Override
    public void initialise(final String graphId, final Schema schema, final StoreProperties properties)
            throws StoreException {
        preInitialise(graphId, schema, properties);
        TableUtils.ensureTableExists(this);
    }

    /**
     * Performs general initialisation without creating the table.
     *
     * @param graphId    the graph ID
     * @param schema     the gaffer Schema
     * @param properties the hbase store properties
     * @throws StoreException the store could not be initialised.
     */
    public void preInitialise(final String graphId, final Schema schema, final StoreProperties properties)
            throws StoreException {
        setProperties(properties);
        final String deprecatedTableName = getProperties().getTableName();
        if (null == graphId && null != deprecatedTableName) {
            // Deprecated
            super.initialise(deprecatedTableName, schema, getProperties());
        } else if (null != deprecatedTableName && !deprecatedTableName.equals(graphId)) {
            throw new IllegalArgumentException(
                    "The table in store.properties should no longer be used. " +
                            "Please use a graphId instead or for now just set the graphId to be the same value as the store.properties table.");
        } else {
            super.initialise(graphId, schema, getProperties());
        }
    }

    public Configuration getConfiguration() {
        final Configuration conf = HBaseConfiguration.create();
        if (null != getProperties().getZookeepers()) {
            conf.set("hbase.zookeeper.quorum", getProperties().getZookeepers());
        }
        return conf;
    }

    public Connection getConnection() throws StoreException {
        if (null == connection || connection.isClosed()) {
            try {
                connection = ConnectionFactory.createConnection(getConfiguration());
            } catch (final IOException e) {
                throw new StoreException(e);
            }
        }
        return connection;
    }

    public TableName getTableName() {
        return TableName.valueOf(getGraphId());
    }

    /**
     * Gets the table.
     *
     * @return the table.
     * @throws StoreException if a reference to the table could not be created.
     */
    public Table getTable() throws StoreException {
        final TableName tableName = getTableName();
        final Connection connection = getConnection();
        try {
            return connection.getTable(tableName);
        } catch (final IOException e) {
            CloseableUtil.close(connection);
            throw new StoreException(e);
        }
    }

    public <OP extends Output<CloseableIterable<? extends Element>> & GraphFilters> HBaseRetriever<OP>
    createRetriever(final OP operation,
                    final User user,
                    final Iterable<? extends ElementId> ids,
                    final boolean includeMatchedVertex,
                    final Class<?>... extraProcessors) throws StoreException {
        return new HBaseRetriever<>(this, operation, user, ids, includeMatchedVertex, extraProcessors);
    }

    @Override
    protected SchemaOptimiser createSchemaOptimiser() {
        return new SchemaOptimiser(new HBaseSerialisationFactory());
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

    @Override
    public Set<StoreTrait> getTraits() {
        return TRAITS;
    }

    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST_OF_RETURN_VALUE", justification = "The properties should always be HBaseProperties")
    @Override
    public HBaseProperties getProperties() {
        return (HBaseProperties) super.getProperties();
    }

    @Override
    protected Class<HBaseProperties> getPropertiesClass() {
        return HBaseProperties.class;
    }

    @Override
    protected Class<? extends ToBytesSerialiser> getRequiredParentSerialiserClass() {
        return ToBytesSerialiser.class;
    }

    @Override
    protected OutputOperationHandler<GetElements, CloseableIterable<? extends Element>> getGetElementsHandler() {
        return new GetElementsHandler();
    }

    @Override
    protected OutputOperationHandler<GetAllElements, CloseableIterable<? extends Element>> getGetAllElementsHandler() {
        return new GetAllElementsHandler();
    }

    @Override
    protected OutputOperationHandler<GetAdjacentIds, CloseableIterable<? extends EntityId>> getAdjacentIdsHandler() {
        return new GetAdjacentIdsHandler();
    }

    @Override
    protected OperationHandler<AddElements> getAddElementsHandler() {
        return new AddElementsHandler();
    }

    @Override
    protected void addAdditionalOperationHandlers() {
        addOperationHandler(SplitStoreFromIterable.class, new SplitStoreFromIterableHandler());
        addOperationHandler(SplitStoreFromFile.class, new HdfsSplitStoreFromFileHandler());
        addOperationHandler(AddElementsFromHdfs.class, new AddElementsFromHdfsHandler());
        addOperationHandler(SampleElementsForSplitPoints.class, new SampleElementsForSplitPointsHandler());
    }
}
