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
import org.apache.hadoop.hbase.client.HTable;
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
import uk.gov.gchq.gaffer.hbasestore.operation.hdfs.handler.AddElementsFromHdfsHandler;
import uk.gov.gchq.gaffer.hbasestore.retriever.HBaseRetriever;
import uk.gov.gchq.gaffer.hbasestore.utils.TableUtils;
import uk.gov.gchq.gaffer.hdfs.operation.AddElementsFromHdfs;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.Options;
import uk.gov.gchq.gaffer.operation.graph.GraphFilters;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;
import java.io.IOException;
import java.util.Collections;
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
    public void initialise(final Schema schema, final StoreProperties properties)
            throws StoreException {
        super.initialise(schema, properties);
        TableUtils.ensureTableExists(this);
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

    /**
     * Gets the HBase table.
     *
     * @return the HBase table
     * @throws StoreException if a reference to the table could not be created.
     */
    public HTable getTable() throws StoreException {
        final TableName tableName = getProperties().getTable();
        final Connection connection = getConnection();
        try {
            return (HTable) connection.getTable(tableName);
        } catch (final IOException e) {
            CloseableUtil.close(connection);
            throw new StoreException(e);
        }
    }

    public <OP extends Output<CloseableIterable<? extends Element>> & GraphFilters & Options> HBaseRetriever<OP>
    createRetriever(final OP operation,
                    final User user,
                    final Iterable<? extends ElementId> ids,
                    final Class<?>... extraProcessors) throws StoreException {
        return new HBaseRetriever<>(this, operation, user, ids, extraProcessors);
    }

    @Override
    public Set<StoreTrait> getTraits() {
        return TRAITS;
    }

    @Override
    public boolean isValidationRequired() {
        return false;
    }

    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST_OF_RETURN_VALUE", justification = "The properties should always be HBaseProperties")
    @Override
    public HBaseProperties getProperties() {
        return (HBaseProperties) super.getProperties();
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
        try {
            addOperationHandler(AddElementsFromHdfs.class, new AddElementsFromHdfsHandler());
        } catch (final NoClassDefFoundError e) {
            LOGGER.warn("Unable to added handler for {} due to missing classes on the classpath", AddElementsFromHdfs.class.getSimpleName(), e);
        }
    }

    @Override
    protected Object doUnhandledOperation(final Operation operation, final Context context) {
        throw new UnsupportedOperationException("Operation: " + operation.getClass() + " is not supported");
    }
}
