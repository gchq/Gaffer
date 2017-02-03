/*
 * Copyright 2016 Crown Copyright
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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.hbasestore.operation.handler.AddElementsHandler;
import uk.gov.gchq.gaffer.hbasestore.operation.handler.GetAllElementsHandler;
import uk.gov.gchq.gaffer.hbasestore.serialisation.ElementSerialisation;
import uk.gov.gchq.gaffer.hbasestore.utils.HBaseStoreConstants;
import uk.gov.gchq.gaffer.hbasestore.utils.Pair;
import uk.gov.gchq.gaffer.hbasestore.utils.TableUtils;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentEntitySeeds;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static uk.gov.gchq.gaffer.store.StoreTrait.ORDERED;


/**
 * An HBase Implementation of the Gaffer Framework
 * <p>
 * The key detail of the HBase implementation is that any Edge inserted by a
 * user is inserted into the hbase table twice, once with the source object
 * being put first in the key and once with the destination bring put first in
 * the key This is to enable an edge to be found in a Range scan when providing
 * only one end of the edge.
 */
public class HBaseStore extends Store {
    public static final Set<StoreTrait> TRAITS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(ORDERED)));
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseStore.class);
    private Connection connection = null;
    private ElementSerialisation elementSerialisation;

    @Override
    public void initialise(final Schema schema, final StoreProperties properties)
            throws StoreException {
        super.initialise(schema, properties);
        elementSerialisation = new ElementSerialisation(schema);
        TableUtils.ensureTableExists(this);
    }

    /**
     * Creates an HBase {@link Connection}
     * using the properties found in properties file associated with the
     * HBaseStore
     *
     * @return A new {@link Connection}
     * @throws StoreException if there is a failure to connect to hbase.
     */
    public Connection getConnection() throws StoreException {
        if (null == connection || connection.isClosed()) {
            connection = TableUtils.getConnection(getProperties().getInstanceName(), getProperties().getZookeepers(),
                    getProperties().getUserName(), getProperties().getPassword());
        }
        return connection;
    }


    @Override
    public <OUTPUT> OUTPUT doUnhandledOperation(final Operation<?, OUTPUT> operation, final Context context) {
        throw new UnsupportedOperationException("Operation: " + operation.getClass() + " is not supported");
    }

    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST_OF_RETURN_VALUE", justification = "The properties should always be HBaseProperties")
    @Override
    public HBaseProperties getProperties() {
        return (HBaseProperties) super.getProperties();
    }

    @Override
    protected void addAdditionalOperationHandlers() {
        // none
    }

    @Override
    protected OperationHandler<GetElements<ElementSeed, Element>, CloseableIterable<Element>> getGetElementsHandler() {
        return null;
    }

    @Override
    protected OperationHandler<GetAllElements<Element>, CloseableIterable<Element>> getGetAllElementsHandler() {
        return new GetAllElementsHandler();
    }

    @Override
    protected OperationHandler<? extends GetAdjacentEntitySeeds, CloseableIterable<EntitySeed>> getAdjacentEntitySeedsHandler() {
        return null;
    }

    @Override
    protected OperationHandler<? extends AddElements, Void> getAddElementsHandler() {
        return new AddElementsHandler();
    }

    @Override
    public Set<StoreTrait> getTraits() {
        return TRAITS;
    }

    public void addElementsOld(final Iterable<Element> elements) throws StoreException {
        final Table table = TableUtils.getTable(this);

        for (final Element element : elements) {
            Put put;
            if (element instanceof Edge) {
                final Edge edge = ((Edge) element);
                put = new Put(Bytes.toBytes(edge.getSource() + ", " + edge.getDestination()));
                put.addColumn(HBaseStoreConstants.EDGE_CF_BYTES, Bytes.toBytes(element.getGroup()), Bytes.toBytes(""));
            } else {
                final Entity entity = ((Entity) element);
                put = new Put(Bytes.toBytes(entity.getVertex().toString()));
                put.addColumn(HBaseStoreConstants.ENTITY_CF_BYTES, Bytes.toBytes(element.getGroup()), Bytes.toBytes(""));
            }
            try {
                table.put(put);
            } catch (IOException e) {
                throw new StoreException(e);
            }
        }
    }

    public void addElements(final Iterable<Element> elements) throws StoreException {
        int batchSize = getProperties().getMaxBufferSizeForBatchWriter();
        try (final Table table = TableUtils.getTable(this)) {
            final Iterator<Element> itr = elements.iterator();
            while (itr.hasNext()) {
                final List<Put> puts = new ArrayList<>(batchSize);
                for (int i = 0; i < batchSize && itr.hasNext(); i++) {
                    final Element element = itr.next();
                    final Pair<byte[]> row;
                    final byte[] cf;
                    final byte[] cq = Bytes.toBytes(element.getGroup());
                    final byte[] value = elementSerialisation.getValue(element);
                    if (element instanceof Edge) {
                        final Edge edge = ((Edge) element);
                        row = elementSerialisation.getRowKeys(edge);
                        cf = HBaseStoreConstants.EDGE_CF_BYTES;
                    } else {
                        final Entity entity = ((Entity) element);
                        row = new Pair<>(elementSerialisation.getRowKey(entity));
                        cf = HBaseStoreConstants.ENTITY_CF_BYTES;
                    }
                    final Put put = new Put(row.getFirst());
                    put.addColumn(cf, cq, value);
                    puts.add(put);

                    if (null != row.getSecond()) {
                        final Put put2 = new Put(row.getSecond());
                        put2.addColumn(cf, cq, value);
                        puts.add(put2);
                        i++;
                    }
                }
                table.put(puts);
            }
        } catch (IOException e) {
            throw new StoreException(e);
        }
    }

    @Override
    public boolean isValidationRequired() {
        return false;
    }
}
