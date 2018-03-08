/*
 * Copyright 2016-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.hbasestore.operation.handler;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.hbasestore.HBaseStore;
import uk.gov.gchq.gaffer.hbasestore.serialisation.ElementSerialisation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * HBase will skip 'puts' if there are multiple 'puts' with the same rowId and column qualifier.
 * To work around this issue, we need to aggregate elements within each batch before adding them to HBase.
 * Due to this, optimising the batch size could have a big impact on performance.
 * Configure the batch size using store property: hbase.writeBufferSize
 */
public class AddElementsHandler implements OperationHandler<AddElements> {
    @Override
    public Void doOperation(final AddElements operation,
                            final Context context, final Store store)
            throws OperationException {
        addElements(operation, (HBaseStore) store);
        return null;
    }

    private void addElements(final AddElements addElementsOperation, final HBaseStore store)
            throws OperationException {
        if (null == addElementsOperation.getInput()) {
            return;
        }

        try {
            final Table table = store.getTable();
            final Iterator<? extends Element> elements = addElementsOperation.getInput().iterator();
            final ElementSerialisation serialisation = new ElementSerialisation(store.getSchema());
            final int batchSize = store.getProperties().getWriteBufferSize();
            List<Put> puts = new ArrayList<>(batchSize);
            while (elements.hasNext()) {
                for (int i = 0; i < batchSize && elements.hasNext(); i++) {
                    final Element element = elements.next();
                    if (null == element) {
                        i--;
                        continue;
                    }
                    final Pair<Put, Put> putPair = serialisation.getPuts(element);
                    puts.add(putPair.getFirst());
                    if (null != putPair.getSecond()) {
                        i++;
                        if (i >= batchSize) {
                            executePuts(table, puts);
                            puts = new ArrayList<>(batchSize);
                            i = 0;
                        }
                        puts.add(putPair.getSecond());
                    }
                }
                executePuts(table, puts);
                puts = new ArrayList<>(batchSize);
            }
        } catch (final IOException | StoreException e) {
            throw new OperationException("Failed to add elements", e);
        }
    }

    private void executePuts(final Table table, final List<Put> puts) throws IOException {
        if (!puts.isEmpty()) {
            table.put(puts);
            if (table instanceof HTable) {
                ((HTable) table).flushCommits();
            }
        }
    }
}
