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

package uk.gov.gchq.gaffer.hbasestore.operation.handler;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.hbasestore.HBaseStore;
import uk.gov.gchq.gaffer.hbasestore.serialisation.ElementSerialisation;
import uk.gov.gchq.gaffer.hbasestore.utils.Pair;
import uk.gov.gchq.gaffer.hbasestore.utils.TableUtils;
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

public class AddElementsHandler implements OperationHandler<AddElements, Void> {
    @Override
    public Void doOperation(final AddElements operation,
                            final Context context, final Store store)
            throws OperationException {
        addElements(operation, (HBaseStore) store);
        return null;
    }

    private void addElements(final AddElements addElementsOperation, final HBaseStore store)
            throws OperationException {
        final ElementSerialisation serialisation = new ElementSerialisation(store.getSchema());
        int batchSize = store.getProperties().getWriteBufferSize();
        try {
            final Table table = TableUtils.getTable(store);
            final Iterator<Element> itr = addElementsOperation.getElements().iterator();
            while (itr.hasNext()) {
                final List<Put> puts = new ArrayList<>(batchSize);
                for (int i = 0; i < batchSize && itr.hasNext(); i++) {
                    final Element element = itr.next();
                    if (null == element) {
                        i--;
                        continue;
                    }

                    final Pair<Put> putPair = serialisation.getPuts(element);
                    puts.add(putPair.getFirst());
                    if (null != putPair.getSecond()) {
                        puts.add(putPair.getSecond());
                        i++;
                    }
                }
                table.put(puts);
            }
        } catch (IOException | StoreException e) {
            throw new OperationException("Failed to add elements", e);
        }
    }
}
