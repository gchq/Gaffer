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

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import uk.gov.gchq.gaffer.commonutil.Pair;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.hbasestore.HBaseStore;
import uk.gov.gchq.gaffer.hbasestore.serialisation.ElementSerialisation;
import uk.gov.gchq.gaffer.hbasestore.utils.TableUtils;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
        final ElementSerialisation serialisation = new ElementSerialisation(store.getSchema());
        int batchSize = store.getProperties().getWriteBufferSize();
        try {
            final HTable table = TableUtils.getTable(store);
            final Iterator<? extends Element> itr = addElementsOperation.getInput().iterator();
            while (itr.hasNext()) {
                final Map<Integer, Set<Integer>> elementMap = new HashMap<>(batchSize);
                final List<Put> puts = new ArrayList<>(batchSize);
                for (int i = 0; i < batchSize && itr.hasNext(); i++) {
                    final Element element = itr.next();
                    if (null == element) {
                        i--;
                        continue;
                    }

                    final Pair<byte[]> row = serialisation.getRowKeys(element);
                    final byte[] rowId = row.getFirst();
                    final byte[] cq = serialisation.getColumnQualifier(element);

                    // HBase will skip 'puts' if there are multiple 'puts' with the same rowId and column qualifier.
                    // The following is a horrible work around for the time being,
                    // it may cause problems when adding large numbers of similar
                    // elements - in that case you would be better off using
                    // AddElementsFromHdfs.
                    // Check if the rowId and cq combination has already been added
                    // if it has then flush the current 'puts' and start a new batch.
                    final boolean sameKey;
                    final int rowIdHash = Arrays.hashCode(rowId);
                    final int cqHash = Arrays.hashCode(cq);
                    Set<Integer> rowSet = elementMap.get(rowIdHash);
                    if (null == rowSet) {
                        rowSet = new HashSet<>();
                        elementMap.put(rowIdHash, rowSet);
                    }
                    sameKey = !rowSet.add(cqHash);
                    if (sameKey) {
                        table.put(puts);
                        // Ensure the table has flushed
                        if (!table.isAutoFlush()) {
                            table.flushCommits();
                        }
                        puts.clear();
                        i = 0;
                    }

                    final Pair<Put> putPair = serialisation.getPuts(element, row, cq);
                    puts.add(putPair.getFirst());
                    if (null != putPair.getSecond()) {
                        puts.add(putPair.getSecond());
                        i++;
                    }
                }

                if (!puts.isEmpty()) {
                    table.put(puts);
                    // Ensure the table has flushed
                    if (!table.isAutoFlush()) {
                        table.flushCommits();
                    }
                }
            }
        } catch (final IOException | StoreException e) {
            throw new OperationException("Failed to add elements", e);
        }
    }
}
