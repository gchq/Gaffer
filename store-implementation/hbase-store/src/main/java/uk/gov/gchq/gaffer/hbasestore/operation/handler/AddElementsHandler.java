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

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import uk.gov.gchq.gaffer.commonutil.Pair;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.data.element.function.ElementAggregator;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.hbasestore.HBaseStore;
import uk.gov.gchq.gaffer.hbasestore.serialisation.ElementSerialisation;
import uk.gov.gchq.gaffer.hbasestore.utils.TableUtils;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.schema.SchemaElementDefinition;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
        try {
            final HTable table = TableUtils.getTable(store);
            final ElementSerialisation serialisation = new ElementSerialisation(store.getSchema());
            final Iterator<? extends Element> elements = addElementsOperation.getInput().iterator();
            while (elements.hasNext()) {
                final Collection<Element> elementBatch = createElementBatch(elements, store);
                final List<Put> puts = new ArrayList<>(elementBatch.size());
                for (final Element element : elementBatch) {
                    final Pair<Put> putPair = serialisation.getPuts(element);
                    puts.add(putPair.getFirst());
                    if (null != putPair.getSecond()) {
                        puts.add(putPair.getSecond());
                    }
                }

                if (!puts.isEmpty()) {
                    table.put(puts);
                    // Ensure the table has flushed otherwise similar elements in the next batch may be skipped.
                    if (!table.isAutoFlush()) {
                        table.flushCommits();
                    }
                }
            }
        } catch (final IOException | StoreException e) {
            throw new OperationException("Failed to add elements", e);
        }
    }

    private Collection<Element> createElementBatch(final Iterator<? extends Element> elements, final HBaseStore store) throws SerialisationException {
        final Map<String, ElementAggregator> aggregators = new HashMap<>(store.getSchema().getEdges().size() + store.getSchema().getEntities().size());
        final ElementSerialisation serialisation = new ElementSerialisation(store.getSchema());
        final int batchSize = store.getProperties().getWriteBufferSize();

        final Map<Integer, Element> keyToElement = new HashMap<>(batchSize);
        for (int i = 0; i < batchSize && elements.hasNext(); i++) {
            final Element element = elements.next();
            if (null == element) {
                i--;
                continue;
            }

            final int keyHashCode = new HashCodeBuilder(17, 37)
                    .append(serialisation.getRowKeys(element).getFirst())
                    .append(serialisation.getColumnQualifier(element))
                    .append(serialisation.getColumnVisibility(element))
                    .toHashCode();
            final Element existingElement = keyToElement.get(keyHashCode);
            if (null == existingElement) {
                keyToElement.put(keyHashCode, element);
            } else {
                ElementAggregator aggregator = aggregators.get(existingElement.getGroup());
                final SchemaElementDefinition elementDef = store.getSchema().getElement(existingElement.getGroup());
                if (null == aggregator) {
                    aggregator = elementDef.getAggregator();
                    aggregators.put(existingElement.getGroup(), aggregator);
                }
                Properties properties = element.getProperties();
                if (null != elementDef.getGroupBy() && !elementDef.getGroupBy().isEmpty()) {
                    properties = properties.clone();
                    properties.remove(elementDef.getGroupBy());
                }
                aggregator.apply(properties, existingElement.getProperties());
            }
        }
        return keyToElement.values();
    }
}
