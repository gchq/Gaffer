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

package gaffer.accumulostore.retriever;

import gaffer.accumulostore.AccumuloStore;
import gaffer.accumulostore.key.exception.AccumuloElementConversionException;
import gaffer.accumulostore.key.exception.RangeFactoryException;
import gaffer.accumulostore.utils.CloseableIterator;
import gaffer.data.element.Element;
import gaffer.operation.GetOperation;
import gaffer.store.StoreException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public abstract class AccumuloItemRetriever<OP_TYPE extends GetOperation<? extends SEED_TYPE, ?>, SEED_TYPE>
        extends AccumuloRetriever<OP_TYPE> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AccumuloItemRetriever.class);

    private final Iterable<? extends SEED_TYPE> ids;

    protected AccumuloItemRetriever(final AccumuloStore store, final OP_TYPE operation,
                                    final IteratorSetting... iteratorSettings) throws StoreException {
        super(store, operation, iteratorSettings);
        this.ids = operation.getSeeds();
    }

    @Override
    public Iterator<Element> iterator() {
        if (!ids.iterator().hasNext()) {
            return Collections.emptyIterator();
        }

        try {
            iterator = new ElementIterator();
        } catch (final RetrieverException e) {
            LOGGER.error(e.getMessage() + " returning empty iterator", e);
            return Collections.emptyIterator();
        }

        return iterator;
    }

    protected abstract void addToRanges(final SEED_TYPE seed, final Set<Range> ranges) throws RangeFactoryException;

    protected class ElementIterator implements CloseableIterator<Element> {
        private final Iterator<? extends SEED_TYPE> idsIterator;
        private int count;
        private BatchScanner scanner;
        private Iterator<Map.Entry<Key, Value>> scannerIterator;

        protected ElementIterator() throws RetrieverException {
            idsIterator = ids.iterator();
            count = 0;
            final Set<Range> ranges = new HashSet<>();
            while (idsIterator.hasNext() && count < store.getProperties().getMaxEntriesForBatchScanner()) {
                count++;
                try {
                    addToRanges(idsIterator.next(), ranges);
                } catch (final RangeFactoryException e) {
                    LOGGER.error("Failed to create a range from given seed pair", e);
                }
            }

            // Create BatchScanner, appropriately configured (i.e. ranges,
            // iterators, etc).
            try {
                scanner = getScanner(ranges);
            } catch (TableNotFoundException | StoreException e) {
                throw new RetrieverException(e);
            }
            scannerIterator = scanner.iterator();
        }

        @Override
        public boolean hasNext() {
            // If current scanner has next then return true.
            if (scannerIterator.hasNext()) {
                return true;
            }
            // If current scanner is spent then go back to the iterator
            // through the provided entities, and see if there are more.
            // If so create the next scanner, if there are no more entities
            // then return false.
            while (idsIterator.hasNext() && !scannerIterator.hasNext()) {
                count = 0;
                final Set<Range> ranges = new HashSet<>();
                while (idsIterator.hasNext() && count < store.getProperties().getMaxEntriesForBatchScanner()) {
                    count++;
                    try {
                        addToRanges(idsIterator.next(), ranges);
                    } catch (final RangeFactoryException e) {
                        LOGGER.error("Failed to create a range from given seed", e);
                    }
                }
                scanner.close();
                try {
                    scanner = getScanner(ranges);
                } catch (TableNotFoundException | StoreException e) {
                    LOGGER.error(e.getMessage() + " returning iterator doesn't have any more elements", e);
                    return false;
                }
                scannerIterator = scanner.iterator();
            }
            if (!scannerIterator.hasNext()) {
                scanner.close();
            }
            return scannerIterator.hasNext();
        }

        @Override
        public Element next() {
            final Map.Entry<Key, Value> entry = scannerIterator.next();
            try {
                final Element elm = elementConverter.getFullElement(entry.getKey(), entry.getValue(),
                        operation.getOptions());
                doTransformation(elm);
                return elm;
            } catch (final AccumuloElementConversionException e) {
                LOGGER.error("Failed to re-create an element from a key value entry set returning next element as null",
                        e);
                return null;
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Unable to remove elements from this iterator");
        }

        @Override
        public void close() {
            if (scanner != null) {
                scanner.close();
            }
        }
    }
}
