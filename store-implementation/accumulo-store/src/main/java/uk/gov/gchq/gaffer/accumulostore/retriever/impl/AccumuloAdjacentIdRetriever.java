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

package uk.gov.gchq.gaffer.accumulostore.retriever.impl;

import com.google.common.collect.Iterators;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.key.exception.AccumuloElementConversionException;
import uk.gov.gchq.gaffer.accumulostore.key.exception.IteratorSettingException;
import uk.gov.gchq.gaffer.accumulostore.key.exception.RangeFactoryException;
import uk.gov.gchq.gaffer.accumulostore.retriever.AccumuloRetriever;
import uk.gov.gchq.gaffer.accumulostore.retriever.RetrieverException;
import uk.gov.gchq.gaffer.commonutil.CloseableUtil;
import uk.gov.gchq.gaffer.commonutil.StringUtil;
import uk.gov.gchq.gaffer.commonutil.iterable.ChainedIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterator;
import uk.gov.gchq.gaffer.commonutil.iterable.EmptyCloseableIterator;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.id.EdgeId;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.user.User;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

public class AccumuloAdjacentIdRetriever extends AccumuloRetriever<GetAdjacentIds, EntityId> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AccumuloAdjacentIdRetriever.class);

    private final Iterable<? extends ElementId> ids;
    private final Set<String> transformGroups;

    public AccumuloAdjacentIdRetriever(final AccumuloStore store, final GetAdjacentIds operation,
                                       final User user)
            throws IteratorSettingException, StoreException {
        super(store, operation, user,
                store.getKeyPackage().getIteratorFactory().getEdgeEntityDirectionFilterIteratorSetting(operation),
                store.getKeyPackage().getIteratorFactory().getElementPreAggregationFilterIteratorSetting(operation.getView(), store),
                store.getKeyPackage().getIteratorFactory().getQueryTimeAggregatorIteratorSetting(operation.getView(), store),
                store.getKeyPackage().getIteratorFactory().getElementPostAggregationFilterIteratorSetting(operation.getView(), store));
        this.ids = operation.getInput();
        transformGroups = getGroupsWithTransforms(operation.getView());
    }

    /**
     * Only 1 iterator can be open at a time.
     *
     * @return a closeable iterator of items.
     */
    @Override
    public CloseableIterator<EntityId> iterator() {
        CloseableUtil.close(iterator);

        final Iterator<? extends ElementId> idIterator = null != ids ? ids.iterator() : Iterators.emptyIterator();
        if (!idIterator.hasNext()) {
            return new EmptyCloseableIterator<>();
        }

        try {
            iterator = new EntityIdIterator(idIterator);
        } catch (final RetrieverException e) {
            LOGGER.error("{} returning empty iterator", e.getMessage(), e);
            return new EmptyCloseableIterator<>();
        }

        return iterator;
    }

    private final class EntityIdIterator implements CloseableIterator<EntityId> {
        private final Iterator<? extends ElementId> idsIterator;
        private int count;
        private BatchScanner scanner;
        private Iterator<Map.Entry<Key, Value>> scannerIterator;
        private EntityId nextId;

        private EntityIdIterator(final Iterator<? extends ElementId> idIterator) throws RetrieverException {
            idsIterator = idIterator;
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

            try {
                scanner = getScanner(ranges);
            } catch (final Exception e) {
                CloseableUtil.close(idsIterator);
                CloseableUtil.close(ids);
                throw new RetrieverException(e);
            }
            scannerIterator = scanner.iterator();
        }

        @Override
        public boolean hasNext() {
            // If current scanner has next then return true.
            if (null != nextId) {
                return true;
            }
            while (scannerIterator.hasNext()) {
                final Map.Entry<Key, Value> entry = scannerIterator.next();

                final String group = StringUtil.toString(entry.getKey().getColumnFamilyData().getBackingArray());
                ElementId elementId = null;
                if (transformGroups.contains(group)) {
                    final Element element;
                    try {
                        element = elementConverter.getFullElement(
                                entry.getKey(),
                                entry.getValue(),
                                true);
                    } catch (final AccumuloElementConversionException e) {
                        LOGGER.error("Failed to re-create an element from a key value entry set returning next EntityId as null",
                                e);
                        continue;
                    }
                    if (null != element) {
                        doTransformation(element);
                        if (doPostFilter(element)) {
                            elementId = element;
                        }
                    }
                } else {
                    try {
                        elementId = elementConverter.getElementId(entry.getKey(), true);
                    } catch (final AccumuloElementConversionException e) {
                        LOGGER.error("Failed to create element id returning next EntityId as null", e);
                        continue;
                    }
                }

                if (null != elementId) {
                    if (elementId instanceof EdgeId) {
                        if (EdgeId.MatchedVertex.DESTINATION == ((EdgeId) elementId).getMatchedVertex()) {
                            nextId = new EntitySeed(((EdgeId) elementId).getSource());
                        } else {
                            nextId = new EntitySeed(((EdgeId) elementId).getDestination());
                        }
                        return true;
                    } else {
                        LOGGER.error("Unexpected EntityId returned, returning next result as null");
                    }
                }
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
                } catch (final TableNotFoundException | StoreException e) {
                    LOGGER.error("{} returning iterator doesn't have any more elements", e.getMessage(), e);
                    return false;
                }
                scannerIterator = scanner.iterator();
            }
            if (!scannerIterator.hasNext()) {
                scanner.close();
                return false;
            } else {
                return hasNext();
            }
        }

        @Override
        public EntityId next() {
            if (null == nextId) {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
            }
            EntityId nextReturn = nextId;
            nextId = null;
            return nextReturn;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Unable to remove elements from this iterator");
        }

        @Override
        public void close() {
            if (null != scanner) {
                scanner.close();
            }
        }
    }

    private void addToRanges(final ElementId seed, final Set<Range> ranges) throws RangeFactoryException {
        ranges.addAll(rangeFactory.getRange(seed, operation));
    }

    private Set<String> getGroupsWithTransforms(final View view) {
        final Set<String> groups = new HashSet<>();
        for (final Map.Entry<String, ViewElementDefinition> entry : new ChainedIterable<Map.Entry<String, ViewElementDefinition>>(view.getEntities().entrySet(), view.getEdges().entrySet())) {
            if (null != entry.getValue()) {
                if (entry.getValue().hasPostTransformFilters()) {
                    groups.add(entry.getKey());
                }
            }
        }
        return groups;
    }
}
