/*
 * Copyright 2016-2023 Crown Copyright
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

package uk.gov.gchq.gaffer.accumulostore.retriever;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.key.exception.AccumuloElementConversionException;
import uk.gov.gchq.gaffer.accumulostore.key.exception.IteratorSettingException;
import uk.gov.gchq.gaffer.accumulostore.key.exception.RangeFactoryException;
import uk.gov.gchq.gaffer.accumulostore.retriever.impl.AccumuloSingleIDRetriever;
import uk.gov.gchq.gaffer.accumulostore.utils.BloomFilterUtils;
import uk.gov.gchq.gaffer.commonutil.CloseableUtil;
import uk.gov.gchq.gaffer.commonutil.iterable.EmptyIterator;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewUtil;
import uk.gov.gchq.gaffer.operation.graph.GraphFilters;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.user.User;

import java.io.Closeable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

public abstract class AccumuloSetRetriever<OP extends InputOutput<Iterable<? extends EntityId>, Iterable<? extends Element>> & GraphFilters>
        extends AccumuloRetriever<OP, Element> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AccumuloSetRetriever.class);
    private boolean readEntriesIntoMemory;

    public AccumuloSetRetriever(final AccumuloStore store, final OP operation, final User user)
            throws StoreException {
        this(store, operation, user, false);
    }

    public AccumuloSetRetriever(final AccumuloStore store, final OP operation, final User user,
                                final boolean readEntriesIntoMemory)
            throws StoreException {
        super(store, operation, user);
        this.readEntriesIntoMemory = readEntriesIntoMemory;
    }

    public AccumuloSetRetriever(final AccumuloStore store, final OP operation, final User user,
                                final IteratorSetting... iteratorSettings)
            throws StoreException {
        this(store, operation, user, false, iteratorSettings);
    }

    public AccumuloSetRetriever(final AccumuloStore store, final OP operation, final User user,
                                final boolean readEntriesIntoMemory, final IteratorSetting... iteratorSettings)
            throws StoreException {
        super(store, operation, user, iteratorSettings);
        this.readEntriesIntoMemory = readEntriesIntoMemory;
    }

    public void setReadEntriesIntoMemory(final boolean readEntriesIntoMemory) {
        this.readEntriesIntoMemory = readEntriesIntoMemory;
    }

    /**
     * Only 1 iterator can be open at a time.
     *
     * @return a closeable iterator of items.
     */
    @Override
    public Iterator<Element> iterator() {
        CloseableUtil.close(iterator);

        if (!hasSeeds()) {
            return new EmptyIterator<>();
        }
        if (readEntriesIntoMemory) {
            try {
                iterator = createElementIteratorReadIntoMemory();
            } catch (final RetrieverException e) {
                LOGGER.error("{} returning empty iterator", e.getMessage(), e);
                return new EmptyIterator<>();
            }
        } else {
            try {
                iterator = createElementIteratorFromBatches();
            } catch (final RetrieverException e) {
                LOGGER.error("{} returning empty iterator", e.getMessage(), e);
                return new EmptyIterator<>();
            }
        }
        return iterator;
    }

    protected abstract boolean hasSeeds();

    protected abstract AbstractElementIteratorReadIntoMemory createElementIteratorReadIntoMemory()
            throws RetrieverException;

    protected abstract AbstractElementIteratorFromBatches createElementIteratorFromBatches() throws RetrieverException;

    protected Set<Object> extractVertices(final Iterator<? extends EntityId> seeds) {
        final Set<Object> vertices = new HashSet<>();
        while (seeds.hasNext()) {
            vertices.add(seeds.next().getVertex());
        }

        return vertices;
    }

    protected void addToBloomFilter(final Iterable<? extends Object> vertices, final BloomFilter filter)
            throws RetrieverException {
        addToBloomFilter(vertices.iterator(), filter);
    }

    protected void addToBloomFilter(final Iterator<? extends Object> vertices, final BloomFilter filter)
            throws RetrieverException {
        try {
            while (vertices.hasNext()) {
                addToBloomFilter(vertices.next(), filter);
            }
        } finally {
            CloseableUtil.close(vertices);
        }
    }

    protected void addToBloomFilter(final Iterator<? extends EntityId> seeds, final BloomFilter filter1,
                                    final BloomFilter filter2)
            throws RetrieverException {
        try {
            while (seeds.hasNext()) {
                addToBloomFilter(seeds.next(), filter1, filter2);
            }
        } finally {
            CloseableUtil.close(seeds);
        }
    }

    protected void addToBloomFilter(final EntityId seed, final BloomFilter filter1, final BloomFilter filter2)
            throws RetrieverException {
        addToBloomFilter(seed.getVertex(), filter1);
        addToBloomFilter(seed.getVertex(), filter2);
    }

    private void addToBloomFilter(final Object vertex, final BloomFilter filter) throws RetrieverException {
        try {
            filter.add(new org.apache.hadoop.util.bloom.Key(elementConverter.serialiseVertex(vertex)));
        } catch (final AccumuloElementConversionException e) {
            throw new RetrieverException("Failed to add identifier to the bloom key", e);
        }
    }

    protected abstract class AbstractElementIteratorReadIntoMemory implements Iterator<Element>, Closeable {
        private AccumuloItemRetriever<?, ?> parentRetriever;
        private Iterator<Element> iterator;
        private Element nextElm;

        @SuppressWarnings({"unchecked", "rawtypes"})
        protected void initialise(final BloomFilter filter) throws RetrieverException {
            IteratorSetting bloomFilter = null;
            final IteratorSetting[] iteratorSettingsCopy = Arrays.copyOf(iteratorSettings, iteratorSettings.length + 1);
            try {
                bloomFilter = iteratorSettingFactory.getBloomFilterIteratorSetting(filter);
            } catch (final IteratorSettingException e) {
                LOGGER.error("Failed to apply the bloom filter to the retriever, "
                        + "creating the gaffer.accumulostore.retriever without bloom filter", e);
            }
            iteratorSettingsCopy[iteratorSettings.length] = bloomFilter;
            try {
                parentRetriever = new AccumuloSingleIDRetriever(store, operation, user, iteratorSettingsCopy);
            } catch (final Exception e) {
                CloseableUtil.close(operation);
                throw new RetrieverException(e.getMessage(), e);
            }
            iterator = parentRetriever.iterator();
        }

        @Override
        public boolean hasNext() {
            if (nonNull(nextElm)) {
                return true;
            }
            if (isNull(iterator)) {
                throw new IllegalStateException(
                        "This iterator has not been initialised. Call initialise before using it.");
            }
            while (iterator.hasNext()) {
                nextElm = iterator.next();
                if (checkIfBothEndsInSet(nextElm) && doPostFilter(nextElm)) {
                    ViewUtil.removeProperties(operation.getView(), nextElm);
                    return true;
                }
            }
            return false;
        }

        @Override
        public Element next() {
            if (nextElm == null && !hasNext()) {
                close();
                throw new NoSuchElementException();
            }
            final Element nextReturn = nextElm;
            nextElm = null;
            return nextReturn;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Can't remove elements from a graph element iterator");
        }

        @Override
        public void close() {
            CloseableUtil.close(parentRetriever);
        }

        protected abstract boolean checkIfBothEndsInSet(final Object source, final Object destination);

        /**
         * Returns {@code true} if either an
         * {@link uk.gov.gchq.gaffer.data.element.Entity} or if an
         * {@link uk.gov.gchq.gaffer.data.element.Edge} then need both ends to be in the
         * set.
         *
         * @param elm the element to check
         * @return True if the provided element is an edge and Both ends are
         *         contained in the provided seed sets or if the element is an
         *         entity
         */
        private boolean checkIfBothEndsInSet(final Element elm) {
            if (Entity.class.isInstance(elm)) {
                return true;
            }
            final Edge edge = (Edge) elm;
            final Object source = edge.getSource();
            final Object destination = edge.getDestination();
            return checkIfBothEndsInSet(source, destination);
        }
    }

    protected abstract class AbstractElementIteratorFromBatches implements Iterator<Element>, Closeable {
        protected Iterator<? extends EntityId> idsAIterator;
        // The Bloom filter that is maintained client-side
        // as a secondary defeat of false positives.
        protected BloomFilter clientSideFilter;
        protected Set<Object> currentSeeds;
        protected BatchScanner scanner;
        protected BloomFilter filter;
        private Iterator<Entry<Key, Value>> scannerIterator;
        private Element nextElm;
        private int count;

        public AbstractElementIteratorFromBatches() {
            // Set up client side filter
            clientSideFilter = BloomFilterUtils.getBloomFilter(store.getProperties().getClientSideBloomFilterSize());
            // Create Bloom filter to be passed to iterators.
            filter = BloomFilterUtils.getBloomFilter(store.getProperties().getFalsePositiveRate(),
                    store.getProperties().getMaxEntriesForBatchScanner(),
                    store.getProperties().getMaxBloomFilterToPassToAnIterator());
            currentSeeds = new HashSet<>();
        }

        @Override
        public boolean hasNext() {
            if (nonNull(nextElm)) {
                return true;
            }
            try {
                while (_hasNext()) {
                    final Entry<Key, Value> entry = scannerIterator.next();
                    try {
                        nextElm = elementConverter.getFullElement(entry.getKey(), entry.getValue(), true);
                    } catch (final AccumuloElementConversionException e) {
                        LOGGER.error("Failed to create next element from key and value entry set", e);
                        continue;
                    }
                    if (secondaryCheck(nextElm)) {
                        doTransformation(nextElm);
                        if (doPostFilter(nextElm)) {
                            ViewUtil.removeProperties(operation.getView(), nextElm);
                            return true;
                        }
                    }
                }
            } catch (final RetrieverException e) {
                LOGGER.debug("Failed to retrieve elements into iterator : {} returning iterator has no more elements",
                        e.getMessage(), e);
                return false;
            }

            return false;
        }

        @Override
        public Element next() {
            if (nextElm == null && !hasNext()) {
                throw new NoSuchElementException();
            }
            final Element nextReturn = nextElm;
            nextElm = null;
            return nextReturn;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException(String.format("Can't remove elements from a %s", this.getClass().getCanonicalName()));
        }

        @Override
        public void close() {
            CloseableUtil.close(scanner);
        }

        protected abstract void updateBloomFilterIfRequired(final EntityId seed) throws RetrieverException;

        protected void updateScanner() throws RetrieverException {
            // Read through the first N entities (where N =
            // maxEntriesForBatchScanner), create the associated ranges
            // and add them to a set.
            count = 0;
            final Set<Range> ranges = new HashSet<>();
            while (idsAIterator.hasNext() && count < store.getProperties().getMaxEntriesForBatchScanner()) {
                final EntityId seed = idsAIterator.next();
                currentSeeds.add(seed.getVertex());
                count++;
                try {
                    ranges.addAll(rangeFactory.getRange(seed, operation));
                } catch (final RangeFactoryException e) {
                    LOGGER.error("Failed to create a range from given seed", e);
                }
                updateBloomFilterIfRequired(seed);
            }

            try {
                scanner = getScanner(ranges);
            } catch (final TableNotFoundException | StoreException e) {
                CloseableUtil.close(idsAIterator);
                CloseableUtil.close(operation);
                throw new RetrieverException(e);
            }
            try {
                scanner.addScanIterator(iteratorSettingFactory.getBloomFilterIteratorSetting(filter));
            } catch (final IteratorSettingException e) {
                LOGGER.error("Failed to apply the bloom filter iterator setting continuing without bloom filter", e);
            }
            scannerIterator = scanner.iterator();
        }

        /**
         * Check whether this is valid, i.e. one end is in the current set of
         * seeds that are being queried for and the other matches the Bloom
         * filter (i.e. the client side Bloom filter that is being used as a
         * secondary defeat of false positives).
         *
         * @param elm the element to check
         * @return true if the element matches the seeds, otherwise false
         */
        protected abstract boolean secondaryCheck(final Element elm);

        private boolean _hasNext() throws RetrieverException {
            // If current scanner has next then return true.
            if (scannerIterator.hasNext()) {
                return true;
            }
            // If current scanner is spent then go back to the iterator
            // through the provided entities, and see if there are more.
            // If so create the next scanner, if there are no more entities
            // then return false.
            while (idsAIterator.hasNext() && !scannerIterator.hasNext()) {
                updateScanner();
            }
            if (!scannerIterator.hasNext()) {
                CloseableUtil.close(scanner);
            }
            return scannerIterator.hasNext();
        }
    }
}
