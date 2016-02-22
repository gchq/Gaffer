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

package gaffer.accumulostore.retriever.impl;

import gaffer.accumulostore.AccumuloStore;
import gaffer.accumulostore.operation.AbstractAccumuloTwoSetSeededOperation;
import gaffer.accumulostore.retriever.AccumuloSetRetriever;
import gaffer.accumulostore.retriever.RetrieverException;
import gaffer.accumulostore.utils.BloomFilterUtils;
import gaffer.operation.data.EntitySeed;
import gaffer.store.StoreException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.hadoop.util.bloom.BloomFilter;
import java.util.Set;

/**
 * Given two sets of {@link gaffer.operation.data.EntitySeed}s, called A and B,
 * this retrieves all {@link gaffer.data.element.Edge}s where one end is in set
 * A and the other is in set B and also returns
 * {@link gaffer.data.element.Entity}s for
 * {@link gaffer.operation.data.EntitySeed}s in set A.
 * <p>
 * This is done by querying for set A, and uses a
 * {@link org.apache.hadoop.util.bloom.BloomFilter}s in a filtering iterator to
 * identify edges that are likely to be between a member of set A and a member
 * of set B. Only these edges are returned to the client, and this reduces the
 * amount of data sent to the client.
 * <p>
 * This operates in two modes. In the first mode the seeds from both sets A and
 * B are loaded into memory (client-side). The seeds from set B are loaded into
 * a {@link org.apache.hadoop.util.bloom.BloomFilter}. This is passed to the
 * iterators to filter out all edges for which the non-query end is definitely
 * not in set B. A secondary check is done within this class to check that the
 * edge is definitely between elements of the set (this defeats any false
 * positives, i.e. edges that passed the
 * {@link org.apache.hadoop.util.bloom.BloomFilter} check in the iterators).
 * This secondary check uses the in memory set of seeds (and hence there are
 * guaranteed to be no false positives returned to the user).
 * <p>
 * In the second mode, where there are too many seeds to be loaded into memory,
 * the seeds in set A are queried for in batches. The seeds in set B are loaded
 * into two {@link org.apache.hadoop.util.bloom.BloomFilter}s. The first of
 * these is relatively small and is passed to the filtering iterator to filter
 * out edges that are definitely not to set B. The second, larger,
 * {@link org.apache.hadoop.util.bloom.BloomFilter} is used client-side to
 * further reduce the chances of false positives making it to the user.
 */
public class AccumuloIDBetweenSetsRetriever extends AccumuloSetRetriever {
    private Iterable<EntitySeed> seedSetA;
    private Iterable<EntitySeed> seedSetB;

    public AccumuloIDBetweenSetsRetriever(final AccumuloStore store,
                                          final AbstractAccumuloTwoSetSeededOperation<EntitySeed, ?> operation,
                                          final IteratorSetting... iteratorSettings) throws StoreException {
        this(store, operation, false, iteratorSettings);
    }

    public AccumuloIDBetweenSetsRetriever(final AccumuloStore store,
                                          final AbstractAccumuloTwoSetSeededOperation<EntitySeed, ?> operation, final boolean readEntriesIntoMemory,
                                          final IteratorSetting... iteratorSettings) throws StoreException {
        super(store, operation, readEntriesIntoMemory, iteratorSettings);
        setSeeds(operation.getSeeds(), operation.getSeedsB());
    }

    private void setSeeds(final Iterable<EntitySeed> setA, final Iterable<EntitySeed> setB) {
        this.seedSetA = setA;
        this.seedSetB = setB;
    }

    @Override
    protected boolean hasSeeds() {
        return seedSetA.iterator().hasNext() && seedSetB.iterator().hasNext();
    }

    @Override
    protected ElementIteratorReadIntoMemory createElementIteratorReadIntoMemory() throws RetrieverException {
        return new ElementIteratorReadIntoMemory();
    }

    @Override
    protected ElementIteratorFromBatches createElementIteratorFromBatches() throws RetrieverException {
        return new ElementIteratorFromBatches();
    }

    private class ElementIteratorReadIntoMemory extends AbstractElementIteratorReadIntoMemory {
        private final Set<Object> verticesA;
        private final Set<Object> verticesB;

        ElementIteratorReadIntoMemory() throws RetrieverException {
            verticesA = extractVertices(seedSetA);
            verticesB = extractVertices(seedSetB);

            // Create Bloom filter, read through set of entities B and add them
            // to Bloom filter
            final BloomFilter filter = BloomFilterUtils.getBloomFilter(store.getProperties().getFalsePositiveRate(),
                    verticesB.size(), store.getProperties().getMaxBloomFilterToPassToAnIterator());
            addToBloomFilter(verticesB, filter);
            addToBloomFilter(verticesA, filter);
            initialise(filter);
        }

        /**
         * @param source      the element source identifier
         * @param destination the element destination identifier
         * @return True if the source and destination contained in the provided seed sets
         */
        @Override
        protected boolean checkIfBothEndsInSet(final Object source, final Object destination) {
            return verticesA.contains(source) && verticesB.contains(destination)
                    || verticesB.contains(source) && verticesA.contains(destination);
        }
    }

    private class ElementIteratorFromBatches extends AbstractElementIteratorFromBatches {
        ElementIteratorFromBatches() throws RetrieverException {
            addToBloomFilter(seedSetB, filter, clientSideFilter);
            addToBloomFilter(seedSetA, filter, clientSideFilter);
            idsAIterator = seedSetA.iterator();
            updateScanner();
        }

        @Override
        protected void updateBloomFilterIfRequired(final EntitySeed seed) throws RetrieverException {
            // no action required.
        }
    }
}
