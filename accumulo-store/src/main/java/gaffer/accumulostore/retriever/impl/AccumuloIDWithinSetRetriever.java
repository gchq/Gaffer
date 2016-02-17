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
import gaffer.accumulostore.retriever.AccumuloSetRetriever;
import gaffer.accumulostore.retriever.RetrieverException;
import gaffer.accumulostore.utils.BloomFilterUtils;
import gaffer.operation.GetOperation;
import gaffer.operation.data.EntitySeed;
import gaffer.store.StoreException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.hadoop.util.bloom.BloomFilter;
import java.util.Set;

/**
 * Retrieves {@link gaffer.data.element.Edge}s where both ends are in a given
 * set of {@link gaffer.operation.data.EntitySeed}'s and
 * {@link gaffer.data.element.Entity}s where the vertex is in the set.
 * <p>
 * {@link org.apache.hadoop.util.bloom.BloomFilter}s are used to identify on the
 * server edges that are likely to be between members of the set and to send
 * only these to the client. This reduces the amount of data sent to the client.
 * <p>
 * This operates in two modes. In the first mode the seeds are loaded into
 * memory (client-side). They are also loaded into a
 * {@link org.apache.hadoop.util.bloom.BloomFilter}. This is passed to the
 * iterators to filter out all edges that are definitely not between elements of
 * the set. A secondary check is done within this class to check that the edge
 * is definitely between elements of the set (this defeats any false positives,
 * i.e. edges that passed the {@link org.apache.hadoop.util.bloom.BloomFilter}
 * check in the iterators). This secondary check uses the in memory set of seeds
 * (and hence there are guaranteed to be no false positives returned to the
 * user).
 * <p>
 * In the second mode, where there are too many seeds to be loaded into memory,
 * the seeds are queried one batch at a time. When the first batch is queried
 * for, a {@link org.apache.hadoop.util.bloom.BloomFilter} of the first batch is
 * created and passed to the iterators. This filters out all edges that are
 * definitely not between elements of the first batch. When the second batch is
 * queried for, the same {@link org.apache.hadoop.util.bloom.BloomFilter} has
 * the second batch added to it. This is passed to the iterators, which filters
 * out all edges that are definitely not between elements of the second batch
 * and the first or second batch. This process repeats until all seeds have been
 * queried for. This is best thought of as a square split into a grid (with the
 * same number of squares in both dimensions). As there are too many seeds to
 * load into memory, we use a client-side
 * {@link org.apache.hadoop.util.bloom.BloomFilter} to further reduce the
 * chances of false positives making it to the user.
 */
public class AccumuloIDWithinSetRetriever extends AccumuloSetRetriever {
    private Iterable<EntitySeed> seeds;

    public AccumuloIDWithinSetRetriever(final AccumuloStore store, final GetOperation<EntitySeed, ?> operation,
                                        final IteratorSetting... iteratorSettings) throws StoreException {
        this(store, operation, false, iteratorSettings);
    }

    public AccumuloIDWithinSetRetriever(final AccumuloStore store, final GetOperation<EntitySeed, ?> operation,
                                        final boolean readEntriesIntoMemory, final IteratorSetting... iteratorSettings) throws StoreException {
        super(store, operation, readEntriesIntoMemory, iteratorSettings);
        setSeeds(operation.getSeeds());
    }

    private void setSeeds(final Iterable<EntitySeed> seeds) {
        this.seeds = seeds;
    }

    @Override
    protected boolean hasSeeds() {
        return seeds.iterator().hasNext();
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
        private final Set<Object> vertices;

        ElementIteratorReadIntoMemory() throws RetrieverException {
            vertices = extractVertices(seeds);

            // Create Bloom filter, read through set of entities and add them to
            // Bloom filter
            final BloomFilter filter = BloomFilterUtils.getBloomFilter(store.getProperties().getFalsePositiveRate(),
                    vertices.size(), store.getProperties().getMaxBloomFilterToPassToAnIterator());
            addToBloomFilter(vertices, filter);

            initialise(filter);
        }

        /**
         * @param source      the element source identifier
         * @param destination the element destination identifier
         * @return True if the source and destination contained in the provided seed sets
         */
        @Override
        protected boolean checkIfBothEndsInSet(final Object source, final Object destination) {
            return vertices.contains(source) && vertices.contains(destination);
        }
    }

    private class ElementIteratorFromBatches extends AbstractElementIteratorFromBatches {
        ElementIteratorFromBatches() throws RetrieverException {
            idsAIterator = seeds.iterator();
            updateScanner();
        }

        @Override
        protected void updateBloomFilterIfRequired(final EntitySeed seed) throws RetrieverException {
            // NB: Do not reset either of the Bloom filters here - when we query
            // for the first batch of seeds the Bloom filters contain that first set
            // (and so we find edges within that first batch);
            // we next query for the second batch of seeds and the Bloom filters
            // contain both the first batch and the second batch
            // (and so we find edges from the second batch to either the first or second batches).
            addToBloomFilter(seed, filter, clientSideFilter);
        }
    }
}
