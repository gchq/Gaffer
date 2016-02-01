/*
 * Copyright 2016 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gaffer.accumulostore.retriever.impl;

import gaffer.accumulostore.AccumuloStore;
import gaffer.accumulostore.retriever.RetrieverException;
import gaffer.accumulostore.utils.BloomFilterUtils;
import gaffer.accumulostore.retriever.AccumuloSetRetriever;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.GetOperation;
import gaffer.store.StoreException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.hadoop.util.bloom.BloomFilter;

import java.util.Set;

public class AccumuloIDWithinSetRetriever extends AccumuloSetRetriever {
    private Iterable<EntitySeed> seeds;

    public AccumuloIDWithinSetRetriever(final AccumuloStore store, final GetOperation<EntitySeed, ?> operation,
                                        final IteratorSetting... iteratorSettings) throws StoreException {
        this(store, operation, false, iteratorSettings);
    }

    public AccumuloIDWithinSetRetriever(final AccumuloStore store, final GetOperation<EntitySeed, ?> operation, final boolean readEntriesIntoMemory,
                                        final IteratorSetting... iteratorSettings) throws StoreException {
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

            // Create Bloom filter, read through set of entities and add them to Bloom filter
            final BloomFilter filter = BloomFilterUtils.getBloomFilter(store.getProperties().getFalsePositiveRate(), vertices.size(),
                    store.getProperties().getMaxBloomFilterToPassToAnIterator());
            addToBloomFilter(vertices, filter);

            initialise(filter);
        }

        /**
         * @param source
         * @param destination
         * @return True if the source and destination contained in the provided seed sets
         */
        protected boolean checkIfBothEndsInSet(final Object source, final Object destination) {
            return vertices.contains(source) && vertices.contains(destination);
        }
    }

    private class ElementIteratorFromBatches extends AbstractElementIteratorFromBatches {
        ElementIteratorFromBatches() throws RetrieverException {
            idsAIterator = seeds.iterator();
            updateScanner();
        }

        protected void updateBloomFilterIfRequired(final EntitySeed seed) throws RetrieverException {
            // NB: Do not reset either of the Bloom filters here - when we query for the first batch of seeds
            // the Bloom filters contain that first set (and so we find edges within that first batch); we next
            // query for the second batch of seeds and the Bloom filters contain both the first batch and the
            // second batch (and so we find edges from the second batch to either the first or second batches).
            addToBloomFilter(seed, filter, clientSideFilter);
        }
    }
}