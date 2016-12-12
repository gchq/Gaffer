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

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.key.exception.AccumuloElementConversionException;
import uk.gov.gchq.gaffer.accumulostore.operation.AbstractAccumuloTwoSetSeededOperation;
import uk.gov.gchq.gaffer.accumulostore.retriever.AccumuloSetRetriever;
import uk.gov.gchq.gaffer.accumulostore.retriever.RetrieverException;
import uk.gov.gchq.gaffer.accumulostore.utils.BloomFilterUtils;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.user.User;
import java.util.Iterator;
import java.util.Set;

/**
 * Given two sets of {@link uk.gov.gchq.gaffer.operation.data.EntitySeed}s, called A and B,
 * this retrieves all {@link uk.gov.gchq.gaffer.data.element.Edge}s where one end is in set
 * A and the other is in set B and also returns
 * {@link uk.gov.gchq.gaffer.data.element.Entity}s for
 * {@link uk.gov.gchq.gaffer.operation.data.EntitySeed}s in set A.
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
    private Iterator<EntitySeed> seedSetAIter;
    private Iterator<EntitySeed> seedSetBIter;


    public AccumuloIDBetweenSetsRetriever(final AccumuloStore store,
                                          final AbstractAccumuloTwoSetSeededOperation<EntitySeed, ?> operation,
                                          final User user,
                                          final IteratorSetting... iteratorSettings) throws StoreException {
        this(store, operation, user, false, iteratorSettings);
    }

    public AccumuloIDBetweenSetsRetriever(final AccumuloStore store,
                                          final AbstractAccumuloTwoSetSeededOperation<EntitySeed, ?> operation,
                                          final User user,
                                          final boolean readEntriesIntoMemory,
                                          final IteratorSetting... iteratorSettings) throws StoreException {
        super(store, operation, user, readEntriesIntoMemory, iteratorSettings);
        setSeeds(operation.getSeeds(), operation.getSeedsB());
    }

    private void setSeeds(final Iterable<EntitySeed> setA, final Iterable<EntitySeed> setB) {
        this.seedSetA = setA;
        this.seedSetB = setB;
    }

    @Override
    protected boolean hasSeeds() {
        seedSetAIter = seedSetA.iterator();
        seedSetBIter = seedSetB.iterator();
        return seedSetAIter.hasNext() && seedSetBIter.hasNext();
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
            verticesA = extractVertices(seedSetAIter);
            verticesB = extractVertices(seedSetBIter);

            // Create Bloom filter, read through set of entities B and add them
            // to Bloom filter
            final BloomFilter filter = BloomFilterUtils.getBloomFilter(store.getProperties().getFalsePositiveRate(),
                    verticesB.size(), store.getProperties().getMaxBloomFilterToPassToAnIterator());
            addToBloomFilter(verticesB, filter);
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
            addToBloomFilter(seedSetBIter, filter, clientSideFilter);
            idsAIterator = seedSetAIter;
            updateScanner();
        }

        @Override
        protected void updateBloomFilterIfRequired(final EntitySeed seed) throws RetrieverException {
            // no action required.
        }

        protected boolean secondaryCheck(final Element elm) {
            if (Entity.class.isInstance(elm)) {
                return true;
            }
            final Edge edge = (Edge) elm;
            final Object source = edge.getSource();
            final Object destination = edge.getDestination();
            final boolean sourceIsInCurrent = currentSeeds.contains(source);
            boolean destMatchesClientFilter;
            try {
                destMatchesClientFilter = clientSideFilter.membershipTest(
                        new Key(elementConverter.serialiseVertex(destination)));
            } catch (final AccumuloElementConversionException e) {
                return false;
            }
            if (sourceIsInCurrent && destMatchesClientFilter) {
                return true;
            }
            final boolean destIsInCurrent = currentSeeds.contains(destination);
            boolean sourceMatchesClientFilter;
            try {
                sourceMatchesClientFilter = clientSideFilter.membershipTest(
                        new Key(elementConverter.serialiseVertex(source)));
            } catch (final AccumuloElementConversionException e) {
                return false;
            }
            return (destIsInCurrent && sourceMatchesClientFilter);
        }
    }
}
