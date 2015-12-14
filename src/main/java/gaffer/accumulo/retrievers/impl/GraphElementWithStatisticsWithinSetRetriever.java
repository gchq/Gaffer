/**
 * Copyright 2015 Crown Copyright
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
package gaffer.accumulo.retrievers.impl;

import gaffer.CloseableIterable;
import gaffer.graph.TypeValue;
import gaffer.utils.BloomFilterUtilities;
import gaffer.accumulo.AccumuloBackedGraph;
import gaffer.accumulo.ConversionUtils;
import gaffer.accumulo.predicate.RawGraphElementWithStatistics;
import gaffer.accumulo.predicate.impl.OtherEndOfEdgePredicate;
import gaffer.graph.Edge;
import gaffer.graph.Entity;
import gaffer.graph.transform.Transform;
import gaffer.graph.wrappers.GraphElement;
import gaffer.graph.wrappers.GraphElementWithStatistics;
import gaffer.predicate.Predicate;
import gaffer.predicate.typevalue.TypeValuePredicate;
import gaffer.predicate.typevalue.impl.ValueInBloomFilterPredicate;
import gaffer.statistics.SetOfStatistics;
import gaffer.statistics.transform.StatisticsTransform;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.util.bloom.BloomFilter;

import java.io.IOException;
import java.util.*;

/**
 * Retrieves {@link Edge}s where both ends are in a given set. Used by the <code>getGraphElementsWithStatisticsWithinSet()</code>
 * method in {@link AccumuloBackedGraph}. Note: also returns {@link Entity}s if required.
 *
 * {@link BloomFilter}s are used to identify on the server edges that are likely to be between members of the set and
 * to send only these to the client. This reduces the amount of data sent to the client.
 *
 * This operates in two modes. In the first mode the seeds are loaded into memory (client-side). They are also loaded
 * into a {@link BloomFilter}. This is passed to the iterators to filter out all edges that are definitely not between
 * elements of the set. A secondary check is done within this class to check that the edge is definitely between
 * elements of the set (this defeats any false positives, i.e. edges that passed the {@link BloomFilter} check
 * in the iterators). This secondary check uses the in memory set of seeds (and hence there are guaranteed to be no
 * false positives returned to the user).
 *
 * In the second mode, where there are too many seeds to be loaded into memory, the seeds are queried one batch at a time.
 * When the first batch is queried for, a {@link BloomFilter} of the first batch is created and passed to the iterators.
 * This filters out all edges that are definitely not between elements of the first batch. When the second batch is
 * queried for, the same {@link BloomFilter} has the second batch added to it. This is passed to the iterators, which
 * filters out all edges that are definitely not between elements of the second batch and the first or second batch.
 * This process repeats until all seeds have been queried for. This is best thought of as a square split into a grid
 * (with the same number of squares in both dimensions). As there are too many seeds to load into memory, we use a
 * client-side {@link BloomFilter} to further reduce the chances of false positives making it to the user.
 */
public class GraphElementWithStatisticsWithinSetRetriever implements CloseableIterable<GraphElementWithStatistics> {

    // Parameters specifying connection to Accumulo
    private Connector connector;
    private Authorizations auths;
    private String tableName;
    private int maxEntriesForBatchScanner;
    private int threadsForBatchScanner;
    private int maxBloomFilterToPassToAnIterator;
    private double falsePositiveRate; // The desired false positive rate from the Bloom filters used in the iterators.
    private int clientSideBloomFilterSize; // The size of the client-side Bloom filter that is used as a secondary
    // test to defeat false positives.

    // View on data
    private boolean useRollUpOverTimeAndVisibilityIterator;
    private Predicate<RawGraphElementWithStatistics> filterPredicate;
    private StatisticsTransform statisticsTransform;
    private Transform postRollUpTransform;
    private boolean returnEntities;
    private boolean returnEdges;

    // TypeValues to retrieve data for
    private Iterable<TypeValue> entities;
    private boolean someEntitiesProvided;

    // Whether we are reading all the seeds into memory or not
    private boolean readEntriesIntoMemory;

    // Iterator
    private GraphElementWithStatisticsIterator graphElementWithStatisticsIterator;

    public GraphElementWithStatisticsWithinSetRetriever(Connector connector, Authorizations auths, String tableName,
                                                        int maxEntriesForBatchScanner, int threadsForBatchScanner,
                                                        int maxBloomFilterToPassToAnIterator,
                                                        double falsePositiveRate,
                                                        int clientSideBloomFilterSize,
                                                        boolean useRollUpOverTimeAndVisibilityIterator,
                                                        Predicate<RawGraphElementWithStatistics> filterPredicate,
                                                        StatisticsTransform statisticsTransform,
                                                        Transform postRollUpTransform,
                                                        boolean returnEntities, boolean returnEdges,
                                                        Iterable<TypeValue> entities,
                                                        boolean readEntriesIntoMemory) {
        this.connector = connector;
        this.auths = auths;
        this.tableName = tableName;
        this.maxEntriesForBatchScanner = maxEntriesForBatchScanner;
        this.threadsForBatchScanner = threadsForBatchScanner;
        this.maxBloomFilterToPassToAnIterator = maxBloomFilterToPassToAnIterator;
        this.falsePositiveRate = falsePositiveRate;
        this.clientSideBloomFilterSize = clientSideBloomFilterSize;
        this.useRollUpOverTimeAndVisibilityIterator = useRollUpOverTimeAndVisibilityIterator;
        this.filterPredicate = filterPredicate;
        this.statisticsTransform = statisticsTransform;
        this.postRollUpTransform = postRollUpTransform;
        this.returnEntities = returnEntities;
        this.returnEdges = returnEdges;
        this.entities = entities;
        this.someEntitiesProvided = this.entities.iterator().hasNext();
        this.readEntriesIntoMemory = readEntriesIntoMemory;
    }

    @Override
    public void close() {
        if (graphElementWithStatisticsIterator != null) {
            graphElementWithStatisticsIterator.close();
        }
    }

    @Override
    public Iterator<GraphElementWithStatistics> iterator() {
        if (!someEntitiesProvided) {
            return Collections.emptyIterator();
        }
        if (readEntriesIntoMemory) {
            graphElementWithStatisticsIterator = new GraphElementWithStatisticsIteratorReadIntoMemory();
        } else {
            graphElementWithStatisticsIterator = new GraphElementWithStatisticsIteratorFromBatches();
        }
        return graphElementWithStatisticsIterator;
    }

    private interface GraphElementWithStatisticsIterator extends Iterator<GraphElementWithStatistics> {

        void close();

    }

    private class GraphElementWithStatisticsIteratorReadIntoMemory implements GraphElementWithStatisticsIterator {

        private CloseableIterable<GraphElementWithStatistics> parentRetriever;
        private Iterator<GraphElementWithStatistics> iterator;
        private Set<TypeValue> typeValues;
        private GraphElementWithStatistics nextGEWS;

        GraphElementWithStatisticsIteratorReadIntoMemory() {
            // Load provided TypeValues into memory
            this.typeValues = new HashSet<TypeValue>();
            for (TypeValue se : entities) {
                this.typeValues.add(se);
            }

            // Create Bloom filter, read through set of entities and add them to Bloom filter
            BloomFilter filter = BloomFilterUtilities.getBloomFilter(falsePositiveRate, this.typeValues.size(), maxBloomFilterToPassToAnIterator);
            for (TypeValue entity : this.typeValues) {
                filter.add(new org.apache.hadoop.util.bloom.Key(entity.getValue().getBytes()));
            }

            // Create TypeValuePredicate from Bloom filter and use that to create an OtherEndOfEdgePredicate.
            TypeValuePredicate bloomPredicate = new ValueInBloomFilterPredicate(filter);
            OtherEndOfEdgePredicate otherEndPredicate = new OtherEndOfEdgePredicate(bloomPredicate);

            // Create overall filtering predicate as usual, and add in Bloom filter predicate
            Predicate<RawGraphElementWithStatistics> predicate = AccumuloBackedGraph.andPredicates(otherEndPredicate, filterPredicate);

            CloseableIterable<GraphElementWithStatistics> parentRetriever = new GraphElementWithStatisticsRetrieverFromEntities(connector, auths,
                    tableName, maxEntriesForBatchScanner, threadsForBatchScanner,
                    useRollUpOverTimeAndVisibilityIterator, predicate, statisticsTransform,
                    postRollUpTransform, returnEntities, returnEdges, typeValues);
            this.parentRetriever = parentRetriever;
            this.iterator = parentRetriever.iterator();
        }

        @Override
        public boolean hasNext() {
            while (iterator.hasNext()) {
                nextGEWS = iterator.next();
                if (checkIfBothEndsInSet(nextGEWS)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public GraphElementWithStatistics next() {
            return nextGEWS;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Can't remove elements from a graph element iterator");
        }

        public void close() {
            if (parentRetriever != null) {
                parentRetriever.close();
            }
        }

        /**
         * Returns <code>true</code> if either an {@link Entity} or if an {@link Edge} then need both ends
         * to be in the set.
         *
         * @param gews
         * @return
         */
        private boolean checkIfBothEndsInSet(GraphElementWithStatistics gews) {
            if (gews.isEntity()) {
                return true;
            }
            TypeValue source = gews.getGraphElement().getEdge().getSourceAsTypeValue();
            TypeValue destination = gews.getGraphElement().getEdge().getDestinationAsTypeValue();
            if (typeValues.contains(source) && typeValues.contains(destination)) {
                return true;
            }
            return false;
        }
    }

    private class GraphElementWithStatisticsIteratorFromBatches implements GraphElementWithStatisticsIterator {

        private BatchScanner scanner;
        private Iterator<TypeValue> entitiesIterator;
        private Set<TypeValue> currentSeeds; // Store the set of seeds that are currently being queried for to enable
            // secondary check.
        private Iterator<Map.Entry<Key,Value>> scannerIterator;
        private int count;
        private BloomFilter filter;
        private BloomFilter clientSideFilter; // The Bloom filter that is maintained client-side as a secondary defeat
            // of false positives.
        private GraphElementWithStatistics nextGEWS;

        GraphElementWithStatisticsIteratorFromBatches() {
            // Set up client side filter
            clientSideFilter = BloomFilterUtilities.getBloomFilter(clientSideBloomFilterSize);

            // Create Bloom filter to be passed to iterators.
            filter = BloomFilterUtilities.getBloomFilter(falsePositiveRate, maxEntriesForBatchScanner, maxBloomFilterToPassToAnIterator);

            // Read through the first N entities (where N = maxEntriesForBatchScanner), create the associated ranges
            // and add them to a set, also add them to the Bloom filter.
            currentSeeds = new HashSet<TypeValue>();
            entitiesIterator = entities.iterator();
            count = 0;
            Set<Range> ranges = new HashSet<Range>();
            while (entitiesIterator.hasNext() && count < maxEntriesForBatchScanner) {
                TypeValue typeValue = entitiesIterator.next();
                currentSeeds.add(typeValue);
                count++;
                Range range = ConversionUtils.getRangeFromTypeAndValue(typeValue.getType(), typeValue.getValue(), returnEntities, returnEdges);
                ranges.add(range);
                filter.add(new org.apache.hadoop.util.bloom.Key(typeValue.getValue().getBytes()));
                clientSideFilter.add(new org.apache.hadoop.util.bloom.Key(typeValue.getValue().getBytes()));
            }

            TypeValuePredicate bloomPredicate = new ValueInBloomFilterPredicate(filter);
            OtherEndOfEdgePredicate otherEndPredicate = new OtherEndOfEdgePredicate(bloomPredicate);

            // Add Bloom filter predicate to existing filter
            Predicate<RawGraphElementWithStatistics> predicate = AccumuloBackedGraph.andPredicates(otherEndPredicate, filterPredicate);

            try {
                scanner = RetrieverUtilities.getScanner(connector, auths, tableName,
                        threadsForBatchScanner, useRollUpOverTimeAndVisibilityIterator,
                        predicate, statisticsTransform);
                scanner.setRanges(ranges);
                scannerIterator = scanner.iterator();
            } catch (TableNotFoundException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public boolean hasNext() {
            while (_hasNext()) {
                Map.Entry<Key,Value> entry = scannerIterator.next();
                try {
                    GraphElement ge = ConversionUtils.getGraphElementFromKey(entry.getKey());
                    SetOfStatistics setOfStatistics = ConversionUtils.getSetOfStatisticsFromValue(entry.getValue());
                    nextGEWS = new GraphElementWithStatistics(ge, setOfStatistics);
                } catch (IOException e) {
                    nextGEWS = null;
                }
                if (secondaryCheck(nextGEWS)) {
                    return true;
                }
            }
            return false;
        }

        public boolean _hasNext() {
            // If current scanner has next then return true.
            if (scannerIterator.hasNext()) {
                return true;
            }
            // If current scanner is spent then go back to the iterator
            // through the provided entities, and see if there are more.
            // If so create the next scanner, if there are no more entities
            // then return false.
            while (entitiesIterator.hasNext() && !scannerIterator.hasNext()) {
                count = 0;
                Set<Range> ranges = new HashSet<Range>();
                currentSeeds.clear();

                while (entitiesIterator.hasNext() && count < maxEntriesForBatchScanner) {
                    TypeValue typeValue = entitiesIterator.next();
                    currentSeeds.add(typeValue);
                    count++;
                    // Get key and use to create appropriate range
                    Range range = ConversionUtils.getRangeFromTypeAndValue(typeValue.getType(), typeValue.getValue(), returnEntities, returnEdges);
                    ranges.add(range);
                    // NB: Do not reset either of the Bloom filters here - when we query for the first batch of seeds
                    // the Bloom filters contain that first set (and so we find edges within that first batch); we next
                    // query for the second batch of seeds and the Bloom filters contain both the first batch and the
                    // second batch (and so we find edges from the second batch to either the first or second batches).
                    filter.add(new org.apache.hadoop.util.bloom.Key(typeValue.getValue().getBytes()));
                    clientSideFilter.add(new org.apache.hadoop.util.bloom.Key(typeValue.getValue().getBytes()));
                }
                // Following 2 lines are probably not necessary as they should still point at the
                // current Bloom filter
                TypeValuePredicate bloomPredicate = new ValueInBloomFilterPredicate(filter);
                OtherEndOfEdgePredicate otherEndPredicate = new OtherEndOfEdgePredicate(bloomPredicate);

                // Add Bloom filter predicate to existing filter
                Predicate<RawGraphElementWithStatistics> predicate = AccumuloBackedGraph.andPredicates(otherEndPredicate, filterPredicate);

                try {
                    scanner.close();
                    scanner = RetrieverUtilities.getScanner(connector, auths, tableName,
                            threadsForBatchScanner, useRollUpOverTimeAndVisibilityIterator,
                            predicate, statisticsTransform);
                    scanner.setRanges(ranges);
                    scannerIterator = scanner.iterator();
                } catch (TableNotFoundException e) {
                    throw new RuntimeException(e);
                }
            }
            if (!scannerIterator.hasNext()) {
                scanner.close();
            }
            return scannerIterator.hasNext();
        }

        @Override
        public GraphElementWithStatistics next() {
            if (postRollUpTransform == null) {
                return nextGEWS;
            }
            return postRollUpTransform.transform(nextGEWS);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Can't remove elements from a " + this.getClass().getCanonicalName());
        }

        public void close() {
            if (scanner != null) {
                scanner.close();
            }
        }

        /**
         * Check whether this is valid, i.e. one end is in the current set of seeds that are being queried for and the
         * other matches the Bloom filter (i.e. the client side Bloom filter that is being used as a secondary defeat
         * of false positives).
         */
        private boolean secondaryCheck(GraphElementWithStatistics gews) {
            if (gews.isEntity()) {
                return true;
            }
            TypeValue source = gews.getGraphElement().getEdge().getSourceAsTypeValue();
            TypeValue destination = gews.getGraphElement().getEdge().getDestinationAsTypeValue();
            boolean sourceIsInCurrent = currentSeeds.contains(source);
            boolean destIsInCurrent = currentSeeds.contains(destination);
            boolean sourceMatchesClientFilter = clientSideFilter.membershipTest(new org.apache.hadoop.util.bloom.Key(source.getValue().getBytes()));
            boolean destMatchesClientFilter = clientSideFilter.membershipTest(new org.apache.hadoop.util.bloom.Key(destination.getValue().getBytes()));
            if (sourceIsInCurrent && destMatchesClientFilter) {
                return true;
            }
            if (destIsInCurrent && sourceMatchesClientFilter) {
                return true;
            }
            if (sourceIsInCurrent && destIsInCurrent) {
                return true;
            }
            return false;
        }
    }

}
