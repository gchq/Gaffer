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
 * Given two sets of {@link TypeValue}s, called A and B, this retrieves all {@link Edge}s where one end is in set A
 * and the other is in set B. Used by the <code>getGraphElementsWithStatisticsBetweenSets()</code>
 * method in {@link AccumuloBackedGraph}. Note: this also returns {@link Entity}s for {@link TypeValue}s in set B
 * if required.
 *
 * This is done by querying for set A, and uses a {@link BloomFilter}s in the filtering iterator to identify edges
 * that are likely to be between a member of set A and a member of set B. Only these edges are returned to the client,
 * and this reduces the amount of data sent to the client.
 *
 * This operates in two modes. In the first mode the seeds from both sets A and B are loaded into memory (client-side).
 * The seeds from set B are loaded into a {@link BloomFilter}. This is passed to the iterators to filter out all edges
 * for which the non-query end is definitely not in set B. A secondary check is done within this class to check that
 * the edge is definitely between elements of the set (this defeats any false positives, i.e. edges that passed the
 * {@link BloomFilter} check in the iterators). This secondary check uses the in memory set of seeds (and hence there
 * are guaranteed to be no false positives returned to the user).
 *
 * In the second mode, where there are too many seeds to be loaded into memory, the seeds in set A are queried for in
 * batches. The seeds in set B are loaded into two {@link BloomFilter}s. The first of these is relatively small and is
 * passed to the filtering iterator to filter out edges that are definitely not to set B. The second, larger, {@link
 * BloomFilter} is used client-side to further reduce the chances of false positives making it to the user.
 */
public class GraphElementWithStatisticsBetweenSetsRetriever implements CloseableIterable<GraphElementWithStatistics> {

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
    private Iterable<TypeValue> entitiesA;
    private Iterable<TypeValue> entitiesB;
    private boolean someEntitiesProvided;

    // Whether we are reading all the seeds into memory or not
    private boolean readEntriesIntoMemory;

    // Iterator
    private GraphElementWithStatisticsIterator graphElementWithStatisticsIterator;

    public GraphElementWithStatisticsBetweenSetsRetriever(Connector connector, Authorizations auths, String tableName,
                                                          int maxEntriesForBatchScanner, int threadsForBatchScanner,
                                                          int maxBloomFilterToPassToAnIterator,
                                                          double falsePositiveRate,
                                                          int clientSideBloomFilterSize,
                                                          boolean useRollUpOverTimeAndVisibilityIterator,
                                                          Predicate<RawGraphElementWithStatistics> filterPredicate,
                                                          StatisticsTransform statisticsTransform,
                                                          Transform postRollUpTransform,
                                                          boolean returnEntities, boolean returnEdges,
                                                          Iterable<TypeValue> entitiesA,
                                                          Iterable<TypeValue> entitiesB,
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
        this.entitiesA = entitiesA;
        this.entitiesB = entitiesB;
        this.someEntitiesProvided = this.entitiesA.iterator().hasNext() && this.entitiesB.iterator().hasNext();
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
        private Set<TypeValue> typeValuesA;
        private Set<TypeValue> typeValuesB;
        private GraphElementWithStatistics nextGEWS;

        GraphElementWithStatisticsIteratorReadIntoMemory() {
            // Load provided TypeValues into memory
            typeValuesA = new HashSet<TypeValue>();
            for (TypeValue entity : entitiesA) {
                typeValuesA.add(entity);
            }
            typeValuesB = new HashSet<TypeValue>();
            for (TypeValue entity : entitiesB) {
                typeValuesB.add(entity);
            }

            // Create Bloom filter, read through set of entities B and add them to Bloom filter
            BloomFilter filter = BloomFilterUtilities.getBloomFilter(falsePositiveRate, typeValuesB.size(), maxBloomFilterToPassToAnIterator);
            for (TypeValue entity : typeValuesB) {
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
                    postRollUpTransform, returnEntities, returnEdges, typeValuesA);
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
         * Returns <code>true</code> if either an {@link Entity} or if an {@link Edge} then need one end to be in
         * set A and the other to be in set B.
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
            if (typeValuesA.contains(source) && typeValuesB.contains(destination)) {
                return true;
            }
            if (typeValuesB.contains(source) && typeValuesA.contains(destination)) {
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

            // Read through all of set B and add to both Bloom filters
            for (TypeValue entity : entitiesB) {
                clientSideFilter.add(new org.apache.hadoop.util.bloom.Key(entity.getValue().getBytes()));
                filter.add(new org.apache.hadoop.util.bloom.Key(entity.getValue().getBytes()));
            }

            // Read through the first N entities (where N = maxEntriesForBatchScanner), create the associated ranges
            // and add them to a set.
            currentSeeds = new HashSet<TypeValue>();
            entitiesIterator = entitiesA.iterator();
            count = 0;
            Set<Range> ranges = new HashSet<Range>();
            while (entitiesIterator.hasNext() && count < maxEntriesForBatchScanner) {
                TypeValue typeValue = entitiesIterator.next();
                currentSeeds.add(typeValue);
                count++;
                Range range = ConversionUtils.getRangeFromTypeAndValue(typeValue.getType(), typeValue.getValue(), returnEntities, returnEdges);
                ranges.add(range);
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
                    Range range = ConversionUtils.getRangeFromTypeAndValue(typeValue.getType(), typeValue.getValue(), returnEntities, returnEdges);
                    ranges.add(range);
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
