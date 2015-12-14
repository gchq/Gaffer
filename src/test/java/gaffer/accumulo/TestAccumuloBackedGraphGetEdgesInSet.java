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
package gaffer.accumulo;

import gaffer.CloseableIterable;
import gaffer.graph.TypeValue;
import gaffer.utils.BloomFilterUtilities;
import gaffer.GraphAccessException;
import gaffer.graph.Edge;
import gaffer.graph.Entity;
import gaffer.graph.wrappers.GraphElement;
import gaffer.graph.wrappers.GraphElementWithStatistics;
import gaffer.statistics.SetOfStatistics;
import gaffer.statistics.impl.Count;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Unit test of the <code>getGraphElementsWithStatisticsWithinSet()</code> method from {@link AccumuloBackedGraph}.
 * Contains tests that the correct edges are returned and that false positives are filtered out. Also tests that
 * standard filtering that {@link AccumuloBackedGraph} offers (e.g. by summary type or time window) is still applied
 * when this method is used.
 */
public class TestAccumuloBackedGraphGetEdgesInSet {

    private static Date sevenDaysBefore;
    private static Date sixDaysBefore;
    private static String visibilityString1 = "private";

    static {
        Calendar sevenDaysBeforeCalendar = new GregorianCalendar();
        sevenDaysBeforeCalendar.setTime(new Date(System.currentTimeMillis()));
        sevenDaysBeforeCalendar.add(Calendar.DAY_OF_MONTH, -7);
        sevenDaysBefore = sevenDaysBeforeCalendar.getTime();
        sevenDaysBeforeCalendar.add(Calendar.DAY_OF_MONTH, 1);
        sixDaysBefore = sevenDaysBeforeCalendar.getTime();
    }

    /**
     * Tests that the correct {@link Edge}s are returned. Tests that {@link Entity}s are also returned
     * (unless the return edges only option has been set on the {@link AccumuloBackedGraph}).
     */
    @Test
    public void testGetCorrectEdges() {
        testGetCorrectEdges(true);
        testGetCorrectEdges(false);
    }

    static void testGetCorrectEdges(boolean loadIntoMemory) {
        AccumuloBackedGraph graph = setupGraph();

        // Query for all edges in set {customer|A0, customer|A23}
        Set<TypeValue> seeds = new HashSet<TypeValue>();
        seeds.add(new TypeValue("customer", "A0"));
        seeds.add(new TypeValue("customer", "A23"));
        CloseableIterable<GraphElementWithStatistics> retriever = graph.getGraphElementsWithStatisticsWithinSet(seeds, loadIntoMemory);
        Set<GraphElementWithStatistics> results = new HashSet<GraphElementWithStatistics>();
        for (GraphElementWithStatistics gews : retriever) {
            results.add(gews);
        }
        retriever.close();
        Set<GraphElementWithStatistics> expectedResults = new HashSet<GraphElementWithStatistics>();
        GraphElement expectedElement1 = new GraphElement(new Edge("customer", "A0", "customer", "A23", "purchase",
                "instore", true, visibilityString1, sevenDaysBefore, sixDaysBefore));
        SetOfStatistics expectedStatistics1 = new SetOfStatistics("count", new Count(23));
        expectedResults.add(new GraphElementWithStatistics(expectedElement1, expectedStatistics1));
        GraphElement expectedElement2 = new GraphElement(new Entity("customer", "A0", "purchase", "instore",
                visibilityString1, sevenDaysBefore, sixDaysBefore));
        SetOfStatistics expectedStatistics2 = new SetOfStatistics("count", new Count(10000));
        expectedResults.add(new GraphElementWithStatistics(expectedElement2, expectedStatistics2));
        GraphElement expectedElement3 = new GraphElement(new Entity("customer", "A23", "purchase", "instore",
                visibilityString1, sevenDaysBefore, sixDaysBefore));
        SetOfStatistics expectedStatistics3 = new SetOfStatistics("count", new Count(23));
        expectedResults.add(new GraphElementWithStatistics(expectedElement3, expectedStatistics3));
        assertEquals(expectedResults, results);

        // Query for all edges in set {customer|A1} - there shouldn't be any, but we will get the entity for A1
        seeds.clear();
        seeds.add(new TypeValue("customer", "A1"));
        retriever = graph.getGraphElementsWithStatisticsWithinSet(seeds, loadIntoMemory);
        results.clear();
        int count = 0;
        for (GraphElementWithStatistics gews : retriever) {
            count++;
            results.add(gews);
        }
        expectedResults.clear();
        expectedElement1 = new GraphElement(new Entity("customer", "A1", "purchase", "instore", visibilityString1,
                sevenDaysBefore, sixDaysBefore));
        expectedStatistics1 = new SetOfStatistics("count", new Count(1));
        expectedResults.add(new GraphElementWithStatistics(expectedElement1, expectedStatistics1));
        assertEquals(1, count);
        assertEquals(expectedResults, results);

        // Query for all edges in set {customer|A1, customer|A2} - there shouldn't be any edges but will
        // get the two entities
        seeds.clear();
        seeds.add(new TypeValue("customer", "A1"));
        seeds.add(new TypeValue("customer", "A2"));
        retriever = graph.getGraphElementsWithStatisticsWithinSet(seeds, loadIntoMemory);
        results.clear();
        count = 0;
        for (GraphElementWithStatistics gews : retriever) {
            count++;
            results.add(gews);
        }
        expectedElement1 = new GraphElement(new Entity("customer", "A1", "purchase", "instore", visibilityString1,
                sevenDaysBefore, sixDaysBefore));
        expectedStatistics1 = new SetOfStatistics("count", new Count(1));
        expectedResults.add(new GraphElementWithStatistics(expectedElement1, expectedStatistics1));
        expectedElement2 = new GraphElement(new Entity("customer", "A2", "purchase", "instore", visibilityString1,
                sevenDaysBefore, sixDaysBefore));
        expectedStatistics2 = new SetOfStatistics("count", new Count(2));
        expectedResults.add(new GraphElementWithStatistics(expectedElement2, expectedStatistics2));
        assertEquals(2, count);
        assertEquals(expectedResults, results);
    }

    /**
     * Tests that the subtle case of setting outgoing or incoming edges only option is dealt with correctly.
     * When querying for edges within a set, the outgoing or incoming edges only needs to be turned off, for
     * two reasons. First, it doesn't make conceptual sense. If the each is from a member of set X to another
     * member of set X, what would it mean for it to be "outgoing"? (It makes sense to ask for directed edges
     * only, or undirected edges only.) Second, if the option is left on then results can be missed. For example,
     * suppose we have a graph with an edge A->B and we ask for all edges with both ends in the set {A,B}. Consider
     * what happens using the batching mechanism, with A in the first batch and B in the second batch. When the
     * first batch is queried for, the Bloom filter will consist solely of {A}. Thus the edge A->B will not be
     * returned. When the next batch is queried for, the Bloom filter will consist of A and B, so normally the
     * edge A->B will be returned. But if the outgoing edges only option is turned on then the edge will not be
     * returned, as it is not an edge out of B.
     *
     * This unit tests creates that situation and tests that the edge is still returned. Effectively this is
     * testing that the outgoing edges only (and incoming edges only) option is ignored when the
     * <code>getGraphElementsWithStatisticsWithinSet</code> method is used.
     */
    @Test
    public void testDealWithOutgoingEdgesOnlyOption() {
        Instance instance = new MockInstance();
        String tableName = "Test";
        long ageOffTimeInMilliseconds = (30 * 24 * 60 * 60 * 1000L); // 30 days in milliseconds

        try {
            // Open connection
            Connector conn = instance.getConnector("user", "password");

            // Create table
            // (this method creates the table, removes the versioning iterator, and adds the SetOfStatisticsCombiner iterator,
            // and sets the age off iterator to age data off after it is more than ageOffTimeInMilliseconds milliseconds old).
            TableUtils.createTable(conn, tableName, ageOffTimeInMilliseconds);

            // Create set of GraphElementWithStatistics to store data before adding it to the graph.
            Set<GraphElementWithStatistics> data = new HashSet<GraphElementWithStatistics>();

            // Create edge A -> B
            Edge edge = new Edge("customer", "A", "customer", "B", "purchase", "instore", true, visibilityString1, sevenDaysBefore, sixDaysBefore);
            data.add(new GraphElementWithStatistics(new GraphElement(edge), new SetOfStatistics("count", new Count(1))));

            // Create Accumulo backed graph
            AccumuloBackedGraph graph = new AccumuloBackedGraph(conn, tableName);

            // Add data
            graph.addGraphElementsWithStatistics(data);

            // Set graph up for query
            graph.setAuthorizations(new Authorizations(visibilityString1));

            // Set outgoing edges only option, and query for the set {A,B}. If we set the batch size to 1 then this
            // should fail.
            graph.setOutgoingEdgesOnly();
            graph.setMaxEntriesForBatchScanner(1);
            SortedSet<TypeValue> seeds = new TreeSet<TypeValue>();
            seeds.add(new TypeValue("customer", "A"));
            seeds.add(new TypeValue("customer", "B"));
            CloseableIterable<GraphElementWithStatistics> retriever = graph.getGraphElementsWithStatisticsWithinSet(seeds, false);
            Set<GraphElementWithStatistics> results = new HashSet<GraphElementWithStatistics>();
            for (GraphElementWithStatistics gews : retriever) {
                results.add(gews);
            }
            retriever.close();
            Set<GraphElementWithStatistics> expectedResults = new HashSet<GraphElementWithStatistics>();
            expectedResults.add(new GraphElementWithStatistics(new GraphElement(edge), new SetOfStatistics("count", new Count(1))));
            assertEquals(expectedResults, results);

        } catch (AccumuloException e) {
            fail("Failed to set up graph in Accumulo with exception: " + e);
        } catch (AccumuloSecurityException e) {
            fail("Failed to set up graph in Accumulo with exception: " + e);
        } catch (TableExistsException e) {
            fail("Failed to set up graph in Accumulo with exception: " + e);
        } catch (TableNotFoundException e) {
            fail("Failed to set up graph in Accumulo with exception: " + e);
        } catch (GraphAccessException e) {
            fail("Failed to set up graph in Accumulo with exception: " + e);
        }
    }

    /**
     * Tests that the directed edges only and undirected edges only options are respected.
     */
    @Test
    public void testDealWithDirectedEdgesOnlyOption() {
        testDealWithDirectedEdgesOnlyOption(true);
        testDealWithDirectedEdgesOnlyOption(false);
    }

    static void testDealWithDirectedEdgesOnlyOption(boolean loadIntoMemory) {
        Instance instance = new MockInstance();
        String tableName = "Test";
        long ageOffTimeInMilliseconds = (30 * 24 * 60 * 60 * 1000L); // 30 days in milliseconds

        try {
            // Open connection
            Connector conn = instance.getConnector("user", "password");

            // Create table
            // (this method creates the table, removes the versioning iterator, and adds the SetOfStatisticsCombiner iterator,
            // and sets the age off iterator to age data off after it is more than ageOffTimeInMilliseconds milliseconds old).
            TableUtils.createTable(conn, tableName, ageOffTimeInMilliseconds);

            // Create set of GraphElementWithStatistics to store data before adding it to the graph.
            Set<GraphElementWithStatistics> data = new HashSet<GraphElementWithStatistics>();

            // Create directed edge A -> B and undirected edge A - B
            Edge edge1 = new Edge("customer", "A", "customer", "B", "purchase", "instore", true, visibilityString1, sevenDaysBefore, sixDaysBefore);
            Edge edge2 = new Edge("customer", "A", "customer", "B", "purchase", "instore", false, visibilityString1, sevenDaysBefore, sixDaysBefore);
            data.add(new GraphElementWithStatistics(new GraphElement(edge1), new SetOfStatistics("count", new Count(1))));
            data.add(new GraphElementWithStatistics(new GraphElement(edge2), new SetOfStatistics("count", new Count(2))));

            // Create Accumulo backed graph
            AccumuloBackedGraph graph = new AccumuloBackedGraph(conn, tableName);

            // Add data
            graph.addGraphElementsWithStatistics(data);

            // Set graph up for query
            graph.setAuthorizations(new Authorizations(visibilityString1));

            // Set undirected edges only option, and query for edges in set {A, B} - should get edge2
            graph.setUndirectedEdgesOnly();
            Set<TypeValue> seeds = new HashSet<TypeValue>();
            seeds.add(new TypeValue("customer", "A"));
            seeds.add(new TypeValue("customer", "B"));
            CloseableIterable<GraphElementWithStatistics> retriever = graph.getGraphElementsWithStatisticsWithinSet(seeds, loadIntoMemory);
            Set<GraphElementWithStatistics> results = new HashSet<GraphElementWithStatistics>();
            for (GraphElementWithStatistics gews : retriever) {
                results.add(gews);
            }
            retriever.close();
            Set<GraphElementWithStatistics> expectedResults = new HashSet<GraphElementWithStatistics>();
            expectedResults.add(new GraphElementWithStatistics(new GraphElement(edge2), new SetOfStatistics("count", new Count(2))));
            assertEquals(expectedResults, results);

            // Set directed edges only option, and query for edges in set {A, B} - should get edge1
            graph.setDirectedEdgesOnly();
            retriever = graph.getGraphElementsWithStatisticsWithinSet(seeds, loadIntoMemory);
            results.clear();
            for (GraphElementWithStatistics gews : retriever) {
                results.add(gews);
            }
            retriever.close();
            expectedResults.clear();
            expectedResults.add(new GraphElementWithStatistics(new GraphElement(edge1), new SetOfStatistics("count", new Count(1))));
            assertEquals(expectedResults, results);

            // Turn off directed / undirected edges only option and check get both edge1 and edge2
            graph.setUndirectedAndDirectedEdges();
            retriever = graph.getGraphElementsWithStatisticsWithinSet(seeds, loadIntoMemory);
            results.clear();
            for (GraphElementWithStatistics gews : retriever) {
                results.add(gews);
            }
            retriever.close();
            expectedResults.add(new GraphElementWithStatistics(new GraphElement(edge2), new SetOfStatistics("count", new Count(2))));
            assertEquals(expectedResults, results);

        } catch (AccumuloException e) {
            fail("Failed to set up graph in Accumulo with exception: " + e);
        } catch (AccumuloSecurityException e) {
            fail("Failed to set up graph in Accumulo with exception: " + e);
        } catch (TableExistsException e) {
            fail("Failed to set up graph in Accumulo with exception: " + e);
        } catch (TableNotFoundException e) {
            fail("Failed to set up graph in Accumulo with exception: " + e);
        } catch (GraphAccessException e) {
            fail("Failed to set up graph in Accumulo with exception: " + e);
        }
    }

    /**
     * Tests that false positives are filtered out. It does this by explicitly finding a false positive (i.e. something
     * that matches the Bloom filter but that wasn't put into the filter) and adding that to the data, and then
     * checking that isn't returned.
     *
     * @throws GraphAccessException
     */
    @Test
    public void testDealWithFalsePositives() throws GraphAccessException {
        testDealWithFalsePositives(true);
        testDealWithFalsePositives(false);
    }

    static void testDealWithFalsePositives(boolean loadIntoMemory) throws GraphAccessException {
        AccumuloBackedGraph graph = setupGraph();

        // Query for all edges in set {customer|A0, customer|A23}
        Set<TypeValue> seeds = new HashSet<TypeValue>();
        seeds.add(new TypeValue("customer", "A0"));
        seeds.add(new TypeValue("customer", "A23"));
        // Add a bunch of items that are not in the data to make the probability of being able to find a false
        // positive sensible.
        for (int i = 0; i < 10; i++) {
            seeds.add(new TypeValue("abc", "abc" + i));
        }

        // Need to make sure that the Bloom filter we create has the same size and the same number of hashes as the
        // one that GraphElementsWithStatisticsWithinSetRetriever creates.
        int numItemsToBeAdded = loadIntoMemory ? seeds.size() : 20;
        if (!loadIntoMemory) {
            graph.setMaxEntriesForBatchScanner(20);
        }

        // Find something that will give a false positive
        // Need to repeat the logic used in the getGraphElementsWithStatisticsWithinSet() method.
        // Calculate sensible size of filter, aiming for false positive rate of 1 in 10000, with a maximum size of
        // maxBloomFilterToPassToAnIterator bytes.
        int size = (int) (-numItemsToBeAdded * Math.log(0.0001) / (Math.pow(Math.log(2.0), 2.0)));
        size = Math.min(size, Constants.MAX_SIZE_BLOOM_FILTER);

        // Work out optimal number of hashes to use in Bloom filter based on size of set - optimal number of hashes is
        // (m/n)ln 2 where m is the size of the filter in bits and n is the number of items that will be added to the set.
        int numHashes = Math.max(1, (int) ((size / numItemsToBeAdded) * Math.log(2)));
        // Create Bloom filter and add seeds to it
        BloomFilter filter = new BloomFilter(size, numHashes, Hash.MURMUR_HASH);
        for (TypeValue entity : seeds) {
            filter.add(new Key(entity.getValue().getBytes()));
        }

        // Test random items against it - should only have to test MAX_SIZE_BLOOM_FILTER / 2 on average before find a
        // false positive (but impose an arbitrary limit to avoid an infinite loop if there's a problem).
        int count = 0;
        int maxNumberOfTries = 50 * Constants.MAX_SIZE_BLOOM_FILTER;
        while (count < maxNumberOfTries) {
            count++;
            if (filter.membershipTest(new Key(("" + count).getBytes()))) {
                break;
            }
        }
        if (count == maxNumberOfTries) {
            fail("Didn't find a false positive");
        }

        // False positive is "" + count so create an edge from seeds to that
        Edge edge = new Edge("customer", "A0", "customer", "" + count, "purchase", "instore", true, visibilityString1, sevenDaysBefore, sixDaysBefore);
        SetOfStatistics statistics = new SetOfStatistics("count", new Count(1000000));
        graph.addGraphElementsWithStatistics(Collections.singleton(new GraphElementWithStatistics(new GraphElement(edge), statistics)));

        // Now query for all edges in set - shouldn't get the false positive
        CloseableIterable<GraphElementWithStatistics> retriever = graph.getGraphElementsWithStatisticsWithinSet(seeds, loadIntoMemory);
        Set<GraphElementWithStatistics> results = new HashSet<GraphElementWithStatistics>();
        for (GraphElementWithStatistics gews : retriever) {
            results.add(gews);
        }
        retriever.close();

        // Check results are as expected
        Set<GraphElementWithStatistics> expectedResults = new HashSet<GraphElementWithStatistics>();
        GraphElement expectedElement1 = new GraphElement(new Edge("customer", "A0", "customer", "A23", "purchase",
                "instore", true, visibilityString1, sevenDaysBefore, sixDaysBefore));
        SetOfStatistics expectedStatistics1 = new SetOfStatistics("count", new Count(23));
        expectedResults.add(new GraphElementWithStatistics(expectedElement1, expectedStatistics1));
        GraphElement expectedElement2 = new GraphElement(new Entity("customer", "A0", "purchase", "instore",
                visibilityString1, sevenDaysBefore, sixDaysBefore));
        SetOfStatistics expectedStatistics2 = new SetOfStatistics("count", new Count(10000));
        expectedResults.add(new GraphElementWithStatistics(expectedElement2, expectedStatistics2));
        GraphElement expectedElement3 = new GraphElement(new Entity("customer", "A23", "purchase", "instore",
                visibilityString1, sevenDaysBefore, sixDaysBefore));
        SetOfStatistics expectedStatistics3 = new SetOfStatistics("count", new Count(23));
        expectedResults.add(new GraphElementWithStatistics(expectedElement3, expectedStatistics3));
        assertEquals(expectedResults, results);
    }

    /**
     * Tests that standard filtering (e.g. by summary type, or by time window, or to only receive entities) is still
     * applied.
     */
    @Test
    public void testOtherFilteringStillApplied() {
        testOtherFilteringStillApplied(true);
        testOtherFilteringStillApplied(false);
    }

    static void testOtherFilteringStillApplied(boolean loadIntoMemory) {
        AccumuloBackedGraph graph = setupGraph();

        // Set graph to give us edges only
        graph.setReturnEdgesOnly();

        // Query for all edges in set {customer|A0, customer|A23}
        Set<TypeValue> seeds = new HashSet<TypeValue>();
        seeds.add(new TypeValue("customer", "A0"));
        seeds.add(new TypeValue("customer", "A23"));
        CloseableIterable<GraphElementWithStatistics> retriever = graph.getGraphElementsWithStatisticsWithinSet(seeds, loadIntoMemory);
        Set<GraphElementWithStatistics> results = new HashSet<GraphElementWithStatistics>();
        for (GraphElementWithStatistics gews : retriever) {
            results.add(gews);
        }
        retriever.close();
        Set<GraphElementWithStatistics> expectedResults = new HashSet<GraphElementWithStatistics>();
        GraphElement expectedElement1 = new GraphElement(new Edge("customer", "A0", "customer", "A23", "purchase",
                "instore", true, visibilityString1, sevenDaysBefore, sixDaysBefore));
        SetOfStatistics expectedStatistics1 = new SetOfStatistics("count", new Count(23));
        expectedResults.add(new GraphElementWithStatistics(expectedElement1, expectedStatistics1));
        assertEquals(expectedResults, results);

        // Set graph to return entities only
        graph.setReturnEntitiesOnly();

        // Query for all edges in set {customer|A0, customer|A23}
        retriever = graph.getGraphElementsWithStatisticsWithinSet(seeds, true);
        results.clear();
        for (GraphElementWithStatistics gews : retriever) {
            results.add(gews);
        }
        retriever.close();
        expectedResults.clear();
        GraphElement expectedElement2 = new GraphElement(new Entity("customer", "A0", "purchase", "instore",
                visibilityString1, sevenDaysBefore, sixDaysBefore));
        SetOfStatistics expectedStatistics2 = new SetOfStatistics("count", new Count(10000));
        expectedResults.add(new GraphElementWithStatistics(expectedElement2, expectedStatistics2));
        GraphElement expectedElement3 = new GraphElement(new Entity("customer", "A23", "purchase", "instore",
                visibilityString1, sevenDaysBefore, sixDaysBefore));
        SetOfStatistics expectedStatistics3 = new SetOfStatistics("count", new Count(23));
        expectedResults.add(new GraphElementWithStatistics(expectedElement3, expectedStatistics3));
        assertEquals(expectedResults, results);

        // Set graph to return both entities and edges again, and to only return summary type "X" (which will result
        // in no data).
        graph.setReturnEntitiesAndEdges();
        graph.setSummaryTypes("X");
        retriever = graph.getGraphElementsWithStatisticsWithinSet(seeds, loadIntoMemory);
        results.clear();
        int count = 0;
        for (@SuppressWarnings("unused") GraphElementWithStatistics gews : retriever) {
            count++;
        }
        retriever.close();
        assertEquals(0, count);

        // Set graph to return all summary types again, but set time window to something different.
        graph.setReturnAllSummaryTypesAndSubTypes();
        graph.setTimeWindow(new Date(0L), new Date(100L));
        retriever = graph.getGraphElementsWithStatisticsWithinSet(seeds, loadIntoMemory);
        results.clear();
        count = 0;
        for (@SuppressWarnings("unused") GraphElementWithStatistics gews : retriever) {
            count++;
        }
        retriever.close();
        assertEquals(0, count);
    }

    @Test
    public void testWhenMoreElementsThanFitInBatchScanner() {
        testWhenMoreElementsThanFitInBatchScanner(true);
        testWhenMoreElementsThanFitInBatchScanner(false);
    }

    static void testWhenMoreElementsThanFitInBatchScanner(boolean loadIntoMemory) {
        AccumuloBackedGraph graph = setupGraph();
        graph.setMaxEntriesForBatchScanner(1);

        // Query for all edges in set {customer|A0, customer|A23}
        Set<TypeValue> seeds = new HashSet<TypeValue>();
        seeds.add(new TypeValue("customer", "A0"));
        seeds.add(new TypeValue("customer", "A23"));
        CloseableIterable<GraphElementWithStatistics> retriever = graph.getGraphElementsWithStatisticsWithinSet(seeds, loadIntoMemory);
        Set<GraphElementWithStatistics> results = new HashSet<GraphElementWithStatistics>();
        for (GraphElementWithStatistics gews : retriever) {
            results.add(gews);
        }
        retriever.close();
        Set<GraphElementWithStatistics> expectedResults = new HashSet<GraphElementWithStatistics>();
        GraphElement expectedElement1 = new GraphElement(new Edge("customer", "A0", "customer", "A23", "purchase",
                "instore", true, visibilityString1, sevenDaysBefore, sixDaysBefore));
        SetOfStatistics expectedStatistics1 = new SetOfStatistics("count", new Count(23));
        expectedResults.add(new GraphElementWithStatistics(expectedElement1, expectedStatistics1));
        GraphElement expectedElement2 = new GraphElement(new Entity("customer", "A0", "purchase", "instore",
                visibilityString1, sevenDaysBefore, sixDaysBefore));
        SetOfStatistics expectedStatistics2 = new SetOfStatistics("count", new Count(10000));
        expectedResults.add(new GraphElementWithStatistics(expectedElement2, expectedStatistics2));
        GraphElement expectedElement3 = new GraphElement(new Entity("customer", "A23", "purchase", "instore",
                visibilityString1, sevenDaysBefore, sixDaysBefore));
        SetOfStatistics expectedStatistics3 = new SetOfStatistics("count", new Count(23));
        expectedResults.add(new GraphElementWithStatistics(expectedElement3, expectedStatistics3));
        assertEquals(expectedResults, results);

        // Query for all edges in set {customer|A1} - there shouldn't be any, but we will get the entity for A1
        seeds.clear();
        seeds.add(new TypeValue("customer", "A1"));
        retriever = graph.getGraphElementsWithStatisticsWithinSet(seeds, loadIntoMemory);
        results.clear();
        int count = 0;
        for (GraphElementWithStatistics gews : retriever) {
            count++;
            results.add(gews);
        }
        expectedResults.clear();
        expectedElement1 = new GraphElement(new Entity("customer", "A1", "purchase", "instore", visibilityString1,
                sevenDaysBefore, sixDaysBefore));
        expectedStatistics1 = new SetOfStatistics("count", new Count(1));
        expectedResults.add(new GraphElementWithStatistics(expectedElement1, expectedStatistics1));
        assertEquals(1, count);
        assertEquals(expectedResults, results);

        // Query for all edges in set {customer|A1, customer|A2} - there shouldn't be any edges but will
        // get the two entities
        seeds.clear();
        seeds.add(new TypeValue("customer", "A1"));
        seeds.add(new TypeValue("customer", "A2"));
        retriever = graph.getGraphElementsWithStatisticsWithinSet(seeds, loadIntoMemory);
        results.clear();
        count = 0;
        for (GraphElementWithStatistics gews : retriever) {
            count++;
            results.add(gews);
        }
        expectedElement1 = new GraphElement(new Entity("customer", "A1", "purchase", "instore", visibilityString1,
                sevenDaysBefore, sixDaysBefore));
        expectedStatistics1 = new SetOfStatistics("count", new Count(1));
        expectedResults.add(new GraphElementWithStatistics(expectedElement1, expectedStatistics1));
        expectedElement2 = new GraphElement(new Entity("customer", "A2", "purchase", "instore", visibilityString1,
                sevenDaysBefore, sixDaysBefore));
        expectedStatistics2 = new SetOfStatistics("count", new Count(2));
        expectedResults.add(new GraphElementWithStatistics(expectedElement2, expectedStatistics2));
        assertEquals(2, count);
        assertEquals(expectedResults, results);
    }

    private static AccumuloBackedGraph setupGraph() {
        Instance instance = new MockInstance();
        String tableName = "Test";
        long ageOffTimeInMilliseconds = (30 * 24 * 60 * 60 * 1000L); // 30 days in milliseconds

        try {
            // Open connection
            Connector conn = instance.getConnector("user", "password");

            // Create table
            // (this method creates the table, removes the versioning iterator, and adds the SetOfStatisticsCombiner iterator,
            // and sets the age off iterator to age data off after it is more than ageOffTimeInMilliseconds milliseconds old).
            TableUtils.createTable(conn, tableName, ageOffTimeInMilliseconds);

            // Create set of GraphElementWithStatistics to store data before adding it to the graph.
            Set<GraphElementWithStatistics> data = new HashSet<GraphElementWithStatistics>();

            // Create edges A0 -> A1, A0 -> A2, ..., A0 -> A99. Also create an Entity for each.
            Entity entity = new Entity("customer", "A0", "purchase", "instore", visibilityString1, sevenDaysBefore, sixDaysBefore);
            SetOfStatistics entityStatistics = new SetOfStatistics("count", new Count(10000));
            data.add(new GraphElementWithStatistics(new GraphElement(entity), entityStatistics));
            for (int i = 1; i < 100; i ++) {
                Edge edge = new Edge("customer", "A0", "customer", "A" + i, "purchase", "instore", true, visibilityString1, sevenDaysBefore, sixDaysBefore);
                SetOfStatistics statistics = new SetOfStatistics("count", new Count(i));
                data.add(new GraphElementWithStatistics(new GraphElement(edge), statistics));
                entity = new Entity("customer", "A" + i, "purchase", "instore", visibilityString1, sevenDaysBefore, sixDaysBefore);
                entityStatistics = new SetOfStatistics("count", new Count(i));
                data.add(new GraphElementWithStatistics(new GraphElement(entity), entityStatistics));
            }

            // Create Accumulo backed graph
            AccumuloBackedGraph graph = new AccumuloBackedGraph(conn, tableName);

            // Add data
            graph.addGraphElementsWithStatistics(data);

            // Set graph up for query
            graph.setAuthorizations(new Authorizations(visibilityString1));
            return graph;
        } catch (AccumuloException e) {
            fail("Failed to set up graph in Accumulo with exception: " + e);
        } catch (AccumuloSecurityException e) {
            fail("Failed to set up graph in Accumulo with exception: " + e);
        } catch (TableExistsException e) {
            fail("Failed to set up graph in Accumulo with exception: " + e);
        } catch (TableNotFoundException e) {
            fail("Failed to set up graph in Accumulo with exception: " + e);
        } catch (GraphAccessException e) {
            fail("Failed to set up graph in Accumulo with exception: " + e);
        }
        return null;
    }

    /**
     * Used to sanity check the calculation used in {@link BloomFilterUtilities}'s <code>calculateBloomFilterSize()</code>
     * and <code>calculateNumHashes()</code> methods.
     *
     * @param args  No arguments necessary
     */
    public static void main(String[] args) {
        int[] numItems = new int[]{10, 100, 1000, 10000, 100000};
        for (int num : numItems) {
            System.out.println("Num items to add = " + num);
            // Calculate sensible size of filter, aiming for false positive rate of 1 in 10000, with a maximum size of
            // 1MB.
            int size = (int) (-num * Math.log(0.0001) / (Math.pow(Math.log(2.0), 2.0)));
            size = Math.min(size, 1024 * 1024);
            System.out.println("Size = " + size + " bits (=" + (size / 8) + " bytes)");
            // Work out optimal number of hashes to use in Bloom filter based on size of set - optimal number of hashes is
            // (m/n)ln 2 where m is the size of the filter in bits and n is the number of items that will be added to the set.
            int numHashes = Math.max(1, (int) ((size / num) * Math.log(2)));
            System.out.println("Num hashes = " + numHashes);
            // Create Bloom filter
            BloomFilter filter = new BloomFilter(size, numHashes, Hash.MURMUR_HASH);
            // Add num items to it
            for (int i = 0; i < num; i++) {
                filter.add(new Key(("" + i).getBytes()));
            }
            // Theoretical probability of false positive is (1 - e^(-kn/m)) ^ k (as long as size hasn't been
            // truncated to 1MB).
            System.out.println("Theoretical probability of false positive = " +
                    Math.pow(1.0 - Math.exp(-(double) numHashes * num / (double) size), numHashes));
            // Test false positive rate - should be approx 1 in 10000
            int numPass = 0;
            for (int i = num; i < 1000000; i++) {
                if (filter.membershipTest(new Key(("" + i).getBytes()))) {
                    numPass++;
                }
            }
            System.out.println("Measured probability of false positive " + (numPass / 1000000.0));
        }
    }
}
