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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.hash.Hash;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import gaffer.accumulostore.AccumuloStore;
import gaffer.accumulostore.MockAccumuloStoreForTest;
import gaffer.accumulostore.key.core.impl.byteEntity.ByteEntityKeyPackage;
import gaffer.accumulostore.key.core.impl.classic.ClassicKeyPackage;
import gaffer.accumulostore.key.exception.AccumuloElementConversionException;
import gaffer.accumulostore.operation.AbstractAccumuloTwoSetSeededOperation;
import gaffer.accumulostore.operation.impl.GetElementsBetweenSets;
import gaffer.accumulostore.retriever.AccumuloRetriever;
import gaffer.accumulostore.utils.AccumuloPropertyNames;
import gaffer.commonutil.TestGroups;
import gaffer.data.element.Edge;
import gaffer.data.element.Element;
import gaffer.data.element.Entity;
import gaffer.data.elementdefinition.view.View;
import gaffer.data.elementdefinition.view.ViewEdgeDefinition;
import gaffer.data.elementdefinition.view.ViewEntityDefinition;
import gaffer.operation.GetOperation.IncludeEdgeType;
import gaffer.operation.GetOperation.IncludeIncomingOutgoingType;
import gaffer.operation.OperationException;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.impl.add.AddElements;
import gaffer.store.StoreException;

public class AccumuloIDBetweenSetsRetrieverTest {

    private static final long TIMESTAMP = System.currentTimeMillis();
    private static View defaultView;
    private static AccumuloStore byteEntityStore;
    private static AccumuloStore gaffer1KeyStore;

    @BeforeClass
    public static void setup() throws StoreException, IOException {
        byteEntityStore = new MockAccumuloStoreForTest(ByteEntityKeyPackage.class);
        gaffer1KeyStore = new MockAccumuloStoreForTest(ClassicKeyPackage.class);
        defaultView = new View.Builder().edge(TestGroups.EDGE, new ViewEdgeDefinition()).entity(TestGroups.ENTITY, new ViewEntityDefinition()).build();
        setupGraph(byteEntityStore);
        setupGraph(gaffer1KeyStore);
    }

    @AfterClass
    public static void tearDown() {
        byteEntityStore = null;
        gaffer1KeyStore = null;
        defaultView = null;
    }

    @Test
    public void testGetCorrectEdges() throws StoreException {
        testGetCorrectEdges(byteEntityStore);
        testGetCorrectEdges(gaffer1KeyStore);
    }

    public void testGetCorrectEdges(final AccumuloStore store) throws StoreException {
        testGetCorrectEdges(true, store);
        testGetCorrectEdges(false, store);
    }

    static void testGetCorrectEdges(final boolean loadIntoMemory, final AccumuloStore store) throws StoreException {
        // Query for all edges between the set {A0} and the set {A23}
        Set<EntitySeed> seedsA = new HashSet<>();
        seedsA.add(new EntitySeed("A0"));
        Set<EntitySeed> seedsB = new HashSet<>();
        seedsB.add(new EntitySeed("A23"));
        AbstractAccumuloTwoSetSeededOperation<EntitySeed, Element> op = new GetElementsBetweenSets<>(seedsA, seedsB, defaultView);
        AccumuloRetriever<?> retriever = new AccumuloIDBetweenSetsRetriever(store, op, loadIntoMemory);
        Set<Element> results = new HashSet<>();
        for (Element elm : retriever) {
            results.add(elm);
        }
        retriever.close();
        Set<Element> expectedResults = new HashSet<>();
        Element expectedElement1 = new Edge(TestGroups.EDGE, "A0", "A23", true);
        expectedElement1.putProperty(AccumuloPropertyNames.COUNT, 23);
        expectedResults.add(expectedElement1);
        Element expectedElement2 = new Entity(TestGroups.ENTITY, "A0");
        expectedElement2.putProperty(AccumuloPropertyNames.COUNT, 10000);
        expectedResults.add(expectedElement2);
        assertEquals(expectedResults, results);

        // Query for all edges between set {A1} and the set {notpresent} - there shouldn't be any, but
        // we will get the entity for A1
        seedsA.clear();
        seedsA.add(new EntitySeed("A1"));
        seedsB.clear();
        seedsB.add(new EntitySeed("notpresent"));
        op = new GetElementsBetweenSets<>(seedsA, seedsB, defaultView);
        retriever = new AccumuloIDBetweenSetsRetriever(store, op, loadIntoMemory);
        results.clear();
        int count = 0;
        for (Element elm : retriever) {
            count++;
            results.add(elm);
        }
        retriever.close();
        expectedResults.clear();
        expectedElement1 = new Entity(TestGroups.ENTITY, "A1");
        expectedElement1.putProperty(AccumuloPropertyNames.COUNT, 1);
        expectedResults.add(expectedElement1);
        assertEquals(1, count);
        assertEquals(expectedResults, results);

        // Query for all edges between set {A1} and the set {A2} - there shouldn't be any edges but will
        // get the entity for A1
        seedsA.clear();
        seedsA.add(new EntitySeed("A1"));
        seedsB.clear();
        seedsB.add(new EntitySeed("A2"));
        op = new GetElementsBetweenSets<>(seedsA, seedsB, defaultView);
        retriever = new AccumuloIDBetweenSetsRetriever(store, op, loadIntoMemory);
        results.clear();
        count = 0;
        for (Element element : retriever) {
            count++;
            results.add(element);
        }
        retriever.close();
        expectedResults.clear();
        expectedElement1 = new Entity(TestGroups.ENTITY, "A1");
        expectedElement1.putProperty(AccumuloPropertyNames.COUNT, 1);
        expectedResults.add(expectedElement1);
        assertEquals(1, count);
        assertEquals(expectedResults, results);
    }

    @Test
    public void testDealWithOutgoingEdgesOnlyOption() {
        testDealWithOutgoingEdgesOnlyOption(byteEntityStore);
        testDealWithOutgoingEdgesOnlyOption(gaffer1KeyStore);
    }

    /**
     * Tests that the options to set outgoing edges or incoming edges only options work correctly.
     */
    public void testDealWithOutgoingEdgesOnlyOption(final AccumuloStore store) {
        try {
            // Create table
            // (this method creates the table, removes the versioning iterator, and adds the SetOfStatisticsCombiner iterator,
            // and sets the age off iterator to age data off after it is more than ageOffTimeInMilliseconds milliseconds old).

            Set<Element> data = new HashSet<>();

            // Create edge A -> B
            Edge edge1 = new Edge(TestGroups.EDGE, "A1", "B1", true);
            Edge edge2 = new Edge(TestGroups.EDGE, "B2", "A2", true);
            edge1.putProperty(AccumuloPropertyNames.COUNT, 1);
            edge1.putProperty(AccumuloPropertyNames.TIMESTAMP, TIMESTAMP);
            edge2.putProperty(AccumuloPropertyNames.COUNT, 100);
            data.add(edge1);
            data.add(edge2);
            addElements(data, store);
            List<EntitySeed> seedsA = new ArrayList<>();
            seedsA.add(new EntitySeed("A1"));
            List<EntitySeed> seedsB = new ArrayList<>();
            seedsB.add(new EntitySeed("B1"));

            // Query for edges between {A1} and {B1}, with outgoing edges only. Should get the edge A1>B1.
            Set<Element> expectedResults = new HashSet<>();
            AbstractAccumuloTwoSetSeededOperation<EntitySeed, Element> op = new GetElementsBetweenSets<>(seedsA, seedsB, defaultView);
            op.setIncludeEntities(false);
            op.setIncludeIncomingOutGoing(IncludeIncomingOutgoingType.OUTGOING);
            AccumuloIDBetweenSetsRetriever retriever = new AccumuloIDBetweenSetsRetriever(store, op, false);
            Set<Element> results = new HashSet<>();
            for (Element elm : retriever) {
                results.add(elm);
            }
            retriever.close();
            expectedResults.add(edge1);
            assertEquals(expectedResults, results);

            // Query for edges between {A1} and {B1}, with incoming edges only. Should get nothing.
            op.setIncludeIncomingOutGoing(IncludeIncomingOutgoingType.INCOMING);
            retriever = new AccumuloIDBetweenSetsRetriever(store, op, false);
            results.clear();
            int count = 0;
            for (@SuppressWarnings("unused") Element elm : retriever) {
                count++;
            }
            retriever.close();
            assertEquals(0, count);

            // Query for edges between {A2} and {B2}, with incoming edges only. Should get the edge B2->A2.
            seedsA.clear();
            seedsA.add(new EntitySeed("A2"));
            seedsB.clear();
            seedsB.add(new EntitySeed("B2"));
            op.setIncludeIncomingOutGoing(IncludeIncomingOutgoingType.INCOMING);
            retriever = new AccumuloIDBetweenSetsRetriever(store, op, false);
            results.clear();
            for (Element element : retriever) {
                results.add(element);
            }
            retriever.close();
            expectedResults.clear();
            expectedResults.add(edge2);
            assertEquals(expectedResults, results);
            retriever.close();

            // Query for edges between {A2} and {B2}, with outgoing edges only. Should get the edge B2->A2.
            op.setIncludeIncomingOutGoing(IncludeIncomingOutgoingType.OUTGOING);
            retriever = new AccumuloIDBetweenSetsRetriever(store, op, false);
            for (@SuppressWarnings("unused") Element elm : retriever) {
                count++;
            }
            retriever.close();
            assertEquals(0, count);
        } catch (StoreException e) {
            fail("Failed to set up graph in Accumulo with exception: " + e);
        }
    }

    /**
     * Tests that the directed edges only and undirected edges only options are respected.
     */
    @Test
    public void testDealWithDirectedEdgesOnlyOption() {
        testDealWithDirectedEdgesOnlyOption(byteEntityStore);
        testDealWithDirectedEdgesOnlyOption(gaffer1KeyStore);
    }

    public void testDealWithDirectedEdgesOnlyOption(final AccumuloStore store) {
        testDealWithDirectedEdgesOnlyOption(true, store);
        testDealWithDirectedEdgesOnlyOption(false, store);
    }

    static void testDealWithDirectedEdgesOnlyOption(boolean loadIntoMemory, AccumuloStore store) {
        try {

            Set<Element> data = new HashSet<>();

            // Create directed edge A -> B and undirected edge A - B
            Edge edge1 = new Edge(TestGroups.EDGE, "A", "B", true);
            Edge edge2 = new Edge(TestGroups.EDGE, "A", "B", false);
            edge1.putProperty(AccumuloPropertyNames.COUNT, 1);
            edge2.putProperty(AccumuloPropertyNames.COUNT, 2);
            data.add(edge1);
            data.add(edge2);
            addElements(data, store);

            // Set undirected edges only option, and query for edges between {A} and {B} - should get edge2
            Set<EntitySeed> seedsA = new HashSet<>();
            seedsA.add(new EntitySeed("A"));
            Set<EntitySeed> seedsB = new HashSet<>();
            seedsB.add(new EntitySeed("B"));
            AbstractAccumuloTwoSetSeededOperation<EntitySeed, Element> op = new GetElementsBetweenSets<>(seedsA, seedsB, defaultView);
            op.setIncludeEdges(IncludeEdgeType.UNDIRECTED);
            op.setIncludeEntities(false);
            AccumuloIDBetweenSetsRetriever retriever = new AccumuloIDBetweenSetsRetriever(store, op, false);
            Set<Element> results = new HashSet<>();
            for (Element elm : retriever) {
                results.add(elm);
            }
            retriever.close();
            Set<Element> expectedResults = new HashSet<>();
            expectedResults.add(edge2);
            assertEquals(expectedResults, results);
            op.setIncludeEdges(IncludeEdgeType.DIRECTED);
            //Set directed edges only option, and query for edges between {A} and {B} - should get edge1
            retriever = new AccumuloIDBetweenSetsRetriever(store, op, false);
            results.clear();
            for (Element elm : retriever) {
                results.add(elm);
            }
            retriever.close();
            expectedResults.clear();
            expectedResults.add(edge1);
            assertEquals(expectedResults, results);

            // Turn off directed / undirected edges only option and check get both edge1 and edge2
            op.setIncludeEdges(IncludeEdgeType.ALL);
            retriever = new AccumuloIDBetweenSetsRetriever(store, op, false);
            results.clear();
            for (Element elm : retriever) {
                results.add(elm);
            }
            retriever.close();
            expectedResults.add(edge2);
            assertEquals(expectedResults, results);
        } catch (StoreException e) {
            fail("Failed to set up graph in Accumulo with exception: " + e);
        }
    }

    /**
     * Tests that false positives are filtered out. It does this by explicitly finding a false positive (i.e. something
     * that matches the Bloom filter but that wasn't put into the filter) and adding that to the data, and then
     * checking that isn't returned.
     *
     * @throws gaffer.store.StoreException
     * @throws AccumuloElementConversionException 
     */
    @Test
    public void testDealWithFalsePositives() throws StoreException, AccumuloElementConversionException {
        testDealWithFalsePositives(byteEntityStore);
        testDealWithFalsePositives(gaffer1KeyStore);
    }

    public void testDealWithFalsePositives(final AccumuloStore store) throws StoreException, AccumuloElementConversionException {
        testDealWithFalsePositives(true, store);
        testDealWithFalsePositives(false, store);
    }

    static void testDealWithFalsePositives(final boolean loadIntoMemory, final AccumuloStore store) throws StoreException, AccumuloElementConversionException {
        Set<EntitySeed> seeds = new HashSet<>();
        seeds.add(new EntitySeed("A0"));
        seeds.add(new EntitySeed("A23"));
        // Add a bunch of items that are not in the data to make the probability of being able to find a false
        // positive sensible.
        for (int i = 0; i < 10; i++) {
            seeds.add(new EntitySeed("abc" + i));
        }

        // Need to make sure that the Bloom filter we create has the same size and the same number of hashes as the
        // one that GraphElementsWithStatisticsWithinSetRetriever creates.
        int numItemsToBeAdded = loadIntoMemory ? seeds.size() : 20;
        if (!loadIntoMemory) {
            store.getProperties().setMaxEntriesForBatchScanner("20");
        }

        // Find something that will give a false positive
        // Need to repeat the logic used in the getGraphElementsWithStatisticsWithinSet() method.
        // Calculate sensible size of filter, aiming for false positive rate of 1 in 10000, with a maximum size of
        // maxBloomFilterToPassToAnIterator bytes.
        int size = (int) (-numItemsToBeAdded * Math.log(0.0001) / (Math.pow(Math.log(2.0), 2.0)));
        size = Math.min(size, store.getProperties().getMaxBloomFilterToPassToAnIterator());

        // Work out optimal number of hashes to use in Bloom filter based on size of set - optimal number of hashes is
        // (m/n)ln 2 where m is the size of the filter in bits and n is the number of items that will be added to the set.
        int numHashes = Math.max(1, (int) ((size / numItemsToBeAdded) * Math.log(2)));
        // Create Bloom filter and add seeds to it
        BloomFilter filter = new BloomFilter(size, numHashes, Hash.MURMUR_HASH);
        for (EntitySeed seed : seeds) {
            filter.add(new org.apache.hadoop.util.bloom.Key(store.getKeyPackage().getKeyConverter().serialiseVertexForBloomKey(seed.getVertex())));
        }

        // Test random items against it - should only have to test MAX_SIZE_BLOOM_FILTER / 2 on average before find a
        // false positive (but impose an arbitrary limit to avoid an infinite loop if there's a problem).
        int count = 0;
        int maxNumberOfTries = 50 * store.getProperties().getMaxBloomFilterToPassToAnIterator();
        while (count < maxNumberOfTries) {
            count++;
            if (filter.membershipTest(new org.apache.hadoop.util.bloom.Key(("" + count).getBytes()))) {
                break;
            }
        }
        if (count == maxNumberOfTries) {
            fail("Didn't find a false positive");
        }

        // False positive is "" + count so create an edge from seeds to that
        Edge edge = new Edge(TestGroups.EDGE, "A0", "" + count, true);
        edge.putProperty(AccumuloPropertyNames.COUNT, 1000000);
        Set<Element> data = new HashSet<>();
        data.add(edge);
        addElements(data, store);
        // Now query for all edges in set - shouldn't get the false positive
        List<EntitySeed> seed = Collections.singletonList(new EntitySeed("A0"));
        AbstractAccumuloTwoSetSeededOperation<EntitySeed, Element> op = new GetElementsBetweenSets<>(seed, seeds, defaultView);
        AccumuloIDBetweenSetsRetriever retriever = new AccumuloIDBetweenSetsRetriever(store, op, loadIntoMemory);
        Set<Element> results = new HashSet<>();
        for (Element elm : retriever) {
            results.add(elm);
        }
        retriever.close();
        // Check results are as expected
        Set<Element> expectedResults = new HashSet<>();
        Element expectedElement1 = new Edge(TestGroups.EDGE, "A0", "A23", true);
        expectedElement1.putProperty(AccumuloPropertyNames.COUNT, 23);
        expectedResults.add(expectedElement1);
        Element expectedElement2 = new Entity(TestGroups.ENTITY, "A0");
        expectedElement2.putProperty(AccumuloPropertyNames.COUNT, 10000);
        expectedResults.add(expectedElement2);
        assertEquals(expectedResults, results);
    }

    /**
     * Tests that standard filtering (e.g. by summary type, or to only receive entities) is still
     * applied.
     *
     * @throws gaffer.store.StoreException
     */
    @Test
    public void testOtherFilteringStillApplied() throws StoreException {
        testOtherFilteringStillApplied(byteEntityStore);
        testOtherFilteringStillApplied(gaffer1KeyStore);
    }


    public void testOtherFilteringStillApplied(final AccumuloStore store) throws StoreException {
        testOtherFilteringStillApplied(true, store);
        testOtherFilteringStillApplied(false, store);
    }

    static void testOtherFilteringStillApplied(final boolean loadIntoMemory, final AccumuloStore store) throws StoreException {
        // Query for all edges between the set {A0} and the set {A23}
        Set<EntitySeed> seedsA = new HashSet<>();
        seedsA.add(new EntitySeed("A0"));
        Set<EntitySeed> seedsB = new HashSet<>();
        seedsB.add(new EntitySeed("A23"));
        AbstractAccumuloTwoSetSeededOperation<EntitySeed, Element> op = new GetElementsBetweenSets<>(seedsA, seedsB, defaultView);
        // Set graph to give us edges only
        op.setIncludeEdges(IncludeEdgeType.ALL);
        op.setIncludeEntities(false);
        AccumuloIDBetweenSetsRetriever retriever = new AccumuloIDBetweenSetsRetriever(store, op, loadIntoMemory);
        Set<Element> results = new HashSet<>();
        for (Element elm : retriever) {
            results.add(elm);
        }
        retriever.close();
        Set<Element> expectedResults = new HashSet<>();
        Element expectedElement1 = new Edge(TestGroups.EDGE, "A0", "A23", true);
        expectedElement1.putProperty(AccumuloPropertyNames.COUNT, 23);
        expectedResults.add(expectedElement1);
        assertEquals(expectedResults, results);

        // Set graph to return entities only
        op = new GetElementsBetweenSets<>(seedsA, seedsB, defaultView);
        op.setIncludeEdges(IncludeEdgeType.NONE);
        op.setIncludeEntities(true);

        // Query for all edges in set {A0, A23}, should get the entity for A0
        retriever = new AccumuloIDBetweenSetsRetriever(store, op, loadIntoMemory);
        results.clear();
        for (Element elm : retriever) {
            results.add(elm);
        }
        retriever.close();
        expectedResults.clear();
        Element expectedElement2 = new Entity(TestGroups.ENTITY, "A0");
        expectedElement2.putProperty(AccumuloPropertyNames.COUNT, 10000);
        expectedResults.add(expectedElement2);
        assertEquals(expectedResults, results);

        // Set graph to return both entities and edges again, and to only return summary type "X" (which will result
        // in no data).
        View view = new View.Builder()
                .edge("X", new ViewEdgeDefinition())
                .entity("X", new ViewEntityDefinition()).build();
        op = new GetElementsBetweenSets<>(seedsA, seedsB, view);
        op.setIncludeEdges(IncludeEdgeType.ALL);
        op.setIncludeEntities(true);

        retriever = new AccumuloIDBetweenSetsRetriever(store, op, loadIntoMemory);
        results.clear();
        int count = 0;
        for (@SuppressWarnings("unused") Element elm : retriever) {
            count++;
        }
        retriever.close();
        assertEquals(0, count);
    }

    @Test
    public void testWhenMoreElementsThanFitInBatchScanner() throws StoreException {
        testWhenMoreElementsThanFitInBatchScanner(byteEntityStore);
        testWhenMoreElementsThanFitInBatchScanner(gaffer1KeyStore);
    }


    public void testWhenMoreElementsThanFitInBatchScanner(final AccumuloStore store) throws StoreException {
        testWhenMoreElementsThanFitInBatchScanner(true, store);
        testWhenMoreElementsThanFitInBatchScanner(false, store);
    }

    static void testWhenMoreElementsThanFitInBatchScanner(final boolean loadIntoMemory, final AccumuloStore store) throws StoreException {
        store.getProperties().setMaxEntriesForBatchScanner("1");

        // Query for all edges between the set {A0} and the set {A23}
        Set<EntitySeed> seedsA = new HashSet<>();
        seedsA.add(new EntitySeed("A0"));
        Set<EntitySeed> seedsB = new HashSet<>();
        seedsB.add(new EntitySeed("A23"));
        AbstractAccumuloTwoSetSeededOperation<EntitySeed, Element> op = new GetElementsBetweenSets<>(seedsA, seedsB, defaultView);
        AccumuloIDBetweenSetsRetriever retriever = new AccumuloIDBetweenSetsRetriever(store, op, loadIntoMemory);
        Set<Element> results = new HashSet<>();
        for (Element elm : retriever) {
            results.add(elm);
        }
        retriever.close();
        Set<Element> expectedResults = new HashSet<>();
        Element expectedElement1 = new Edge(TestGroups.EDGE, "A0", "A23", true);
        expectedElement1.putProperty(AccumuloPropertyNames.COUNT, 23);
        expectedResults.add(expectedElement1);
        Element expectedElement2 = new Entity(TestGroups.ENTITY, "A0");
        expectedElement2.putProperty(AccumuloPropertyNames.COUNT, 10000);
        expectedResults.add(expectedElement2);
        assertEquals(expectedResults, results);

        // Query for all edges between set {A1} and the set {notpresent} - there shouldn't be any, but
        // we will get the entity for A1
        seedsA.clear();
        seedsA.add(new EntitySeed("A1"));
        seedsB.clear();
        seedsB.add(new EntitySeed("notpresent"));
        op = new GetElementsBetweenSets<>(seedsA, seedsB, defaultView);
        retriever = new AccumuloIDBetweenSetsRetriever(store, op, loadIntoMemory);
        results.clear();
        int count = 0;
        for (Element elm : retriever) {
            count++;
            results.add(elm);
        }
        retriever.close();
        expectedResults.clear();
        expectedElement1 = new Entity(TestGroups.ENTITY, "A1");
        expectedElement1.putProperty(AccumuloPropertyNames.COUNT, 1);
        expectedResults.add(expectedElement1);
        assertEquals(1, count);
        assertEquals(expectedResults, results);

        // Query for all edges between set {A1} and the set {A2} - there shouldn't be any edges but will
        // get the entity for A1
        seedsA.clear();
        seedsA.add(new EntitySeed("A1"));
        seedsB.clear();
        seedsB.add(new EntitySeed("A2"));
        op = new GetElementsBetweenSets<>(seedsA, seedsB, defaultView);
        retriever = new AccumuloIDBetweenSetsRetriever(store, op, loadIntoMemory);
        results.clear();
        count = 0;
        for (Element elm : retriever) {
            count++;
            results.add(elm);
        }
        retriever.close();
        expectedElement1 = new Entity(TestGroups.ENTITY, "A1");
        expectedElement1.putProperty(AccumuloPropertyNames.COUNT, 1);
        expectedResults.add(expectedElement1);
        assertEquals(1, count);
        assertEquals(expectedResults, results);
    }

    private static void setupGraph(final AccumuloStore store) {
        Set<Element> data = new HashSet<>();

        // Create edges A0 -> A1, A0 -> A2, ..., A0 -> A99. Also create an Entity for each.
        Entity entity = new Entity(TestGroups.ENTITY, "A0");
        entity.putProperty(AccumuloPropertyNames.COUNT, 10000);
        data.add(entity);
        for (int i = 1; i < 100; i++) {
            Edge edge = new Edge(TestGroups.EDGE, "A0", "A" + i, true);
            edge.putProperty(AccumuloPropertyNames.COUNT, 1);
            edge.putProperty(AccumuloPropertyNames.TIMESTAMP, TIMESTAMP);
            data.add(edge);
            entity = new Entity(TestGroups.ENTITY, "A" + i);
            entity.putProperty(AccumuloPropertyNames.COUNT, i);
            entity.putProperty(AccumuloPropertyNames.TIMESTAMP, TIMESTAMP);
            data.add(entity);
        }
        addElements(data, store);
    }


    private static void addElements(final Iterable<Element> data, final AccumuloStore store) {
        try {
            store.execute(new AddElements(data));
        } catch (OperationException e) {
            fail("Failed to set up graph in Accumulo with exception: " + e);
        }
    }
}