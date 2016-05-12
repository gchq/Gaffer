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
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import gaffer.accumulostore.retriever.impl.data.AccumuloRetrieverTestData;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.hash.Hash;
import org.hamcrest.core.IsCollectionContaining;
import org.junit.After;
import org.junit.Before;
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
import gaffer.operation.GetOperation.IncludeEdgeType;
import gaffer.operation.GetOperation.IncludeIncomingOutgoingType;
import gaffer.operation.OperationException;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.impl.add.AddElements;
import gaffer.store.StoreException;

public class AccumuloIDBetweenSetsRetrieverTest {

    private View defaultView;
    private AccumuloStore byteEntityStore;
    private AccumuloStore gaffer1KeyStore;

    @Before
    public void setup() throws StoreException, IOException {
        byteEntityStore = new MockAccumuloStoreForTest(ByteEntityKeyPackage.class);
        gaffer1KeyStore = new MockAccumuloStoreForTest(ClassicKeyPackage.class);
        defaultView = new View.Builder().edge(TestGroups.EDGE).entity(TestGroups.ENTITY).build();
        setupGraph(byteEntityStore);
        setupGraph(gaffer1KeyStore);
    }

    @After
    public void tearDown() {
        byteEntityStore = null;
        gaffer1KeyStore = null;
        defaultView = null;
    }

    @Test
    public void shouldGetCorrectEdgesInMemoryFromByteEntityStore() throws StoreException {
        shouldGetCorrectEdges(true, byteEntityStore);
    }

    @Test
    public void shouldGetCorrectEdgesInMemoryFromGaffer1Store() throws StoreException {
        shouldGetCorrectEdges(true, gaffer1KeyStore);
    }

    @Test
    public void shouldGetCorrectEdgesFromByteEntityStore() throws StoreException {
        shouldGetCorrectEdges(false, byteEntityStore);
    }

    @Test
    public void shouldGetCorrectEdgesFromGaffer1Store() throws StoreException {
        shouldGetCorrectEdges(false, gaffer1KeyStore);
    }


    private void shouldGetCorrectEdges(final boolean loadIntoMemory, final AccumuloStore store) throws StoreException {
        // Query for all edges between the set {A0} and the set {A23}
        final AbstractAccumuloTwoSetSeededOperation<EntitySeed, Element> op = new GetElementsBetweenSets<>(AccumuloRetrieverTestData.SEED_A0_SET, AccumuloRetrieverTestData.SEED_A23_SET, defaultView);
        final Set<Element> initalResults = returnElementsFromOperation(store, op, loadIntoMemory);
        assertThat(initalResults, IsCollectionContaining.hasItems(AccumuloRetrieverTestData.EDGE_A0_A23, AccumuloRetrieverTestData.A0_ENTITY));

        // Query for all edges between set {A1} and the set {notpresent} - there shouldn't be any, but
        // we will get the entity for A1
        final AbstractAccumuloTwoSetSeededOperation<EntitySeed, Element>  secondOp = new GetElementsBetweenSets<>(AccumuloRetrieverTestData.SEED_A1_SET, AccumuloRetrieverTestData.NOT_PRESENT_ENTITY_SEED_SET, defaultView);
        final Set<Element> secondResults = returnElementsFromOperation(store, secondOp, loadIntoMemory);
        assertEquals(1, secondResults.size());
        assertThat(secondResults, IsCollectionContaining.hasItem(AccumuloRetrieverTestData.A1_ENTITY));

        // Query for all edges between set {A1} and the set {A2} - there shouldn't be any edges but will
        // get the entity for A1
        final AbstractAccumuloTwoSetSeededOperation<EntitySeed, Element>  thirdOp = new GetElementsBetweenSets<>(AccumuloRetrieverTestData.SEED_A1_SET, AccumuloRetrieverTestData.SEED_A2_SET, defaultView);
        final Set<Element> thirdResults = returnElementsFromOperation(store, thirdOp, loadIntoMemory);
        assertEquals(1, thirdResults.size());
        assertThat(thirdResults, IsCollectionContaining.hasItem(AccumuloRetrieverTestData.A1_ENTITY));
    }

    @Test
    public void shouldDealWithOutgoingEdgesOnlyOptionGaffer1KeyStore() {
        shouldDealWithOutgoingEdgesOnlyOption(gaffer1KeyStore);
    }

    @Test
    public void shouldDealWithOutgoingEdgesOnlyOptionByteEntityStore() {
        shouldDealWithOutgoingEdgesOnlyOption(byteEntityStore);
    }

    /**
     * Tests that the options to set outgoing edges or incoming edges only options work correctly.
     */
    private void shouldDealWithOutgoingEdgesOnlyOption(final AccumuloStore store) {
        try {
             /*
             Create table
             (this method creates the table, removes the versioning iterator, and adds the SetOfStatisticsCombiner iterator,
             and sets the age off iterator to age data off after it is more than ageOffTimeInMilliseconds milliseconds old).
              */

            final Set<Element> data = new HashSet<>();

            data.add(AccumuloRetrieverTestData.EDGE_A1_B1);
            data.add(AccumuloRetrieverTestData.EDGE_B2_A2);
            addElements(data, store);

            // Query for edges between {A1} and {B1}, with outgoing edges only. Should get the edge A1>B1.
            final AbstractAccumuloTwoSetSeededOperation<EntitySeed, Element> opA1B1 = new GetElementsBetweenSets<>(AccumuloRetrieverTestData.SEED_A1_SET, AccumuloRetrieverTestData.SEED_B1_SET, defaultView);
            opA1B1.setIncludeEntities(false);
            opA1B1.setIncludeIncomingOutGoing(IncludeIncomingOutgoingType.OUTGOING);
            final Set<Element> a1B1OutgoingEdgeResults = returnElementsFromOperation(store, opA1B1, false);
            assertThat(a1B1OutgoingEdgeResults, IsCollectionContaining.hasItem(AccumuloRetrieverTestData.EDGE_A1_B1));

            // Query for edges between {A1} and {B1}, with incoming edges only. Should get nothing.
            opA1B1.setIncludeIncomingOutGoing(IncludeIncomingOutgoingType.INCOMING);
            final Set<Element> a1B1EdgeIncomingResults = returnElementsFromOperation(store, opA1B1, false);
            assertEquals(0, a1B1EdgeIncomingResults.size());

            // Query for edges between {A2} and {B2}, with incoming edges only. Should get the edge B2->A2.
            final AbstractAccumuloTwoSetSeededOperation<EntitySeed, Element> opA2B2 = new GetElementsBetweenSets<>(AccumuloRetrieverTestData.SEED_A2_SET, AccumuloRetrieverTestData.SEED_B2_SET, defaultView);
            opA2B2.setIncludeEntities(false);
            opA2B2.setIncludeIncomingOutGoing(IncludeIncomingOutgoingType.INCOMING);
            final Set<Element> a2B2EdgeIncomingResults = returnElementsFromOperation(store, opA2B2, false);
            assertThat(a2B2EdgeIncomingResults, IsCollectionContaining.hasItem(AccumuloRetrieverTestData.EDGE_B2_A2));

            // Query for edges between {A2} and {B2}, with outgoing edges only. Should get nothing.
            opA2B2.setIncludeIncomingOutGoing(IncludeIncomingOutgoingType.OUTGOING);
            final Set<Element> a2B2EdgeOutgoingResults = returnElementsFromOperation(store, opA2B2, false);
            assertEquals(0, a2B2EdgeOutgoingResults.size());

        } catch (StoreException e) {
            fail("Failed to set up graph in Accumulo with exception: " + e);
        }
    }

    /**
     * Tests that the directed edges only and undirected edges only options are respected.
     */
    @Test
    public void shouldDealWithDirectedEdgesOnlyInMemoryByteEntityStore() {
        shouldDealWithDirectedEdgesOnlyOption(true, byteEntityStore);
    }

    @Test
    public void shouldDealWithDirectedEdgesOnlyInMemoryGaffer1Store() {
        shouldDealWithDirectedEdgesOnlyOption(true, gaffer1KeyStore);
    }

    @Test
    public void shouldDealWithDirectedEdgesOnlyByteEntityStore() {
        shouldDealWithDirectedEdgesOnlyOption(false, byteEntityStore);
    }

    @Test
    public void shouldDealWithDirectedEdgesOnlyGaffer1Store() {
        shouldDealWithDirectedEdgesOnlyOption(false, gaffer1KeyStore);
    }

    private void shouldDealWithDirectedEdgesOnlyOption(boolean loadIntoMemory, AccumuloStore store) {
        try {

            final Set<Element> data = new HashSet<>();
            data.add(AccumuloRetrieverTestData.EDGE_A_B_1);
            data.add(AccumuloRetrieverTestData.EDGE_A_B_2);
            addElements(data, store);

            // Set undirected edges only option, and query for edges between {A} and {B} - should get EDGE_B2_A2
            final AbstractAccumuloTwoSetSeededOperation<EntitySeed, Element> op = new GetElementsBetweenSets<>(AccumuloRetrieverTestData.SEED_A_SET, AccumuloRetrieverTestData.SEED_B_SET, defaultView);
            op.setIncludeEdges(IncludeEdgeType.UNDIRECTED);
            op.setIncludeEntities(false);

            final Set<Element> results = returnElementsFromOperation(store, op, false);
            assertThat(results, IsCollectionContaining.hasItem(AccumuloRetrieverTestData.EDGE_A_B_2));

            op.setIncludeEdges(IncludeEdgeType.DIRECTED);
            final Set<Element> secondResults = returnElementsFromOperation(store, op, false);
            assertThat(secondResults, IsCollectionContaining.hasItem(AccumuloRetrieverTestData.EDGE_A_B_1));

            // Turn off directed / undirected edges only option and check get both EDGE_A1_B1 and EDGE_B2_A2
            op.setIncludeEdges(IncludeEdgeType.ALL);

            final Set<Element> thirdResults = returnElementsFromOperation(store, op, false);
            assertThat(thirdResults, IsCollectionContaining.hasItem(AccumuloRetrieverTestData.EDGE_A_B_2));
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
    public void shouldDealWithFalsePositivesInMemoryByteEntityStore() throws StoreException, AccumuloElementConversionException {
        shouldDealWithFalsePositives(true, byteEntityStore);
    }

    @Test
    public void shouldDealWithFalsePositivesInMemoryGaffer1Store() throws StoreException, AccumuloElementConversionException {
        shouldDealWithFalsePositives(true, gaffer1KeyStore);
    }

    @Test
    public void shouldDealWithFalsePositivesByteEntityStore() throws StoreException, AccumuloElementConversionException {
        shouldDealWithFalsePositives(false, byteEntityStore);
    }

    @Test
    public void shouldDealWithFalsePositivesGaffer1Store() throws StoreException, AccumuloElementConversionException {
        shouldDealWithFalsePositives(false, gaffer1KeyStore);
    }

    private void shouldDealWithFalsePositives(final boolean loadIntoMemory, final AccumuloStore store) throws StoreException, AccumuloElementConversionException {
        final Set<EntitySeed> seeds = new HashSet<>();
        seeds.add(AccumuloRetrieverTestData.SEED_A0);
        seeds.add(AccumuloRetrieverTestData.SEED_A23);
        // Add a bunch of items that are not in the data to make the probability of being able to find a false
        // positive sensible.
        for (int i = 0; i < 10; i++) {
            seeds.add(new EntitySeed("abc" + i));
        }

        // Need to make sure that the Bloom filter we create has the same size and the same number of hashes as the
        // one that GraphElementsWithStatisticsWithinSetRetriever creates.
        final int numItemsToBeAdded = loadIntoMemory ? seeds.size() : 20;
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
        final int numHashes = Math.max(1, (int) ((size / numItemsToBeAdded) * Math.log(2)));
        // Create Bloom filter and add seeds to it
        final BloomFilter filter = new BloomFilter(size, numHashes, Hash.MURMUR_HASH);
        for (final EntitySeed seed : seeds) {
            filter.add(new org.apache.hadoop.util.bloom.Key(store.getKeyPackage().getKeyConverter().serialiseVertexForBloomKey(seed.getVertex())));
        }

        // Test random items against it - should only have to shouldRetieveElementsInRangeBetweenSeeds MAX_SIZE_BLOOM_FILTER / 2 on average before find a
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
        final Edge edge = new Edge(TestGroups.EDGE, "A0", "" + count, true);
        edge.putProperty(AccumuloPropertyNames.COUNT, 1000000);
        Set<Element> data = new HashSet<>();
        data.add(edge);
        addElements(data, store);
        // Now query for all edges in set - shouldn't get the false positive
        AbstractAccumuloTwoSetSeededOperation<EntitySeed, Element> op = new GetElementsBetweenSets<>(AccumuloRetrieverTestData.SEED_A0_SET, seeds, defaultView);
        final Set<Element> results = returnElementsFromOperation(store, op, loadIntoMemory);
        // Check results are as expected

        assertThat(results, IsCollectionContaining.hasItems(AccumuloRetrieverTestData.EDGE_A0_A23, AccumuloRetrieverTestData.A0_ENTITY));
    }

    /**
     * Tests that standard filtering (e.g. by summary type, or to only receive entities) is still
     * applied.
     *
     * @throws gaffer.store.StoreException
     */
    @Test
    public void shouldOtherFilteringStillAppliedByteEntityStoreInMemoryEntities() throws StoreException {
        shouldStillApplyOtherFilter(true, byteEntityStore);
    }

    @Test
    public void shouldOtherFilteringStillAppliedGaffer1StoreInMemoryEntities() throws StoreException {
        shouldStillApplyOtherFilter(true, gaffer1KeyStore);
    }

    @Test
    public void shouldOtherFilteringStillAppliedByteEntityStore() throws StoreException {
        shouldStillApplyOtherFilter(false, byteEntityStore);
    }

    @Test
    public void shouldOtherFilteringStillAppliedGaffer1Store() throws StoreException {
        shouldStillApplyOtherFilter(false, gaffer1KeyStore);
    }

    private void shouldStillApplyOtherFilter(final boolean loadIntoMemory, final AccumuloStore store) throws StoreException {
        // Query for all edges between the set {A0} and the set {A23}
        final AbstractAccumuloTwoSetSeededOperation<EntitySeed, Element> op = new GetElementsBetweenSets<>(AccumuloRetrieverTestData.SEED_A0_SET, AccumuloRetrieverTestData.SEED_A23_SET, defaultView);
        // Set graph to give us edges only
        op.setIncludeEdges(IncludeEdgeType.ALL);
        op.setIncludeEntities(false);

        final Set<Element> results = returnElementsFromOperation(store, op, loadIntoMemory);
        assertThat(results, IsCollectionContaining.hasItem(AccumuloRetrieverTestData.EDGE_A0_A23));

        // Set graph to return entities only
        final AbstractAccumuloTwoSetSeededOperation<EntitySeed, Element> secondOp = new GetElementsBetweenSets<>(AccumuloRetrieverTestData.SEED_A0_SET, AccumuloRetrieverTestData.SEED_A23_SET, defaultView);
        secondOp.setIncludeEdges(IncludeEdgeType.NONE);
        secondOp.setIncludeEntities(true);

        // Query for all edges in set {A0, A23}, should get the entity for A0
        final Set<Element> secondResults = returnElementsFromOperation(store, secondOp, loadIntoMemory);

        assertThat(secondResults, IsCollectionContaining.hasItem(AccumuloRetrieverTestData.A0_ENTITY));

        // Set graph to return both entities and edges again, and to only return summary type "X" (which will result
        // in no data).
        final View view = new View.Builder()
                .edge("edgeX")
                .entity("entityX")
                .build();
        final AbstractAccumuloTwoSetSeededOperation<EntitySeed, Element> thirdOp  = new GetElementsBetweenSets<>(AccumuloRetrieverTestData.SEED_A0_SET, AccumuloRetrieverTestData.SEED_A23_SET, view);
        thirdOp.setIncludeEdges(IncludeEdgeType.ALL);
        thirdOp.setIncludeEntities(true);

        final Set<Element> thirdResults = returnElementsFromOperation(store, thirdOp, loadIntoMemory);
        assertEquals(0, thirdResults.size());
    }

    @Test
    public void shouldReturnMoreElementsThanFitInBatchScannerByteStoreInMemory() throws StoreException {
        shouldLoadElementsWhenMoreElementsThanFitInBatchScanner(true, byteEntityStore);
    }

    @Test
    public void shouldReturnMoreElementsThanFitInBatchScannerGaffer1StoreInMemory() throws StoreException {
        shouldLoadElementsWhenMoreElementsThanFitInBatchScanner(true, gaffer1KeyStore);
    }

    @Test
    public void shouldReturnMoreElementsThanFitInBatchScannerByteStore() throws StoreException {
        shouldLoadElementsWhenMoreElementsThanFitInBatchScanner(true, byteEntityStore);
    }

    @Test
    public void shouldReturnMoreElementsThanFitInBatchScannerGaffer1Store() throws StoreException {
        shouldLoadElementsWhenMoreElementsThanFitInBatchScanner(true, gaffer1KeyStore);
    }

    private void shouldLoadElementsWhenMoreElementsThanFitInBatchScanner(final boolean loadIntoMemory, final AccumuloStore store) throws StoreException {
        store.getProperties().setMaxEntriesForBatchScanner("1");

        // Query for all edges between the set {A0} and the set {A23}
        final AbstractAccumuloTwoSetSeededOperation<EntitySeed, Element> op = new GetElementsBetweenSets<>(AccumuloRetrieverTestData.SEED_A0_SET, AccumuloRetrieverTestData.SEED_A23_SET, defaultView);
        final Set<Element> betweenA0A23results = returnElementsFromOperation(store, op, loadIntoMemory);
        assertThat(betweenA0A23results, IsCollectionContaining.hasItems(AccumuloRetrieverTestData.EDGE_A0_A23, AccumuloRetrieverTestData.A0_ENTITY));

        // Query for all edges between set {A1} and the set {notpresent} - there shouldn't be any, but
        // we will get the entity for A1
        final AbstractAccumuloTwoSetSeededOperation<EntitySeed, Element> secondOp = new GetElementsBetweenSets<>(AccumuloRetrieverTestData.SEED_A1_SET, AccumuloRetrieverTestData.NOT_PRESENT_ENTITY_SEED_SET, defaultView);
        final Set<Element> betweenA1andNotPresentResults = returnElementsFromOperation(store, secondOp, loadIntoMemory);
        assertEquals(1, betweenA1andNotPresentResults.size());
        assertThat(betweenA1andNotPresentResults, IsCollectionContaining.hasItem(AccumuloRetrieverTestData.A1_ENTITY));

        // Query for all edges between set {A1} and the set {A2} - there shouldn't be any edges but will
        // get the entity for A1
        final AbstractAccumuloTwoSetSeededOperation<EntitySeed, Element> thirdOp = new GetElementsBetweenSets<>(AccumuloRetrieverTestData.SEED_A1_SET, AccumuloRetrieverTestData.SEED_A2_SET, defaultView);

        final Set<Element> betweenA1A2Results = returnElementsFromOperation(store, thirdOp, loadIntoMemory);
        assertEquals(1, betweenA1A2Results.size());
        assertThat(betweenA1A2Results, IsCollectionContaining.hasItem(AccumuloRetrieverTestData.A1_ENTITY));
    }

    private Set<Element> returnElementsFromOperation(final AccumuloStore store, final AbstractAccumuloTwoSetSeededOperation operation, final boolean loadIntoMemory) throws StoreException {

        final AccumuloRetriever<?> retriever = new AccumuloIDBetweenSetsRetriever(store, operation, loadIntoMemory);
        final Set<Element> results = new HashSet<>();

        for (final Element elm : retriever) {
            results.add(elm);
        }
        retriever.close();

        return results;
    }

    private void setupGraph(final AccumuloStore store) {
        Set<Element> data = new HashSet<>();

        // Create edges A0 -> A1, A0 -> A2, ..., A0 -> A99. Also create an Entity for each.
        final Entity entity = new Entity(TestGroups.ENTITY, "A0");
        entity.putProperty(AccumuloPropertyNames.COUNT, 10000);
        data.add(entity);
        for (int i = 1; i < 100; i++) {
            final Edge edge = new Edge(TestGroups.EDGE, "A0", "A" + i, true);
            edge.putProperty(AccumuloPropertyNames.COUNT, 1);
            edge.putProperty(AccumuloPropertyNames.TIMESTAMP, AccumuloRetrieverTestData.TIMESTAMP);
            data.add(edge);
            final Entity edgeEntity = new Entity(TestGroups.ENTITY, "A" + i);
            edgeEntity.putProperty(AccumuloPropertyNames.COUNT, i);
            edgeEntity.putProperty(AccumuloPropertyNames.TIMESTAMP, AccumuloRetrieverTestData.TIMESTAMP);
            data.add(edgeEntity);
        }
        addElements(data, store);
    }


    private void addElements(final Iterable<Element> data, final AccumuloStore store) {
        try {
            store.execute(new AddElements(data));
        } catch (OperationException e) {
            fail("Failed to set up graph in Accumulo with exception: " + e);
        }
    }
}