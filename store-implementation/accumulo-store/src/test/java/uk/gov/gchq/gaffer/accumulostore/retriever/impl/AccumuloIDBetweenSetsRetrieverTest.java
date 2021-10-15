/*
 * Copyright 2016-2021 Crown Copyright
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

import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.SingleUseMiniAccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.operation.impl.GetElementsBetweenSets;
import uk.gov.gchq.gaffer.accumulostore.retriever.AccumuloRetriever;
import uk.gov.gchq.gaffer.accumulostore.utils.AccumuloPropertyNames;
import uk.gov.gchq.gaffer.accumulostore.utils.AccumuloTestData;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters.IncludeIncomingOutgoingType;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

public class AccumuloIDBetweenSetsRetrieverTest {

    private static final AccumuloStore BYTE_ENTITY_STORE = new SingleUseMiniAccumuloStore();
    private static final AccumuloStore GAFFER_1_KEY_STORE = new SingleUseMiniAccumuloStore();
    private static final Schema SCHEMA = Schema.fromJson(StreamUtil.schemas(AccumuloIDBetweenSetsRetrieverTest.class));
    private static final AccumuloProperties PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.storeProps(AccumuloIDBetweenSetsRetrieverTest.class));
    private static final AccumuloProperties CLASSIC_PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.openStream(AccumuloIDBetweenSetsRetrieverTest.class, "/accumuloStoreClassicKeys.properties"));

    private static View defaultView;
    private static View edgeOnlyView;
    private static View entityOnlyView;

    @BeforeAll
    public static void setup() throws StoreException {
        BYTE_ENTITY_STORE.initialise("byteEntityGraph", SCHEMA, PROPERTIES);
        GAFFER_1_KEY_STORE.initialise("gaffer1Graph", SCHEMA, CLASSIC_PROPERTIES);
        defaultView = new View.Builder().edge(TestGroups.EDGE).entity(TestGroups.ENTITY).build();
        edgeOnlyView = new View.Builder().edge(TestGroups.EDGE).build();
        entityOnlyView = new View.Builder().entity(TestGroups.ENTITY).build();
    }

    @BeforeEach
    public void reInitialise() throws StoreException {
        BYTE_ENTITY_STORE.initialise("byteEntityGraph", SCHEMA, PROPERTIES);
        GAFFER_1_KEY_STORE.initialise("gaffer1Graph", SCHEMA, CLASSIC_PROPERTIES);
        setupGraph(BYTE_ENTITY_STORE);
        setupGraph(GAFFER_1_KEY_STORE);
    }

    @Test
    public void shouldGetCorrectEdgesInMemoryFromByteEntityStore() throws StoreException {
        shouldGetCorrectEdges(true, BYTE_ENTITY_STORE);
    }

    @Test
    public void shouldGetCorrectEdgesInMemoryFromGaffer1Store() throws StoreException {
        shouldGetCorrectEdges(true, GAFFER_1_KEY_STORE);
    }

    @Test
    public void shouldGetCorrectEdgesFromByteEntityStore() throws StoreException {
        shouldGetCorrectEdges(false, BYTE_ENTITY_STORE);
    }

    @Test
    public void shouldGetCorrectEdgesFromGaffer1Store() throws StoreException {
        shouldGetCorrectEdges(false, GAFFER_1_KEY_STORE);
    }


    private void shouldGetCorrectEdges(final boolean loadIntoMemory, final AccumuloStore store) throws StoreException {
        // Query for all edges between the set {A0} and the set {A23}
        final GetElementsBetweenSets op = new GetElementsBetweenSets.Builder()
                .input(AccumuloTestData.SEED_A0_SET)
                .inputB(AccumuloTestData.SEED_A23_SET)
                .view(defaultView)
                .build();

        final Set<Element> initialResults = returnElementsFromOperation(store, op, new User(), loadIntoMemory);
        assertThat(initialResults).contains(AccumuloTestData.EDGE_A0_A23, AccumuloTestData.A0_ENTITY);

        // Query for all edges between set {A1} and the set {notpresent} - there shouldn't be any, but
        // we will get the entity for A1
        final GetElementsBetweenSets secondOp = new GetElementsBetweenSets.Builder()
                .input(AccumuloTestData.SEED_A1_SET)
                .inputB(AccumuloTestData.NOT_PRESENT_ENTITY_SEED_SET)
                .view(defaultView)
                .build();

        final Set<Element> secondResults = returnElementsFromOperation(store, secondOp, new User(), loadIntoMemory);
        assertThat(secondResults).hasSize(1)
                .contains(AccumuloTestData.A1_ENTITY);

        // Query for all edges between set {A1} and the set {A2} - there shouldn't be any edges but will
        // get the entity for A1
        final GetElementsBetweenSets thirdOp = new GetElementsBetweenSets.Builder()
                .input(AccumuloTestData.SEED_A1_SET)
                .inputB(AccumuloTestData.SEED_A2_SET)
                .view(defaultView)
                .build();
        final Set<Element> thirdResults = returnElementsFromOperation(store, thirdOp, new User(), loadIntoMemory);
        assertThat(thirdResults).hasSize(1)
                .contains(AccumuloTestData.A1_ENTITY);
    }

    @Test
    public void shouldDealWithOutgoingEdgesOnlyOptionGaffer1KeyStore() {
        shouldDealWithOutgoingEdgesOnlyOption(GAFFER_1_KEY_STORE);
    }

    @Test
    public void shouldDealWithOutgoingEdgesOnlyOptionByteEntityStore() {
        shouldDealWithOutgoingEdgesOnlyOption(BYTE_ENTITY_STORE);
    }

    /**
     * Tests that the options to set outgoing edges or incoming edges only options work correctly.
     *
     * @param store the Store instance
     */
    private void shouldDealWithOutgoingEdgesOnlyOption(final AccumuloStore store) {
        try {
             /*
             Create table
             (this method creates the table, removes the versioning iterator, and adds the SetOfStatisticsCombiner iterator,
             and sets the age off iterator to age data off after it is more than ageOffTimeInMilliseconds milliseconds old).
              */

            final Set<Element> data = new HashSet<>();

            data.add(AccumuloTestData.EDGE_A1_B1);
            data.add(AccumuloTestData.EDGE_B2_A2);
            addElements(data, store, new User());

            // Query for edges between {A1} and {B1}, with outgoing edges only. Should get the edge A1>B1.
            final GetElementsBetweenSets opA1B1 = new GetElementsBetweenSets.Builder().input(AccumuloTestData.SEED_A1_SET).inputB(AccumuloTestData.SEED_B1_SET).view(edgeOnlyView).build();
            opA1B1.setIncludeIncomingOutGoing(IncludeIncomingOutgoingType.OUTGOING);
            final Set<Element> a1B1OutgoingEdgeResults = returnElementsFromOperation(store, opA1B1, new User(), false);
            assertThat(a1B1OutgoingEdgeResults).contains(AccumuloTestData.EDGE_A1_B1);

            // Query for edges between {A1} and {B1}, with incoming edges only. Should get nothing.
            opA1B1.setIncludeIncomingOutGoing(IncludeIncomingOutgoingType.INCOMING);
            final Set<Element> a1B1EdgeIncomingResults = returnElementsFromOperation(store, opA1B1, new User(), false);
            assertThat(a1B1EdgeIncomingResults).isEmpty();

            // Query for edges between {A2} and {B2}, with incoming edges only. Should get the edge B2->A2.
            final GetElementsBetweenSets opA2B2 = new GetElementsBetweenSets.Builder().input(AccumuloTestData.SEED_A2_SET).inputB(AccumuloTestData.SEED_B2_SET).view(edgeOnlyView).build();
            opA2B2.setIncludeIncomingOutGoing(IncludeIncomingOutgoingType.INCOMING);
            final Set<Element> a2B2EdgeIncomingResults = returnElementsFromOperation(store, opA2B2, new User(), false);
            assertThat(a2B2EdgeIncomingResults).contains(AccumuloTestData.EDGE_B2_A2);

            // Query for edges between {A2} and {B2}, with outgoing edges only. Should get nothing.
            opA2B2.setIncludeIncomingOutGoing(IncludeIncomingOutgoingType.OUTGOING);
            final Set<Element> a2B2EdgeOutgoingResults = returnElementsFromOperation(store, opA2B2, new User(), false);
            assertThat(a2B2EdgeOutgoingResults).isEmpty();

        } catch (final StoreException e) {
            fail("Failed to set up graph in Accumulo with exception: " + e);
        }
    }

    /**
     * Tests that the directed edges only and undirected edges only options are respected.
     */
    @Test
    public void shouldDealWithDirectedEdgesOnlyInMemoryByteEntityStore() {
        shouldDealWithDirectedEdgesOnlyOption(true, BYTE_ENTITY_STORE);
    }

    @Test
    public void shouldDealWithDirectedEdgesOnlyInMemoryGaffer1Store() {
        shouldDealWithDirectedEdgesOnlyOption(true, GAFFER_1_KEY_STORE);
    }

    @Test
    public void shouldDealWithDirectedEdgesOnlyByteEntityStore() {
        shouldDealWithDirectedEdgesOnlyOption(false, BYTE_ENTITY_STORE);
    }

    @Test
    public void shouldDealWithDirectedEdgesOnlyGaffer1Store() {
        shouldDealWithDirectedEdgesOnlyOption(false, GAFFER_1_KEY_STORE);
    }

    private void shouldDealWithDirectedEdgesOnlyOption(final boolean loadIntoMemory, final AccumuloStore store) {
        try {

            final Set<Element> data = new HashSet<>();
            data.add(AccumuloTestData.EDGE_A_B_1);
            data.add(AccumuloTestData.EDGE_A_B_2);
            addElements(data, store, new User());

            // Set undirected edges only option, and query for edges between {A} and {B} - should get EDGE_B2_A2
            final GetElementsBetweenSets op = new GetElementsBetweenSets.Builder().input(AccumuloTestData.SEED_A_SET).inputB(AccumuloTestData.SEED_B_SET).view(edgeOnlyView).build();
            op.setDirectedType(DirectedType.UNDIRECTED);

            final Set<Element> results = returnElementsFromOperation(store, op, new User(), loadIntoMemory);
            assertThat(results).contains(AccumuloTestData.EDGE_A_B_2);

            op.setDirectedType(DirectedType.DIRECTED);
            final Set<Element> secondResults = returnElementsFromOperation(store, op, new User(), loadIntoMemory);
            assertThat(secondResults).contains(AccumuloTestData.EDGE_A_B_1);
            // Turn off directed / undirected edges only option and check get both EDGE_A1_B1 and EDGE_B2_A2
            op.setDirectedType(DirectedType.EITHER);

            final Set<Element> thirdResults = returnElementsFromOperation(store, op, new User(), loadIntoMemory);
            assertThat(thirdResults).contains(AccumuloTestData.EDGE_A_B_2);
        } catch (final StoreException e) {
            fail("Failed to set up graph in Accumulo with exception: " + e);
        }
    }

    /**
     * Tests that false positives are filtered out. It does this by explicitly finding a false positive (i.e. something
     * that matches the Bloom filter but that wasn't put into the filter) and adding that to the data, and then
     * checking that isn't returned.
     *
     * @throws uk.gov.gchq.gaffer.store.StoreException if an error is encountered
     */
    @Test
    public void shouldDealWithFalsePositivesInMemoryByteEntityStore() throws StoreException {
        shouldDealWithFalsePositives(true, BYTE_ENTITY_STORE);
    }

    @Test
    public void shouldDealWithFalsePositivesInMemoryGaffer1Store() throws StoreException {
        shouldDealWithFalsePositives(true, GAFFER_1_KEY_STORE);
    }

    @Test
    public void shouldDealWithFalsePositivesByteEntityStore() throws StoreException {
        shouldDealWithFalsePositives(false, BYTE_ENTITY_STORE);
    }

    @Test
    public void shouldDealWithFalsePositivesGaffer1Store() throws StoreException {
        shouldDealWithFalsePositives(false, GAFFER_1_KEY_STORE);
    }

    private void shouldDealWithFalsePositives(final boolean loadIntoMemory, final AccumuloStore store) throws StoreException {
        final Set<EntityId> seeds = new HashSet<>();
        seeds.add(AccumuloTestData.SEED_A0);
        seeds.add(AccumuloTestData.SEED_A23);
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
        int size = (int) (-numItemsToBeAdded * Math.log(0.0001) / Math.pow(Math.log(2.0), 2.0));
        size = Math.min(size, store.getProperties().getMaxBloomFilterToPassToAnIterator());

        // Work out optimal number of hashes to use in Bloom filter based on size of set - optimal number of hashes is
        // (m/n)ln 2 where m is the size of the filter in bits and n is the number of items that will be added to the set.
        final int numHashes = Math.max(1, (int) ((size / numItemsToBeAdded) * Math.log(2)));
        // Create Bloom filter and add seeds to it
        final BloomFilter filter = new BloomFilter(size, numHashes, Hash.MURMUR_HASH);
        for (final EntityId seed : seeds) {
            filter.add(new Key(store.getKeyPackage().getKeyConverter().serialiseVertex(seed.getVertex())));
        }

        // Test random items against it - should only have to shouldRetrieveElementsInRangeBetweenSeeds MAX_SIZE_BLOOM_FILTER / 2 on average before find a
        // false positive (but impose an arbitrary limit to avoid an infinite loop if there's a problem).
        int count = 0;
        int maxNumberOfTries = 50 * store.getProperties().getMaxBloomFilterToPassToAnIterator();
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
        final Edge edge = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source("A0")
                .dest("" + count)
                .directed(true)
                .build();
        edge.putProperty(AccumuloPropertyNames.COUNT, 1000000);
        Set<Element> data = new HashSet<>();
        data.add(edge);
        final User user = new User();
        addElements(data, store, user);
        // Now query for all edges in set - shouldn't get the false positive
        GetElementsBetweenSets op = new GetElementsBetweenSets.Builder().input(AccumuloTestData.SEED_A0_SET).inputB(seeds).view(defaultView).build();
        final Set<Element> results = returnElementsFromOperation(store, op, new User(), loadIntoMemory);
        // Check results are as expected

        assertThat(results).hasSize(2)
                .contains(AccumuloTestData.EDGE_A0_A23, AccumuloTestData.A0_ENTITY);
    }

    /**
     * Tests that standard filtering (e.g. by summary type, or to only receive entities) is still
     * applied.
     *
     * @throws uk.gov.gchq.gaffer.store.StoreException if an error is encountered
     */
    @Test
    public void shouldOtherFilteringStillAppliedByteEntityStoreInMemoryEntities() throws StoreException {
        shouldStillApplyOtherFilter(true, BYTE_ENTITY_STORE);
    }

    @Test
    public void shouldOtherFilteringStillAppliedGaffer1StoreInMemoryEntities() throws StoreException {
        shouldStillApplyOtherFilter(true, GAFFER_1_KEY_STORE);
    }

    @Test
    public void shouldOtherFilteringStillAppliedByteEntityStore() throws StoreException {
        shouldStillApplyOtherFilter(false, BYTE_ENTITY_STORE);
    }

    @Test
    public void shouldOtherFilteringStillAppliedGaffer1Store() throws StoreException {
        shouldStillApplyOtherFilter(false, GAFFER_1_KEY_STORE);
    }

    private void shouldStillApplyOtherFilter(final boolean loadIntoMemory, final AccumuloStore store) throws StoreException {
        // Query for all edges between the set {A0} and the set {A23}
        final GetElementsBetweenSets op = new GetElementsBetweenSets.Builder().input(AccumuloTestData.SEED_A0_SET).inputB(AccumuloTestData.SEED_A23_SET).view(edgeOnlyView).build();
        // Set graph to give us edges only
        op.setDirectedType(DirectedType.EITHER);

        final Set<Element> results = returnElementsFromOperation(store, op, new User(), loadIntoMemory);
        assertThat(results).contains(AccumuloTestData.EDGE_A0_A23);

        // Set graph to return entities only
        final GetElementsBetweenSets secondOp = new GetElementsBetweenSets.Builder().input(AccumuloTestData.SEED_A0_SET).inputB(AccumuloTestData.SEED_A23_SET).view(entityOnlyView).build();

        // Query for all edges in set {A0, A23}, should get the entity for A0
        final Set<Element> secondResults = returnElementsFromOperation(store, secondOp, new User(), loadIntoMemory);

        assertThat(secondResults).contains(AccumuloTestData.A0_ENTITY);

        // Set graph to return both entities and edges again, and to only return summary type "X" (which will result
        // in no data).
        final View view = new View.Builder()
                .edge("edgeX")
                .entity("entityX")
                .build();
        final GetElementsBetweenSets thirdOp = new GetElementsBetweenSets.Builder().input(AccumuloTestData.SEED_A0_SET).inputB(AccumuloTestData.SEED_A23_SET).view(view).build();
        thirdOp.setDirectedType(DirectedType.EITHER);

        final Set<Element> thirdResults = returnElementsFromOperation(store, thirdOp, new User(), loadIntoMemory);
        assertThat(thirdResults).isEmpty();
    }

    @Test
    public void shouldReturnMoreElementsThanFitInBatchScannerByteStoreInMemory() throws StoreException {
        shouldLoadElementsWhenMoreElementsThanFitInBatchScanner(true, BYTE_ENTITY_STORE);
    }

    @Test
    public void shouldReturnMoreElementsThanFitInBatchScannerGaffer1StoreInMemory() throws StoreException {
        shouldLoadElementsWhenMoreElementsThanFitInBatchScanner(true, GAFFER_1_KEY_STORE);
    }

    @Test
    public void shouldReturnMoreElementsThanFitInBatchScannerByteStore() throws StoreException {
        shouldLoadElementsWhenMoreElementsThanFitInBatchScanner(true, BYTE_ENTITY_STORE);
    }

    @Test
    public void shouldReturnMoreElementsThanFitInBatchScannerGaffer1Store() throws StoreException {
        shouldLoadElementsWhenMoreElementsThanFitInBatchScanner(true, GAFFER_1_KEY_STORE);
    }

    private void shouldLoadElementsWhenMoreElementsThanFitInBatchScanner(final boolean loadIntoMemory, final AccumuloStore store) throws StoreException {
        store.getProperties().setMaxEntriesForBatchScanner("1");

        // Query for all edges between the set {A0} and the set {A23}
        final GetElementsBetweenSets op = new GetElementsBetweenSets.Builder().input(AccumuloTestData.SEED_A0_SET).inputB(AccumuloTestData.SEED_A23_SET).view(defaultView).build();
        final Set<Element> betweenA0A23results = returnElementsFromOperation(store, op, new User(), loadIntoMemory);
        assertThat(betweenA0A23results).hasSize(2)
                .contains(AccumuloTestData.EDGE_A0_A23, AccumuloTestData.A0_ENTITY);

        // Query for all edges between set {A1} and the set {notpresent} - there shouldn't be any, but
        // we will get the entity for A1
        final GetElementsBetweenSets secondOp = new GetElementsBetweenSets.Builder().input(AccumuloTestData.SEED_A1_SET).inputB(AccumuloTestData.NOT_PRESENT_ENTITY_SEED_SET).view(defaultView).build();
        final Set<Element> betweenA1andNotPresentResults = returnElementsFromOperation(store, secondOp, new User(), loadIntoMemory);
        assertThat(betweenA1andNotPresentResults).hasSize(1)
                .contains(AccumuloTestData.A1_ENTITY);

        // Query for all edges between set {A1} and the set {A2} - there shouldn't be any edges but will
        // get the entity for A1
        final GetElementsBetweenSets thirdOp = new GetElementsBetweenSets.Builder().input(AccumuloTestData.SEED_A1_SET).inputB(AccumuloTestData.SEED_A2_SET).view(defaultView).build();

        final Set<Element> betweenA1A2Results = returnElementsFromOperation(store, thirdOp, new User(), loadIntoMemory);
        assertThat(betweenA1A2Results).hasSize(1)
                .contains(AccumuloTestData.A1_ENTITY);
    }

    @Test
    public void testEdgesWithinSetAAreNotReturnedByteStoreInMemory() throws StoreException {
        testEdgesWithinSetAAreNotReturned(true, BYTE_ENTITY_STORE);
    }

    @Test
    public void testEdgesWithinSetAAreNotReturnedByteStoreGaffer1StoreInMemory() throws StoreException {
        testEdgesWithinSetAAreNotReturned(true, GAFFER_1_KEY_STORE);
    }

    @Test
    public void testEdgesWithinSetAAreNotReturnedByteStore() throws StoreException {
        testEdgesWithinSetAAreNotReturned(false, BYTE_ENTITY_STORE);
    }

    @Test
    public void testEdgesWithinSetAAreNotReturnedByteStoreGaffer1Store() throws StoreException {
        testEdgesWithinSetAAreNotReturned(false, GAFFER_1_KEY_STORE);
    }

    private void testEdgesWithinSetAAreNotReturned(final boolean loadIntoMemory, final AccumuloStore store) throws StoreException {
        final GetElementsBetweenSets op = new GetElementsBetweenSets.Builder().input(AccumuloTestData.SEED_A0_A23_SET).inputB(AccumuloTestData.SEED_B_SET).view(defaultView).build();
        final Set<Element> betweenA0A23_B_Results = returnElementsFromOperation(store, op, new User(), loadIntoMemory);
        //Should have the two entities A0 A23 but not the edge A0-23
        assertThat(betweenA0A23_B_Results).hasSize(2)
                .contains(AccumuloTestData.A0_ENTITY, AccumuloTestData.A23_ENTITY);
    }

    private Set<Element> returnElementsFromOperation(final AccumuloStore store, final GetElementsBetweenSets operation, final User user, final boolean loadIntoMemory) throws StoreException {

        AccumuloRetriever<?, Element> retriever = null;
        final Set<Element> results = new HashSet<>();
        try {
            retriever = new AccumuloIDBetweenSetsRetriever(store, operation, user, loadIntoMemory,
                    store.getKeyPackage().getIteratorFactory().getEdgeEntityDirectionFilterIteratorSetting(operation));
            for (final Element elm : retriever) {
                results.add(elm);
            }
        } finally {
            if (retriever != null) {
                retriever.close();
            }
        }

        return results;
    }

    private static void setupGraph(final AccumuloStore store) {
        Set<Element> data = new HashSet<>();

        // Create edges A0 -> A1, A0 -> A2, ..., A0 -> A99. Also create an Entity for each.
        final Entity entity = new Entity(TestGroups.ENTITY, "A0");
        entity.putProperty(AccumuloPropertyNames.COUNT, 10000);
        data.add(entity);
        for (int i = 1; i < 100; i++) {
            final Edge edge = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("A0")
                    .dest("A" + i)
                    .directed(true)
                    .build();
            edge.putProperty(AccumuloPropertyNames.COUNT, 23);
            edge.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 1);
            data.add(edge);
            final Entity edgeEntity = new Entity(TestGroups.ENTITY, "A" + i);
            edgeEntity.putProperty(AccumuloPropertyNames.COUNT, i);
            data.add(edgeEntity);
        }

        final User user = new User();
        addElements(data, store, user);
    }


    private static void addElements(final Iterable<Element> data, final AccumuloStore store, final User user) {
        try {
            store.execute(new AddElements.Builder()
                            .input(data)
                            .build(),
                    new Context(user));
        } catch (final OperationException e) {
            fail("Failed to set up graph in Accumulo with exception: " + e);
        }
    }
}
