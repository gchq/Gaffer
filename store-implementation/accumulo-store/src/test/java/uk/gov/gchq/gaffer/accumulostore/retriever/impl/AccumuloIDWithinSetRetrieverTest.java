/*
 * Copyright 2016-2019 Crown Copyright
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

import org.apache.accumulo.core.client.TableExistsException;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;
import org.hamcrest.core.IsCollectionContaining;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.SingleUseMockAccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.operation.impl.GetElementsWithinSet;
import uk.gov.gchq.gaffer.accumulostore.retriever.AccumuloRetriever;
import uk.gov.gchq.gaffer.accumulostore.utils.AccumuloPropertyNames;
import uk.gov.gchq.gaffer.accumulostore.utils.AccumuloTestData;
import uk.gov.gchq.gaffer.accumulostore.utils.TableUtils;
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
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class AccumuloIDWithinSetRetrieverTest {

    private static View defaultView;
    private static AccumuloStore byteEntityStore;
    private static AccumuloStore gaffer1KeyStore;
    private static final Schema SCHEMA = Schema.fromJson(StreamUtil.schemas(AccumuloIDWithinSetRetrieverTest.class));
    private static final AccumuloProperties PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.storeProps(AccumuloIDWithinSetRetrieverTest.class));
    private static final AccumuloProperties CLASSIC_PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.openStream(AccumuloIDWithinSetRetrieverTest.class, "/accumuloStoreClassicKeys.properties"));

    @BeforeClass
    public static void setup() {
        byteEntityStore = new SingleUseMockAccumuloStore();
        gaffer1KeyStore = new SingleUseMockAccumuloStore();
        defaultView = new View.Builder().edge(TestGroups.EDGE).entity(TestGroups.ENTITY).build();
    }

    @Before
    public void reInitialise() throws StoreException {
        byteEntityStore.initialise("byteEntityGraph", SCHEMA, PROPERTIES);
        gaffer1KeyStore.initialise("gaffer1Graph", SCHEMA, CLASSIC_PROPERTIES);
        setupGraph(byteEntityStore);
        setupGraph(gaffer1KeyStore);
    }

    @AfterClass
    public static void tearDown() {
        byteEntityStore = null;
        gaffer1KeyStore = null;
    }


    private Set<Element> returnElementsFromOperation(final AccumuloStore store, final GetElementsWithinSet operation, final User user, final boolean loadIntoMemory) throws StoreException {

        final AccumuloRetriever<?, Element> retriever = new AccumuloIDWithinSetRetriever(store, operation, user, loadIntoMemory);
        final Set<Element> results = new HashSet<>();

        for (final Element elm : retriever) {
            results.add(elm);
        }
        retriever.close();

        return results;
    }

    /**
     * Tests that the correct {@link uk.gov.gchq.gaffer.data.element.Edge}s are returned. Tests that {@link uk.gov.gchq.gaffer.data.element.Entity}s are also returned
     * (unless the return edges only option has been set on the {@link GetElementsWithinSet}). It is desirable
     * for {@link uk.gov.gchq.gaffer.data.element.Entity}s to be returned as a common use-case is to use this method to complete the "half-hop"
     * in a breadth-first search, and then getting all the information about the nodes is often required.
     *
     * @throws StoreException if StoreException
     */
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
        // Query for all edges in set {A0, A23}
        final Set<EntityId> seeds = new HashSet<>();
        seeds.add(AccumuloTestData.SEED_A0);
        seeds.add(AccumuloTestData.SEED_A23);
        final GetElementsWithinSet op = new GetElementsWithinSet.Builder()
                .view(defaultView)
                .input(seeds)
                .build();
        final Set<Element> results = returnElementsFromOperation(store, op, new User(), loadIntoMemory);
        assertThat(results, IsCollectionContaining.hasItems(AccumuloTestData.EDGE_A0_A23, AccumuloTestData.A0_ENTITY, AccumuloTestData.A23_ENTITY));

        // Query for all edges in set {A1} - there shouldn't be any, but we will get the entity for A1
        final GetElementsWithinSet a1Operation = new GetElementsWithinSet.Builder()
                .view(defaultView)
                .input(AccumuloTestData.SEED_A1_SET)
                .build();
        final Set<Element> a1Results = returnElementsFromOperation(store, a1Operation, new User(), loadIntoMemory);
        assertEquals(1, a1Results.size());
        assertThat(a1Results, IsCollectionContaining.hasItem(AccumuloTestData.A1_ENTITY));

        // Query for all edges in set {A1, A2} - there shouldn't be any edges but will
        // get the two entities
        final Set<EntityId> a1A2Seeds = new HashSet<>();
        a1A2Seeds.add(AccumuloTestData.SEED_A1);
        a1A2Seeds.add(AccumuloTestData.SEED_A2);
        final GetElementsWithinSet a1A2Operation = new GetElementsWithinSet.Builder()
                .view(defaultView)
                .input(a1A2Seeds)
                .build();
        final Set<Element> a1A2Results = returnElementsFromOperation(store, a1A2Operation, new User(), loadIntoMemory);
        assertEquals(2, a1A2Results.size());
        assertThat(a1A2Results, IsCollectionContaining.hasItems(AccumuloTestData.A1_ENTITY, AccumuloTestData.A2_ENTITY));
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
     * edge A to B will be returned. But if the outgoing edges only option is turned on then the edge will not be
     * returned, as it is not an edge out of B.
     */
    @Test
    public void shouldDealWithOutgoingEdgesOnlyOptionGaffer1KeyStore() {
        shouldDealWithOutgoingEdgesOnlyOption(gaffer1KeyStore);
    }

    @Test
    public void shouldDealWithOutgoingEdgesOnlyOptionByteEntityStore() {
        shouldDealWithOutgoingEdgesOnlyOption(byteEntityStore);
    }

    private void shouldDealWithOutgoingEdgesOnlyOption(final AccumuloStore store) {
        try {
            // Set outgoing edges only option, and query for the set {C,D}.
            final Set<EntityId> seeds = new HashSet<>();
            seeds.add(new EntitySeed("C"));
            seeds.add(new EntitySeed("D"));
            final Set<Element> expectedResults = new HashSet<>();
            expectedResults.add(AccumuloTestData.EDGE_C_D_DIRECTED);
            expectedResults.add(AccumuloTestData.EDGE_C_D_UNDIRECTED);
            final GetElementsWithinSet op = new GetElementsWithinSet.Builder()
                    .view(defaultView)
                    .input(seeds)
                    .build();
            final Set<Element> results = returnElementsFromOperation(store, op, new User(), true);
            assertEquals(expectedResults, results);
        } catch (final StoreException e) {
            fail("Failed to set up graph in Accumulo with exception: " + e);
        }
    }

    /**
     * Tests that the directed edges only and undirected edges only options are respected.
     *
     * @throws StoreException if StoreException
     */
    @Test
    public void shouldDealWithDirectedEdgesOnlyInMemoryByteEntityStore() throws StoreException {
        shouldDealWithDirectedEdgesOnlyOption(true, byteEntityStore);
    }

    @Test
    public void shouldDealWithDirectedEdgesOnlyInMemoryGaffer1Store() throws StoreException {
        shouldDealWithDirectedEdgesOnlyOption(true, gaffer1KeyStore);
    }

    @Test
    public void shouldDealWithDirectedEdgesOnlyByteEntityStore() throws StoreException {
        shouldDealWithDirectedEdgesOnlyOption(false, byteEntityStore);
    }

    @Test
    public void shouldDealWithDirectedEdgesOnlyGaffer1Store() throws StoreException {
        shouldDealWithDirectedEdgesOnlyOption(false, gaffer1KeyStore);
    }

    private void shouldDealWithDirectedEdgesOnlyOption(final boolean loadIntoMemory, final AccumuloStore store) throws StoreException {
        final Set<EntityId> seeds = new HashSet<>();
        seeds.add(new EntitySeed("C"));
        seeds.add(new EntitySeed("D"));
        final GetElementsWithinSet op = new GetElementsWithinSet.Builder()
                .view(defaultView)
                .input(seeds)
                .build();
        // Set undirected edges only option, and query for edges in set {C, D} - should get the undirected edge
        op.setDirectedType(DirectedType.UNDIRECTED);
        final Set<Element> results = returnElementsFromOperation(store, op, new User(), loadIntoMemory);
        assertThat(results, IsCollectionContaining.hasItem(AccumuloTestData.EDGE_C_D_UNDIRECTED));

        // Set directed edges only option, and query for edges in set {C, D} - should get the directed edge
        final GetElementsWithinSet directedCOop = new GetElementsWithinSet.Builder().view(defaultView)
                .input(seeds)
                .build();
        directedCOop.setDirectedType(DirectedType.DIRECTED);
        final Set<Element> directedCDResults = returnElementsFromOperation(store, directedCOop, new User(), loadIntoMemory);
        assertThat(directedCDResults, IsCollectionContaining.hasItem(AccumuloTestData.EDGE_C_D_DIRECTED));

        final GetElementsWithinSet bothDirectedAndUndirectedOp = new GetElementsWithinSet.Builder().view(defaultView)
                .input(seeds)
                .build();
        // Turn off directed / undirected edges only option and check get both the undirected and directed edge
        bothDirectedAndUndirectedOp.setDirectedType(DirectedType.EITHER);
        final Set<Element> bothDirectedAndUndirectedResults = returnElementsFromOperation(store, bothDirectedAndUndirectedOp, new User(), loadIntoMemory);
        assertThat(bothDirectedAndUndirectedResults, IsCollectionContaining.hasItems(AccumuloTestData.EDGE_C_D_DIRECTED, (Element) AccumuloTestData.EDGE_C_D_UNDIRECTED));
    }

    /**
     * Tests that false positives are filtered out. It does this by explicitly finding a false positive (i.e. something
     * that matches the Bloom filter but that wasn't put into the filter) and adding that to the data, and then
     * checking that isn't returned.
     *
     * @throws StoreException if StoreException
     */
    @Test
    public void shouldDealWithFalsePositivesInMemoryByteEntityStore() throws StoreException {
        shouldDealWithFalsePositives(true, byteEntityStore);
    }

    @Test
    public void shouldDealWithFalsePositivesInMemoryGaffer1Store() throws StoreException {
        shouldDealWithFalsePositives(true, gaffer1KeyStore);
    }

    @Test
    public void shouldDealWithFalsePositivesByteEntityStore() throws StoreException {
        shouldDealWithFalsePositives(false, byteEntityStore);
    }

    @Test
    public void shouldDealWithFalsePositivesGaffer1Store() throws StoreException {
        shouldDealWithFalsePositives(false, gaffer1KeyStore);
    }

    private void shouldDealWithFalsePositives(final boolean loadIntoMemory, final AccumuloStore store) throws StoreException {
        // Query for all edges in set {A0, A23}
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
        final GetElementsWithinSet op = new GetElementsWithinSet.Builder().view(defaultView)
                .input(seeds)
                .build();
        // Now query for all edges in set - shouldn't get the false positive
        final Set<Element> results = returnElementsFromOperation(store, op, new User(), loadIntoMemory);

        // Check results are as expected
        assertThat(results, IsCollectionContaining.hasItems(AccumuloTestData.EDGE_A0_A23, AccumuloTestData.A0_ENTITY, AccumuloTestData.A23_ENTITY));
    }

    /**
     * Tests that standard filtering (e.g. by summary type, or by time window, or to only receive entities) is still
     * applied.
     *
     * @throws StoreException if StoreException
     */
    @Test
    public void shouldStillApplyOtherFilterByteEntityStoreInMemoryEntities() throws StoreException {
        shouldStillApplyOtherFilter(true, byteEntityStore);
    }

    @Test
    public void shouldStillApplyFilterGaffer1StoreInMemoryEntities() throws StoreException {
        shouldStillApplyOtherFilter(true, gaffer1KeyStore);
    }

    @Test
    public void shouldStillApplyOtherFilterByteEntityStore() throws StoreException {
        shouldStillApplyOtherFilter(false, byteEntityStore);
    }

    @Test
    public void shouldStillApplyFilterGaffer1Store() throws StoreException {
        shouldStillApplyOtherFilter(false, gaffer1KeyStore);
    }

    private void shouldStillApplyOtherFilter(final boolean loadIntoMemory, final AccumuloStore store) throws StoreException {
        // Query for all edges in set {A0, A23}
        final Set<EntityId> seeds = new HashSet<>();
        seeds.add(AccumuloTestData.SEED_A0);
        seeds.add(AccumuloTestData.SEED_A23);

        final View edgesOnlyView = new View.Builder().edge(TestGroups.EDGE).build();
        final GetElementsWithinSet op = new GetElementsWithinSet.Builder()
                .view(edgesOnlyView)
                .input(seeds)
                .build();
        // Set graph to give us edges only
        final Set<Element> results = returnElementsFromOperation(store, op, new User(), loadIntoMemory);
        assertThat(results, IsCollectionContaining.hasItem(AccumuloTestData.EDGE_A0_A23));

        // Set graph to return entities only
        final View entitiesOnlyView = new View.Builder().entity(TestGroups.ENTITY).build();
        final GetElementsWithinSet entitiesOnlyOp = new GetElementsWithinSet.Builder()
                .view(entitiesOnlyView)
                .input(seeds)
                .build();
        // Query for all edges in set {A0, A23}
        final Set<Element> entitiesOnlyResults = returnElementsFromOperation(store, entitiesOnlyOp, new User(), loadIntoMemory);
        assertThat(entitiesOnlyResults, IsCollectionContaining.hasItems(AccumuloTestData.A0_ENTITY, AccumuloTestData.A23_ENTITY));

        // Set graph to return both entities and edges again, and to only return summary type "X" (which will result
        // in no data)
        final View view = new View.Builder().edge("X").build();
        final GetElementsWithinSet entitiesAndEdgesOp = new GetElementsWithinSet.Builder()
                .view(view)
                .input(seeds)
                .build();
        final Set<Element> entitiesAndEdgesResults = returnElementsFromOperation(store, entitiesAndEdgesOp, new User(), loadIntoMemory);
        assertEquals(0, entitiesAndEdgesResults.size());
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
        shouldLoadElementsWhenMoreElementsThanFitInBatchScanner(false, byteEntityStore);
    }

    @Test
    public void shouldReturnMoreElementsThanFitInBatchScannerGaffer1Store() throws StoreException {
        shouldLoadElementsWhenMoreElementsThanFitInBatchScanner(false, gaffer1KeyStore);
    }

    private void shouldLoadElementsWhenMoreElementsThanFitInBatchScanner(final boolean loadIntoMemory, final AccumuloStore store) throws StoreException {
        store.getProperties().setMaxEntriesForBatchScanner("1");

        // Query for all edges in set {A0, A23}
        final Set<EntityId> seeds = new HashSet<>();
        seeds.add(AccumuloTestData.SEED_A0);
        seeds.add(AccumuloTestData.SEED_A23);
        final GetElementsWithinSet op = new GetElementsWithinSet.Builder()
                .view(defaultView)
                .input(seeds)
                .build();
        final Set<Element> results = returnElementsFromOperation(store, op, new User(), loadIntoMemory);
        assertThat(results, IsCollectionContaining.hasItems(AccumuloTestData.EDGE_A0_A23, AccumuloTestData.A0_ENTITY, AccumuloTestData.A23_ENTITY));

        // Query for all edges in set {A1} - there shouldn't be any, but we will get the entity for A1
        final GetElementsWithinSet a1Operation = new GetElementsWithinSet.Builder()
                .view(defaultView)
                .input(AccumuloTestData.SEED_A1_SET)
                .build();
        final Set<Element> a1Results = returnElementsFromOperation(store, a1Operation, new User(), loadIntoMemory);
        assertEquals(1, a1Results.size());
        assertThat(a1Results, IsCollectionContaining.hasItem(AccumuloTestData.A1_ENTITY));

        // Query for all edges in set {A1, A2} - there shouldn't be any edges but will
        // get the two entities
        final Set<EntityId> a1A2Seeds = new HashSet<>();
        a1A2Seeds.add(AccumuloTestData.SEED_A1);
        a1A2Seeds.add(AccumuloTestData.SEED_A2);
        final GetElementsWithinSet a1A23Operation = new GetElementsWithinSet.Builder()
                .view(defaultView)
                .input(a1A2Seeds)
                .build();
        final Set<Element> a1A23Results = returnElementsFromOperation(store, a1A23Operation, new User(), loadIntoMemory);
        assertEquals(2, a1A23Results.size());
        assertThat(a1A23Results, IsCollectionContaining.hasItems(AccumuloTestData.A1_ENTITY, AccumuloTestData.A2_ENTITY));
    }

    private static void setupGraph(final AccumuloStore store) {
        try {
            // Create table
            // (this method creates the table, removes the versioning iterator, and adds the SetOfStatisticsCombiner iterator,
            // and sets the age off iterator to age data off after it is more than ageOffTimeInMilliseconds milliseconds old).
            TableUtils.createTable(store);

            final Set<Element> data = new HashSet<>();
            // Create edges A0 -> A1, A0 -> A2, ..., A0 -> A99. Also create an Entity for each.
            final Entity entity = new Entity(TestGroups.ENTITY);
            entity.setVertex("A0");
            entity.putProperty(AccumuloPropertyNames.COUNT, 10000);
            data.add(entity);
            for (int i = 1; i < 100; i++) {
                data.add(new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source("A0")
                        .dest("A" + i)
                        .directed(true)
                        .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 1)
                        .property(AccumuloPropertyNames.COUNT, i)
                        .build());

                data.add(new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex("A" + i)
                        .property(AccumuloPropertyNames.COUNT, i)
                        .build()
                );
            }
            data.add(AccumuloTestData.EDGE_C_D_DIRECTED);
            data.add(AccumuloTestData.EDGE_C_D_UNDIRECTED);
            addElements(data, store, new User());
        } catch (final TableExistsException | StoreException e) {
            fail("Failed to set up graph in Accumulo with exception: " + e);
        }
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
