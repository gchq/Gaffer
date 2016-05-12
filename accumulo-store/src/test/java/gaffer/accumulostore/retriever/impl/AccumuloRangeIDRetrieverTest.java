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

import com.google.common.collect.Iterables;
import gaffer.accumulostore.AccumuloStore;
import gaffer.accumulostore.MockAccumuloStoreForTest;
import gaffer.accumulostore.key.core.impl.byteEntity.ByteEntityKeyPackage;
import gaffer.accumulostore.key.core.impl.classic.ClassicKeyPackage;
import gaffer.accumulostore.key.exception.IteratorSettingException;
import gaffer.accumulostore.operation.impl.GetElementsInRanges;
import gaffer.accumulostore.utils.Pair;
import gaffer.commonutil.TestGroups;
import gaffer.data.element.Edge;
import gaffer.data.element.Element;
import gaffer.data.elementdefinition.view.View;
import gaffer.operation.AbstractGetOperation;
import gaffer.operation.OperationException;
import gaffer.operation.data.ElementSeed;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.impl.add.AddElements;
import gaffer.store.StoreException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class AccumuloRangeIDRetrieverTest {

    private final int numEntries = 1000;
    private View defaultView;
    private AccumuloStore byteEntityStore;
    private AccumuloStore gaffer1KeyStore;

    @Before
    public void setup() throws StoreException, IOException {
        byteEntityStore = new MockAccumuloStoreForTest(ByteEntityKeyPackage.class);
        gaffer1KeyStore = new MockAccumuloStoreForTest(ClassicKeyPackage.class);
        defaultView = new View.Builder().edge(TestGroups.EDGE).entity(TestGroups.ENTITY).build();
        setupGraph(byteEntityStore, numEntries);
        setupGraph(gaffer1KeyStore, numEntries);
    }

    @After
    public void tearDown() {
        byteEntityStore = null;
        gaffer1KeyStore = null;
        defaultView = null;
    }

    @Test
    public void shouldRetieveElementsInRangeBetweenSeedsByteEntityStore() throws StoreException {
        shouldRetieveElementsInRangeBetweenSeeds(byteEntityStore);
    }

    @Test
    public void shouldRetieveElementsInRangeBetweenSeedsGaffer1Store() throws StoreException {
        shouldRetieveElementsInRangeBetweenSeeds(gaffer1KeyStore);
    }

    private void shouldRetieveElementsInRangeBetweenSeeds(final AccumuloStore store) throws StoreException {
        // Create set to query for
        final Set<Pair<ElementSeed>> simpleEntityRanges = new HashSet<>();
        simpleEntityRanges.add(new Pair<ElementSeed>(new EntitySeed("0000"), new EntitySeed("0999")));

        // Retrieve elements when less simple entities are provided than the max number of entries for the batch scanner
        final AbstractGetOperation<Pair<ElementSeed>, Element> operation = new GetElementsInRanges<>(defaultView, simpleEntityRanges);
        try {
            final AccumuloRangeIDRetriever retriever = new AccumuloRangeIDRetriever(store, operation);
            assertEquals(numEntries, Iterables.size(retriever));
        } catch (IteratorSettingException e) {
            fail("Unable to construct Range Retriever");
        }
    }

    private void setupGraph(final AccumuloStore store, int numEntries) {
        final List<Element> elements = new ArrayList<>();
        for (int i = 0; i < numEntries; i++) {
            final Edge edge = new Edge(TestGroups.EDGE);
            String s = "" + i;
            while (s.length() < 4) {
                s = "0" + s;
            }
            edge.setSource(s);
            edge.setDestination("B");
            edge.setDirected(false);
            elements.add(edge);
        }
        try {
            store.execute(new AddElements(elements));
        } catch (OperationException e) {
            fail("Couldn't add element: " + e);
        }
    }
}