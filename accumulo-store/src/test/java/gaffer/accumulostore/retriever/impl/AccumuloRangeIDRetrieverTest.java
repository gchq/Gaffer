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

import gaffer.accumulostore.AccumuloStore;
import gaffer.accumulostore.MockAccumuloStoreForTest;
import gaffer.accumulostore.key.core.impl.byteEntity.ByteEntityKeyPackage;
import gaffer.accumulostore.key.core.impl.classic.ClassicKeyPackage;
import gaffer.accumulostore.key.exception.IteratorSettingException;
import gaffer.accumulostore.operation.AbstractGetRangeFromPair;
import gaffer.accumulostore.operation.impl.GetElementsInRanges;
import gaffer.accumulostore.utils.Pair;
import gaffer.commonutil.TestGroups;
import gaffer.data.element.Edge;
import gaffer.data.element.Element;
import gaffer.data.elementdefinition.view.View;
import gaffer.data.elementdefinition.view.ViewEdgeDefinition;
import gaffer.data.elementdefinition.view.ViewEntityDefinition;
import gaffer.operation.OperationException;
import gaffer.operation.data.ElementSeed;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.impl.add.AddElements;
import gaffer.store.StoreException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class AccumuloRangeIDRetrieverTest {

    private static final int numEntries = 1000;
    private static View defaultView;
    private static AccumuloStore byteEntityStore;
    private static AccumuloStore gaffer1KeyStore;

    @BeforeClass
    public static void setup() throws StoreException, IOException {
        byteEntityStore = new MockAccumuloStoreForTest(ByteEntityKeyPackage.class);
        gaffer1KeyStore = new MockAccumuloStoreForTest(ClassicKeyPackage.class);
        defaultView = new View.Builder().edge(TestGroups.EDGE, new ViewEdgeDefinition()).entity(TestGroups.ENTITY, new ViewEntityDefinition()).build();
        setupGraph(byteEntityStore, numEntries);
        setupGraph(gaffer1KeyStore, numEntries);
    }

    @AfterClass
    public static void tearDown() {
        byteEntityStore = null;
        gaffer1KeyStore = null;
        defaultView = null;
    }

    @Test
    public void test() throws StoreException {
        test(byteEntityStore);
        test(gaffer1KeyStore);
    }

    public void test(final AccumuloStore store) throws StoreException {
        // Create set to query for
        Set<Pair<ElementSeed>> simpleEntityRanges = new HashSet<>();
        simpleEntityRanges.add(new Pair<ElementSeed>(new EntitySeed("0000"), new EntitySeed("0999")));

        // Retrieve elements when less simple entities are provided than the max number of entries for the batch scanner
        AccumuloRangeIDRetriever retriever = null;
        AbstractGetRangeFromPair<ElementSeed, Element> operation = new GetElementsInRanges<>(defaultView, simpleEntityRanges);
        try {
            retriever = new AccumuloRangeIDRetriever(store, operation);
        } catch (IteratorSettingException e) {
            e.printStackTrace();
        }
        int count = 0;
        for (@SuppressWarnings("unused") Element elm : retriever) {
            count++;
        }
        assertEquals(numEntries, count);
    }

    private static void setupGraph(final AccumuloStore store, int numEntries) {
        List<Element> elements = new ArrayList<>();
        for (int i = 0; i < numEntries; i++) {
            Edge edge = new Edge(TestGroups.EDGE);
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