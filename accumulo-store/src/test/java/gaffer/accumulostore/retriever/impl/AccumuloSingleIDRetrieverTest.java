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

import gaffer.accumulostore.AccumuloStore;
import gaffer.accumulostore.MockAccumuloStoreForTest;
import gaffer.accumulostore.key.core.impl.byteEntity.ByteEntityKeyPackage;
import gaffer.accumulostore.key.core.impl.classic.ClassicKeyPackage;
import gaffer.accumulostore.key.exception.IteratorSettingException;
import gaffer.accumulostore.utils.Constants;
import gaffer.commonutil.TestGroups;
import gaffer.data.element.Edge;
import gaffer.data.element.Element;
import gaffer.data.elementdefinition.view.View;
import gaffer.data.elementdefinition.view.ViewEdgeDefinition;
import gaffer.operation.OperationChain;
import gaffer.operation.OperationException;
import gaffer.operation.data.EdgeSeed;
import gaffer.operation.data.ElementSeed;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.impl.add.AddElements;
import gaffer.operation.impl.get.GetElements;
import gaffer.operation.impl.get.GetRelatedElements;
import gaffer.store.StoreException;

import org.apache.accumulo.core.client.AccumuloException;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class AccumuloSingleIDRetrieverTest {

    private static final String AUTHS = "Test";
    private static AccumuloStore byteEntityStore;
    private static AccumuloStore gaffer1KeyStore;

    @Before
    public void setup() throws IOException, StoreException {
        byteEntityStore = new MockAccumuloStoreForTest(ByteEntityKeyPackage.class);
        gaffer1KeyStore = new MockAccumuloStoreForTest(ClassicKeyPackage.class);
    }

    @Test
    public void testEntitySeedQueries() throws AccumuloException, StoreException {
        testEntitySeedQueries(byteEntityStore);
        testEntitySeedQueries(gaffer1KeyStore);
    }

    public void testEntitySeedQueries(final AccumuloStore store) throws AccumuloException, StoreException {
        // Create mock Accumulo instance and table
        // Populate graph
        int numEntries = 1000;
        setupGraph(store, numEntries, false);
        // Create set to query for
        Set<ElementSeed> ids = new HashSet<>();
        for (int i = 0; i < numEntries; i++) {
            ids.add(new EntitySeed(""+i));
        }
        final View view = new View.Builder().edge(TestGroups.EDGE, new ViewEdgeDefinition()).build();

        // Retrieve elements when less simple entities are provided than the max number of entries for the batch scanner
        AccumuloSingleIDRetriever retriever = null;
        GetElements<ElementSeed, ?> operation = new GetRelatedElements<>(view, ids);
        operation.addOption(Constants.OPERATION_AUTHORISATIONS, AUTHS);
        try {
            retriever = new AccumuloSingleIDRetriever(store, operation);
        } catch (IteratorSettingException e) {
            e.printStackTrace();
        }
        int count = 0;
        for (@SuppressWarnings("unused") Element element : retriever) {
            count++;
        }
        assertEquals(numEntries, count);
    }

    @Test
    public void testUndirectedEdgeSeedQueries() throws AccumuloException, StoreException {
        testUndirectedEdgeSeedQueries(byteEntityStore);
        testUndirectedEdgeSeedQueries(gaffer1KeyStore);
    }

    public void testUndirectedEdgeSeedQueries(final AccumuloStore store) throws AccumuloException, StoreException {
        // Create mock Accumulo instance and table
        // Populate graph
        int numEntries = 1000;
        setupGraph(store, numEntries, false);
        // Create set to query for
        Set<ElementSeed> ids = new HashSet<>();
        for (int i = 0; i < numEntries; i++) {
            ids.add(new EdgeSeed("" +i, "B", false));
        }
        final View view = new View.Builder().edge(TestGroups.EDGE, new ViewEdgeDefinition()).build();

        // Retrieve elements when less simple entities are provided than the max number of entries for the batch scanner
        AccumuloSingleIDRetriever retriever = null;
        GetElements<ElementSeed, ?> operation = new GetRelatedElements<>(view, ids);
        operation.addOption(Constants.OPERATION_AUTHORISATIONS, AUTHS);
        try {
            retriever = new AccumuloSingleIDRetriever(store, operation);
        } catch (IteratorSettingException e) {
            e.printStackTrace();
        }
        int count = 0;
        for (@SuppressWarnings("unused") Element element : retriever) {
            count++;
        }
        assertEquals(numEntries, count);
    }


    @Test
    public void testDirectedEdgeSeedQueries() throws AccumuloException, StoreException {
        testDirectedEdgeSeedQueries(byteEntityStore);
        testDirectedEdgeSeedQueries(gaffer1KeyStore);
    }

    public void testDirectedEdgeSeedQueries(final AccumuloStore store) throws AccumuloException, StoreException {
        // Create mock Accumulo instance and table
        // Populate graph
        int numEntries = 1000;
        setupGraph(store, numEntries, true);
        // Create set to query for
        Set<ElementSeed> ids = new HashSet<>();
        for (int i = 0; i < numEntries; i++) {
            ids.add(new EdgeSeed("" +i, "B", true));
        }
        final View view = new View.Builder().edge(TestGroups.EDGE, new ViewEdgeDefinition()).build();

        // Retrieve elements when less simple entities are provided than the max number of entries for the batch scanner
        AccumuloSingleIDRetriever retriever = null;
        GetElements<ElementSeed, ?> operation = new GetRelatedElements<>(view, ids);
        operation.addOption(Constants.OPERATION_AUTHORISATIONS, AUTHS);
        try {
            retriever = new AccumuloSingleIDRetriever(store, operation);
        } catch (IteratorSettingException e) {
            e.printStackTrace();
        }
        int count = 0;
        for (@SuppressWarnings("unused") Element element : retriever) {
            count++;
        }
        assertEquals(numEntries, count);
    }

    private void setupGraph(final AccumuloStore store, final int numEntries, final boolean directed) {
        List<Element> elements = new ArrayList<>();
        for (int i = 0; i < numEntries; i++) {
            Edge edge = new Edge(TestGroups.EDGE);
            edge.setSource(""+i);
            edge.setDestination("B");
            edge.setDirected(directed);
            elements.add(edge);
        }
        AddElements add = new AddElements(elements);
        add.addOption(Constants.OPERATION_AUTHORISATIONS, AUTHS);
        try {
            store.execute(new OperationChain<>(add));
        } catch (OperationException e) {
            fail("Couldn't add element: " + e);
        }
    }
}