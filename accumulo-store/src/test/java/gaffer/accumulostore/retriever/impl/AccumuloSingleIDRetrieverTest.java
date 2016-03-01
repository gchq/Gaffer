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
import gaffer.operation.data.EdgeSeed;
import gaffer.operation.data.ElementSeed;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.impl.add.AddElements;
import gaffer.operation.impl.get.GetElements;
import gaffer.operation.impl.get.GetRelatedElements;
import gaffer.store.StoreException;
import org.apache.accumulo.core.client.AccumuloException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class AccumuloSingleIDRetrieverTest {

    private static final int numEntries = 1000;
    private static AccumuloStore byteEntityStore;
    private static AccumuloStore gaffer1KeyStore;

    @BeforeClass
    public static void setup() throws IOException, StoreException {
        byteEntityStore = new MockAccumuloStoreForTest(ByteEntityKeyPackage.class);
        gaffer1KeyStore = new MockAccumuloStoreForTest(ClassicKeyPackage.class);
        setupGraph(byteEntityStore, numEntries);
        setupGraph(gaffer1KeyStore, numEntries);
    }

    @AfterClass
    public static void tearDown() {
        byteEntityStore = null;
        gaffer1KeyStore = null;
    }

    @Test
    public void testEntitySeedQueryEdgesAndEntities() throws AccumuloException, StoreException {
        testEntitySeedQueryEdgesAndEntities(byteEntityStore);
        testEntitySeedQueryEdgesAndEntities(gaffer1KeyStore);
    }

    public void testEntitySeedQueryEdgesAndEntities(final AccumuloStore store) throws AccumuloException, StoreException {
        setupGraph(store, numEntries);
        // Create set to query for
        Set<ElementSeed> ids = new HashSet<>();
        for (int i = 0; i < numEntries; i++) {
            ids.add(new EntitySeed("" + i));
        }
        final View view = new View.Builder().edge(TestGroups.EDGE, new ViewEdgeDefinition()).entity(TestGroups.ENTITY, new ViewEntityDefinition()).build();

        AccumuloSingleIDRetriever retriever = null;
        GetElements<ElementSeed, ?> operation = new GetRelatedElements<>(view, ids);
        operation.setIncludeEntities(true);
        operation.setIncludeEdges(IncludeEdgeType.ALL);
        try {
            retriever = new AccumuloSingleIDRetriever(store, operation);
        } catch (IteratorSettingException e) {
            e.printStackTrace();
        }
        int count = 0;
        for (@SuppressWarnings("unused") Element element : retriever) {
            count++;
        }
        //Should find both i-B and i-C edges and entities i
        assertEquals(numEntries * 3, count);
    }

    @Test
    public void testEntitySeedQueryEdgesOnly() throws AccumuloException, StoreException {
        testEntitySeedQueryEdgesOnly(byteEntityStore);
        testEntitySeedQueryEdgesOnly(gaffer1KeyStore);
    }

    public void testEntitySeedQueryEdgesOnly(final AccumuloStore store) throws AccumuloException, StoreException {
        setupGraph(store, numEntries);
        // Create set to query for
        Set<ElementSeed> ids = new HashSet<>();
        for (int i = 0; i < numEntries; i++) {
            ids.add(new EntitySeed("" + i));
        }
        final View view = new View.Builder().edge(TestGroups.EDGE, new ViewEdgeDefinition()).entity(TestGroups.ENTITY, new ViewEntityDefinition()).build();

        AccumuloSingleIDRetriever retriever = null;
        GetElements<ElementSeed, ?> operation = new GetRelatedElements<>(view, ids);
        operation.setIncludeEntities(false);
        try {
            retriever = new AccumuloSingleIDRetriever(store, operation);
        } catch (IteratorSettingException e) {
            e.printStackTrace();
        }
        int count = 0;
        for (Element element : retriever) {
            count++;
            assertEquals(TestGroups.EDGE, element.getGroup());
        }
        //Should find both i-B and i-C edges.
        assertEquals(numEntries * 2, count);
    }

    @Test
    public void testEntitySeedQueryEntitiesOnly() throws AccumuloException, StoreException {
        testEntitySeedQueryEntitiesOnly(byteEntityStore);
        testEntitySeedQueryEntitiesOnly(gaffer1KeyStore);
    }

    public void testEntitySeedQueryEntitiesOnly(final AccumuloStore store) throws AccumuloException, StoreException {
        setupGraph(store, numEntries);
        // Create set to query for
        Set<ElementSeed> ids = new HashSet<>();
        for (int i = 0; i < numEntries; i++) {
            ids.add(new EntitySeed("" + i));
        }
        final View view = new View.Builder().edge(TestGroups.EDGE, new ViewEdgeDefinition()).entity(TestGroups.ENTITY, new ViewEntityDefinition()).build();

        AccumuloSingleIDRetriever retriever = null;
        GetElements<ElementSeed, ?> operation = new GetRelatedElements<>(view, ids);
        operation.setIncludeEntities(true);
        operation.setIncludeEdges(IncludeEdgeType.NONE);
        try {
            retriever = new AccumuloSingleIDRetriever(store, operation);
        } catch (IteratorSettingException e) {
            e.printStackTrace();
        }
        int count = 0;
        for (Element element : retriever) {
            count++;
            assertEquals(TestGroups.ENTITY, element.getGroup());
        }
        //Should find only the entities i
        assertEquals(numEntries, count);
    }

    @Test
    public void testUndirectedEdgeSeedQueries() throws AccumuloException, StoreException {
        testUndirectedEdgeSeedQueries(byteEntityStore);
        testUndirectedEdgeSeedQueries(gaffer1KeyStore);
    }

    public void testUndirectedEdgeSeedQueries(final AccumuloStore store) throws AccumuloException, StoreException {
        setupGraph(store, numEntries);
        // Create set to query for
        Set<ElementSeed> ids = new HashSet<>();
        for (int i = 0; i < numEntries; i++) {
            ids.add(new EdgeSeed("" + i, "B", false));
            ids.add(new EdgeSeed("" + i, "C", true));
        }
        final View view = new View.Builder().edge(TestGroups.EDGE, new ViewEdgeDefinition()).build();

        AccumuloSingleIDRetriever retriever = null;
        GetElements<ElementSeed, ?> operation = new GetRelatedElements<>(view, ids);
        operation.setIncludeEdges(IncludeEdgeType.UNDIRECTED);
        try {
            retriever = new AccumuloSingleIDRetriever(store, operation);
        } catch (IteratorSettingException e) {
            e.printStackTrace();
        }
        int count = 0;
        for (Element element : retriever) {
            count++;
            Edge edge = (Edge) element;
            assertEquals("B", edge.getDestination());
        }
        //We should have only 1000 returned the i-B edges that are undirected
        assertEquals(numEntries, count);
    }

    @Test
    public void testDirectedEdgeSeedQueries() throws AccumuloException, StoreException {
        testDirectedEdgeSeedQueries(byteEntityStore);
        testDirectedEdgeSeedQueries(gaffer1KeyStore);
    }

    public void testDirectedEdgeSeedQueries(final AccumuloStore store) throws AccumuloException, StoreException {
        setupGraph(store, numEntries);
        // Create set to query for
        Set<ElementSeed> ids = new HashSet<>();
        for (int i = 0; i < numEntries; i++) {
            ids.add(new EdgeSeed("" + i, "B", false));
            ids.add(new EdgeSeed("" + i, "C", true));
        }
        final View view = new View.Builder().edge(TestGroups.EDGE, new ViewEdgeDefinition()).build();

        AccumuloSingleIDRetriever retriever = null;
        GetElements<ElementSeed, ?> operation = new GetRelatedElements<>(view, ids);
        operation.setIncludeEdges(IncludeEdgeType.DIRECTED);
        try {
            retriever = new AccumuloSingleIDRetriever(store, operation);
        } catch (IteratorSettingException e) {
            e.printStackTrace();
        }
        int count = 0;
        for (Element element : retriever) {
            count++;
            Edge edge = (Edge) element;
            assertEquals("C", edge.getDestination());
        }

        //Should find 1000 only A-C
        assertEquals(numEntries, count);
    }

    @Test
    public void testEntitySeedQueryIncomingEdgesOnly() throws AccumuloException, StoreException {
        testEntitySeedQueryIncomingEdgesOnly(byteEntityStore);
        testEntitySeedQueryIncomingEdgesOnly(gaffer1KeyStore);
    }

    public void testEntitySeedQueryIncomingEdgesOnly(final AccumuloStore store) throws AccumuloException, StoreException {
        setupGraph(store, numEntries);
        // Create set to query for
        Set<ElementSeed> ids = new HashSet<>();
        for (int i = 0; i < numEntries; i++) {
            ids.add(new EntitySeed("" + i));
        }
        final View view = new View.Builder().edge(TestGroups.EDGE, new ViewEdgeDefinition()).entity(TestGroups.ENTITY, new ViewEntityDefinition()).build();

        AccumuloSingleIDRetriever retriever = null;
        GetElements<ElementSeed, ?> operation = new GetRelatedElements<>(view, ids);
        operation.setIncludeEntities(false);
        operation.setIncludeIncomingOutGoing(IncludeIncomingOutgoingType.INCOMING);
        try {
            retriever = new AccumuloSingleIDRetriever(store, operation);
        } catch (IteratorSettingException e) {
            e.printStackTrace();
        }
        int count = 0;
        for (Element element : retriever) {
            count++;
            Edge edge = (Edge) element;
            assertEquals("B", edge.getDestination());
        }
        //Incoming option should find all edges i-B as undirected are both incoming and outgoing.
        assertEquals(numEntries, count);
    }

    @Test
    public void testEntitySeedQueryOutgoingEdgesOnly() throws AccumuloException, StoreException {
        testEntitySeedQueryOutgoingEdgesOnly(byteEntityStore);
        testEntitySeedQueryOutgoingEdgesOnly(gaffer1KeyStore);
    }

    public void testEntitySeedQueryOutgoingEdgesOnly(final AccumuloStore store) throws AccumuloException, StoreException {
        setupGraph(store, numEntries);
        // Create set to query for
        Set<ElementSeed> ids = new HashSet<>();
        for (int i = 0; i < numEntries; i++) {
            ids.add(new EntitySeed("" + i));
        }
        final View view = new View.Builder().edge(TestGroups.EDGE, new ViewEdgeDefinition()).entity(TestGroups.ENTITY, new ViewEntityDefinition()).build();

        AccumuloSingleIDRetriever retriever = null;
        GetElements<ElementSeed, ?> operation = new GetRelatedElements<>(view, ids);
        operation.setIncludeEntities(false);
        operation.setIncludeIncomingOutGoing(IncludeIncomingOutgoingType.OUTGOING);
        try {
            retriever = new AccumuloSingleIDRetriever(store, operation);
        } catch (IteratorSettingException e) {
            e.printStackTrace();
        }
        int count = 0;
        for (Element element : retriever) {
            count++;
            assertEquals(TestGroups.EDGE, element.getGroup());
        }
        //Should find both i-B and i-C edges.
        assertEquals(numEntries * 2, count);
    }

    private static void setupGraph(final AccumuloStore store, final int numEntries) {
        List<Element> elements = new ArrayList<>();
        for (int i = 0; i < numEntries; i++) {
            Entity entity = new Entity(TestGroups.ENTITY);
            Edge edge = new Edge(TestGroups.EDGE);
            Edge edge2 = new Edge(TestGroups.EDGE);
            entity.setVertex("" + i);
            edge.setSource("" + i);
            edge2.setSource("" + i);
            edge.setDestination("B");
            edge2.setDestination("C");
            edge.setDirected(false);
            edge2.setDirected(true);
            elements.add(edge);
            elements.add(edge2);
            elements.add(entity);
        }
        try {
            store.execute(new AddElements(elements));
        } catch (OperationException e) {
            fail("Couldn't add element: " + e);
        }
    }

}
