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

package uk.gov.gchq.gaffer.accumulostore.retriever.impl;

import com.google.common.collect.Iterables;
import org.apache.accumulo.core.client.AccumuloException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.SingleUseMockAccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.key.exception.IteratorSettingException;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.GetOperation.IncludeEdgeType;
import uk.gov.gchq.gaffer.operation.GetOperation.IncludeIncomingOutgoingType;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class AccumuloSingleIDRetrieverTest {

    private static final int numEntries = 1000;
    private static AccumuloStore byteEntityStore;
    private static AccumuloStore gaffer1KeyStore;
    private static final Schema schema = Schema.fromJson(StreamUtil.schemas(AccumuloSingleIDRetrieverTest.class));
    private static final AccumuloProperties PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.storeProps(AccumuloSingleIDRetrieverTest.class));
    private static final AccumuloProperties CLASSIC_PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.openStream(AccumuloSingleIDRetrieverTest.class, "/accumuloStoreClassicKeys.properties"));

    @BeforeClass
    public static void setup() throws StoreException, IOException {
        byteEntityStore = new SingleUseMockAccumuloStore();
        gaffer1KeyStore = new SingleUseMockAccumuloStore();
    }

    @Before
    public void reInitialise() throws StoreException {
        byteEntityStore.initialise(schema, PROPERTIES);
        gaffer1KeyStore.initialise(schema, CLASSIC_PROPERTIES);
        setupGraph(byteEntityStore, numEntries);
        setupGraph(gaffer1KeyStore, numEntries);
    }

    @AfterClass
    public static void tearDown() {
        byteEntityStore = null;
        gaffer1KeyStore = null;
    }

    @Test
    public void testEntitySeedQueryEdgesAndEntitiesByteEntityStore() throws AccumuloException, StoreException {
        testEntitySeedQueryEdgesAndEntities(byteEntityStore);
    }

    @Test
    public void testEntitySeedQueryEdgesAndEntitiesGaffer1Store() throws AccumuloException, StoreException {
        testEntitySeedQueryEdgesAndEntities(gaffer1KeyStore);
    }

    private void testEntitySeedQueryEdgesAndEntities(final AccumuloStore store) throws AccumuloException, StoreException {
        setupGraph(store, numEntries);
        final User user = new User();

        // Create set to query for
        final Set<ElementSeed> ids = new HashSet<>();
        for (int i = 0; i < numEntries; i++) {
            ids.add(new EntitySeed("" + i));
        }
        final View view = new View.Builder().edge(TestGroups.EDGE).entity(TestGroups.ENTITY).build();

        final GetElements<ElementSeed, ?> operation = new GetElements<>(view, ids);
        operation.setIncludeEntities(true);
        operation.setIncludeEdges(IncludeEdgeType.ALL);
        try {
            final AccumuloSingleIDRetriever retriever = new AccumuloSingleIDRetriever(store, operation, new User());
            assertEquals(numEntries * 3, Iterables.size(retriever));
        } catch (IteratorSettingException e) {
            fail("Unable to construct SingleID Retriever");
        }
        //Should find both i-B and i-C edges and entities i
    }

    @Test
    public void testEntitySeedQueryEdgesOnly() throws AccumuloException, StoreException {
        testEntitySeedQueryEdgesOnly(byteEntityStore);
        testEntitySeedQueryEdgesOnly(gaffer1KeyStore);
    }

    private void testEntitySeedQueryEdgesOnly(final AccumuloStore store) throws AccumuloException, StoreException {
        setupGraph(store, numEntries);
        final User user = new User();

        // Create set to query for
        final Set<ElementSeed> ids = new HashSet<>();
        for (int i = 0; i < numEntries; i++) {
            ids.add(new EntitySeed("" + i));
        }
        final View view = new View.Builder().edge(TestGroups.EDGE).entity(TestGroups.ENTITY).build();

        AccumuloSingleIDRetriever retriever = null;
        final GetElements<ElementSeed, ?> operation = new GetElements<>(view, ids);
        operation.setIncludeEntities(false);
        try {
            retriever = new AccumuloSingleIDRetriever(store, operation, user);
        } catch (IteratorSettingException e) {
            e.printStackTrace();
        }
        //Should find both i-B and i-C edges.
        assertEquals(numEntries * 2, Iterables.size(retriever));
    }

    @Test
    public void testEntitySeedQueryEntitiesOnly() throws AccumuloException, StoreException {
        testEntitySeedQueryEntitiesOnly(byteEntityStore);
        testEntitySeedQueryEntitiesOnly(gaffer1KeyStore);
    }

    private void testEntitySeedQueryEntitiesOnly(final AccumuloStore store) throws AccumuloException, StoreException {
        setupGraph(store, numEntries);
        final User user = new User();

        // Create set to query for
        final Set<ElementSeed> ids = new HashSet<>();
        for (int i = 0; i < numEntries; i++) {
            ids.add(new EntitySeed("" + i));
        }
        final View view = new View.Builder().edge(TestGroups.EDGE).entity(TestGroups.ENTITY).build();

        AccumuloSingleIDRetriever retriever = null;
        final GetElements<ElementSeed, ?> operation = new GetElements<>(view, ids);
        operation.setIncludeEntities(true);
        operation.setIncludeEdges(IncludeEdgeType.NONE);
        try {
            retriever = new AccumuloSingleIDRetriever(store, operation, user);
        } catch (IteratorSettingException e) {
            e.printStackTrace();
        }
        //Should find only the entities i
        assertEquals(numEntries, Iterables.size(retriever));
    }

    @Test
    public void testUndirectedEdgeSeedQueries() throws AccumuloException, StoreException {
        testUndirectedEdgeSeedQueries(byteEntityStore);
        testUndirectedEdgeSeedQueries(gaffer1KeyStore);
    }

    private void testUndirectedEdgeSeedQueries(final AccumuloStore store) throws AccumuloException, StoreException {
        setupGraph(store, numEntries);
        final User user = new User();

        // Create set to query for
        final Set<ElementSeed> ids = new HashSet<>();
        for (int i = 0; i < numEntries; i++) {
            ids.add(new EdgeSeed("" + i, "B", false));
            ids.add(new EdgeSeed("" + i, "C", true));
        }
        final View view = new View.Builder().edge(TestGroups.EDGE).build();

        AccumuloSingleIDRetriever retriever = null;
        final GetElements<ElementSeed, ?> operation = new GetElements<>(view, ids);
        operation.setIncludeEdges(IncludeEdgeType.UNDIRECTED);
        try {
            retriever = new AccumuloSingleIDRetriever(store, operation, user);
        } catch (IteratorSettingException e) {
            e.printStackTrace();
        }
        for (final Element element : retriever) {
            Edge edge = (Edge) element;
            assertEquals("B", edge.getDestination());
        }
        //We should have only 1000 returned the i-B edges that are undirected
        assertEquals(numEntries, Iterables.size(retriever));
    }

    @Test
    public void testDirectedEdgeSeedQueries() throws AccumuloException, StoreException {
        testDirectedEdgeSeedQueries(byteEntityStore);
        testDirectedEdgeSeedQueries(gaffer1KeyStore);
    }

    private void testDirectedEdgeSeedQueries(final AccumuloStore store) throws AccumuloException, StoreException {
        setupGraph(store, numEntries);
        final User user = new User();

        // Create set to query for
        final Set<ElementSeed> ids = new HashSet<>();
        for (int i = 0; i < numEntries; i++) {
            ids.add(new EdgeSeed("" + i, "B", false));
            ids.add(new EdgeSeed("" + i, "C", true));
        }
        final View view = new View.Builder().edge(TestGroups.EDGE).build();

        AccumuloSingleIDRetriever retriever = null;
        final GetElements<ElementSeed, ?> operation = new GetElements<>(view, ids);
        operation.setIncludeEdges(IncludeEdgeType.DIRECTED);
        try {
            retriever = new AccumuloSingleIDRetriever(store, operation, user);
        } catch (IteratorSettingException e) {
            e.printStackTrace();
        }
        for (final Element element : retriever) {
            Edge edge = (Edge) element;
            assertEquals("C", edge.getDestination());
        }

        //Should find 1000 only A-C
        assertEquals(numEntries, Iterables.size(retriever));
    }

    @Test
    public void testEntitySeedQueryIncomingEdgesOnly() throws AccumuloException, StoreException {
        testEntitySeedQueryIncomingEdgesOnly(byteEntityStore);
        testEntitySeedQueryIncomingEdgesOnly(gaffer1KeyStore);
    }

    private void testEntitySeedQueryIncomingEdgesOnly(final AccumuloStore store) throws AccumuloException, StoreException {
        setupGraph(store, numEntries);
        final User user = new User();

        // Create set to query for
        final Set<ElementSeed> ids = new HashSet<>();
        for (int i = 0; i < numEntries; i++) {
            ids.add(new EntitySeed("" + i));
        }
        final View view = new View.Builder().edge(TestGroups.EDGE).entity(TestGroups.ENTITY).build();

        AccumuloSingleIDRetriever retriever = null;
        final GetElements<ElementSeed, ?> operation = new GetElements<>(view, ids);
        operation.setIncludeEntities(false);
        operation.setIncludeIncomingOutGoing(IncludeIncomingOutgoingType.INCOMING);
        try {
            retriever = new AccumuloSingleIDRetriever(store, operation, user);
        } catch (IteratorSettingException e) {
            e.printStackTrace();
        }
        for (final Element element : retriever) {
            Edge edge = (Edge) element;
            assertEquals("B", edge.getDestination());
        }
        //Incoming option should find all edges i-B as undirected are both incoming and outgoing.
        assertEquals(numEntries, Iterables.size(retriever));
    }

    @Test
    public void testEntitySeedQueryOutgoingEdgesOnly() throws AccumuloException, StoreException {
        testEntitySeedQueryOutgoingEdgesOnly(byteEntityStore);
        testEntitySeedQueryOutgoingEdgesOnly(gaffer1KeyStore);
    }

    private void testEntitySeedQueryOutgoingEdgesOnly(final AccumuloStore store) throws AccumuloException, StoreException {
        setupGraph(store, numEntries);
        final User user = new User();

        // Create set to query for
        Set<ElementSeed> ids = new HashSet<>();
        for (int i = 0; i < numEntries; i++) {
            ids.add(new EntitySeed("" + i));
        }
        final View view = new View.Builder().edge(TestGroups.EDGE).entity(TestGroups.ENTITY).build();

        AccumuloSingleIDRetriever retriever = null;
        GetElements<ElementSeed, ?> operation = new GetElements<>(view, ids);
        operation.setIncludeEntities(false);
        operation.setIncludeIncomingOutGoing(IncludeIncomingOutgoingType.OUTGOING);
        try {
            retriever = new AccumuloSingleIDRetriever(store, operation, user);
        } catch (IteratorSettingException e) {
            e.printStackTrace();
        }
        int count = 0;
        for (final Element element : retriever) {
            count++;
            assertEquals(TestGroups.EDGE, element.getGroup());
        }
        //Should find both i-B and i-C edges.
        assertEquals(numEntries * 2, count);
    }

    private static void setupGraph(final AccumuloStore store, final int numEntries) {
        final List<Element> elements = new ArrayList<>();
        for (int i = 0; i < numEntries; i++) {
            final Entity entity = new Entity(TestGroups.ENTITY);
            entity.setVertex("" + i);

            final Edge edge = new Edge(TestGroups.EDGE);
            edge.setSource("" + i);
            edge.setDestination("B");
            edge.setDirected(false);

            final Edge edge2 = new Edge(TestGroups.EDGE);
            edge2.setSource("" + i);
            edge2.setDestination("C");
            edge2.setDirected(true);

            elements.add(edge);
            elements.add(edge2);
            elements.add(entity);
        }
        try {
            store.execute(new AddElements(elements), new User());
        } catch (OperationException e) {
            fail("Couldn't add element: " + e);
        }
    }

}
