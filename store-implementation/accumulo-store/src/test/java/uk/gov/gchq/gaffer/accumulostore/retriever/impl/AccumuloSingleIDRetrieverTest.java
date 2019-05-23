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
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters.IncludeIncomingOutgoingType;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class AccumuloSingleIDRetrieverTest {

    private static final int NUM_ENTRIES = 1000;
    private static AccumuloStore byteEntityStore;
    private static AccumuloStore gaffer1KeyStore;
    private static final Schema SCHEMA = Schema.fromJson(StreamUtil.schemas(AccumuloSingleIDRetrieverTest.class));
    private static final AccumuloProperties PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.storeProps(AccumuloSingleIDRetrieverTest.class));
    private static final AccumuloProperties CLASSIC_PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.openStream(AccumuloSingleIDRetrieverTest.class, "/accumuloStoreClassicKeys.properties"));

    @BeforeClass
    public static void setup() {
        byteEntityStore = new SingleUseMockAccumuloStore();
        gaffer1KeyStore = new SingleUseMockAccumuloStore();
    }

    @Before
    public void reInitialise() throws StoreException {
        byteEntityStore.initialise("byteEntityGraph", SCHEMA, PROPERTIES);
        gaffer1KeyStore.initialise("gaffer1Graph", SCHEMA, CLASSIC_PROPERTIES);
        setupGraph(byteEntityStore, NUM_ENTRIES);
        setupGraph(gaffer1KeyStore, NUM_ENTRIES);
    }

    @AfterClass
    public static void tearDown() {
        byteEntityStore = null;
        gaffer1KeyStore = null;
    }

    @Test
    public void testEntityIdQueryEdgesAndEntitiesByteEntityStore() throws AccumuloException, StoreException {
        testEntityIdQueryEdgesAndEntities(byteEntityStore);
    }

    @Test
    public void testEntityIdQueryEdgesAndEntitiesGaffer1Store() throws AccumuloException, StoreException {
        testEntityIdQueryEdgesAndEntities(gaffer1KeyStore);
    }

    private void testEntityIdQueryEdgesAndEntities(final AccumuloStore store) throws AccumuloException, StoreException {
        setupGraph(store, NUM_ENTRIES);
        final User user = new User();

        // Create set to query for
        final Set<ElementId> ids = new HashSet<>();
        for (int i = 0; i < NUM_ENTRIES; i++) {
            ids.add(new EntitySeed("" + i));
        }
        final View view = new View.Builder().edge(TestGroups.EDGE).entity(TestGroups.ENTITY).build();

        final GetElements operation = new GetElements.Builder().view(view).input(ids).build();
        try {
            final AccumuloSingleIDRetriever retriever = new AccumuloSingleIDRetriever(store, operation, new User());
            assertEquals(NUM_ENTRIES * 3, Iterables.size(retriever));
        } catch (final IteratorSettingException e) {
            fail("Unable to construct SingleID Retriever");
        }
        //Should find both i-B and i-C edges and entities i
    }

    @Test
    public void testEntityIdQueryEdgesOnly() throws AccumuloException, StoreException {
        testEntityIdQueryEdgesOnly(byteEntityStore);
        testEntityIdQueryEdgesOnly(gaffer1KeyStore);
    }

    private void testEntityIdQueryEdgesOnly(final AccumuloStore store) throws StoreException {
        setupGraph(store, NUM_ENTRIES);
        final User user = new User();

        // Create set to query for
        final Set<ElementId> ids = new HashSet<>();
        for (int i = 0; i < NUM_ENTRIES; i++) {
            ids.add(new EntitySeed("" + i));
        }
        final View view = new View.Builder().edge(TestGroups.EDGE).build();

        AccumuloSingleIDRetriever retriever = null;
        final GetElements operation = new GetElements.Builder().view(view).input(ids).build();
        try {
            retriever = new AccumuloSingleIDRetriever(store, operation, user);
        } catch (final IteratorSettingException e) {
            throw new RuntimeException(e);
        }
        //Should find both i-B and i-C edges.
        assertEquals(NUM_ENTRIES * 2, Iterables.size(retriever));
    }

    @Test
    public void testEntityIdQueryEntitiesOnly() throws StoreException {
        testEntityIdQueryEntitiesOnly(byteEntityStore);
        testEntityIdQueryEntitiesOnly(gaffer1KeyStore);
    }

    private void testEntityIdQueryEntitiesOnly(final AccumuloStore store) throws StoreException {
        setupGraph(store, NUM_ENTRIES);
        final User user = new User();

        // Create set to query for
        final Set<ElementId> ids = new HashSet<>();
        for (int i = 0; i < NUM_ENTRIES; i++) {
            ids.add(new EntitySeed("" + i));
        }
        final View view = new View.Builder().entity(TestGroups.ENTITY).build();

        AccumuloSingleIDRetriever retriever = null;
        final GetElements operation = new GetElements.Builder().view(view).input(ids).build();
        try {
            retriever = new AccumuloSingleIDRetriever(store, operation, user);
        } catch (final IteratorSettingException e) {
            throw new RuntimeException(e);
        }
        //Should find only the entities i
        assertEquals(NUM_ENTRIES, Iterables.size(retriever));
    }

    @Test
    public void testUndirectedEdgeIdQueries() throws StoreException {
        testUndirectedEdgeIdQueries(byteEntityStore);
        testUndirectedEdgeIdQueries(gaffer1KeyStore);
    }

    private void testUndirectedEdgeIdQueries(final AccumuloStore store) throws StoreException {
        setupGraph(store, NUM_ENTRIES);
        final User user = new User();

        // Create set to query for
        final Set<ElementId> ids = new HashSet<>();
        for (int i = 0; i < NUM_ENTRIES; i++) {
            ids.add(new EdgeSeed("" + i, "B", false));
            ids.add(new EdgeSeed("" + i, "C", true));
        }
        final View view = new View.Builder().edge(TestGroups.EDGE).build();

        AccumuloSingleIDRetriever<?> retriever = null;
        final GetElements operation = new GetElements.Builder().view(view).input(ids).build();
        operation.setDirectedType(DirectedType.UNDIRECTED);
        try {
            retriever = new AccumuloSingleIDRetriever<>(store, operation, user);
        } catch (final IteratorSettingException e) {
            throw new RuntimeException(e);
        }
        for (final Element element : retriever) {
            Edge edge = (Edge) element;
            assertEquals("B", edge.getDestination());
        }
        //We should have only 1000 returned the i-B edges that are undirected
        assertEquals(NUM_ENTRIES, Iterables.size(retriever));
    }

    @Test
    public void testDirectedEdgeIdQueries() throws StoreException {
        testDirectedEdgeIdQueries(byteEntityStore);
        testDirectedEdgeIdQueries(gaffer1KeyStore);
    }

    private void testDirectedEdgeIdQueries(final AccumuloStore store) throws StoreException {
        setupGraph(store, NUM_ENTRIES);
        final User user = new User();

        // Create set to query for
        final Set<ElementId> ids = new HashSet<>();
        for (int i = 0; i < NUM_ENTRIES; i++) {
            ids.add(new EdgeSeed("" + i, "B", false));
            ids.add(new EdgeSeed("" + i, "C", true));
        }
        final View view = new View.Builder().edge(TestGroups.EDGE).build();

        AccumuloSingleIDRetriever<?> retriever = null;
        final GetElements operation = new GetElements.Builder().view(view).input(ids).build();
        operation.setDirectedType(DirectedType.DIRECTED);
        try {
            retriever = new AccumuloSingleIDRetriever<>(store, operation, user);
        } catch (final IteratorSettingException e) {
            throw new RuntimeException(e);
        }
        for (final Element element : retriever) {
            Edge edge = (Edge) element;
            assertEquals("C", edge.getDestination());
        }

        //Should find 1000 only A-C
        assertEquals(NUM_ENTRIES, Iterables.size(retriever));
    }

    @Test
    public void testEntityIdQueryIncomingEdgesOnly() throws StoreException {
        testEntityIdQueryIncomingEdgesOnly(byteEntityStore);
        testEntityIdQueryIncomingEdgesOnly(gaffer1KeyStore);
    }

    private void testEntityIdQueryIncomingEdgesOnly(final AccumuloStore store) throws StoreException {
        setupGraph(store, NUM_ENTRIES);
        final User user = new User();

        // Create set to query for
        final Set<ElementId> ids = new HashSet<>();
        for (int i = 0; i < NUM_ENTRIES; i++) {
            ids.add(new EntitySeed("" + i));
        }
        final View view = new View.Builder().edge(TestGroups.EDGE).build();

        AccumuloSingleIDRetriever<?> retriever = null;
        final GetElements operation = new GetElements.Builder().view(view).input(ids).build();
        operation.setIncludeIncomingOutGoing(IncludeIncomingOutgoingType.INCOMING);
        try {
            retriever = new AccumuloSingleIDRetriever<>(store, operation, user);
        } catch (final IteratorSettingException e) {
            throw new RuntimeException(e);
        }
        for (final Element element : retriever) {
            Edge edge = (Edge) element;
            assertEquals("B", edge.getDestination());
        }
        //Incoming option should find all edges i-B as undirected are both incoming and outgoing.
        assertEquals(NUM_ENTRIES, Iterables.size(retriever));
    }

    @Test
    public void testEntityIdQueryOutgoingEdgesOnly() throws StoreException {
        testEntityIdQueryOutgoingEdgesOnly(byteEntityStore);
        testEntityIdQueryOutgoingEdgesOnly(gaffer1KeyStore);
    }

    private void testEntityIdQueryOutgoingEdgesOnly(final AccumuloStore store) throws StoreException {
        setupGraph(store, NUM_ENTRIES);
        final User user = new User();

        // Create set to query for
        Set<ElementId> ids = new HashSet<>();
        for (int i = 0; i < NUM_ENTRIES; i++) {
            ids.add(new EntitySeed("" + i));
        }
        final View view = new View.Builder().edge(TestGroups.EDGE).build();

        AccumuloSingleIDRetriever<?> retriever = null;
        GetElements operation = new GetElements.Builder().view(view).input(ids).build();
        operation.setIncludeIncomingOutGoing(IncludeIncomingOutgoingType.OUTGOING);
        try {
            retriever = new AccumuloSingleIDRetriever<>(store, operation, user);
        } catch (final IteratorSettingException e) {
            throw new RuntimeException(e);
        }
        int count = 0;
        for (final Element element : retriever) {
            count++;
            assertEquals(TestGroups.EDGE, element.getGroup());
        }
        //Should find both i-B and i-C edges.
        assertEquals(NUM_ENTRIES * 2, count);
    }

    private static void setupGraph(final AccumuloStore store, final int numEntries) {
        final List<Element> elements = new ArrayList<>();
        for (int i = 0; i < numEntries; i++) {
            elements.add(new Entity.Builder()
                    .group(TestGroups.ENTITY)
                    .vertex("" + i)
                    .build()
            );

            elements.add(new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("" + i)
                    .dest("B")
                    .directed(false)
                    .build()
            );

            elements.add(new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("" + i)
                    .dest("C")
                    .directed(true)
                    .build()
            );
        }
        try {
            store.execute(new AddElements.Builder().input(elements).build(), new Context(new User()));
        } catch (final OperationException e) {
            fail("Couldn't add element: " + e);
        }
    }
}
