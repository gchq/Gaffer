/*
 * Copyright 2016 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gaffer.accumulostore.operation.handler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.google.common.collect.Iterables;
import gaffer.accumulostore.AccumuloProperties;
import gaffer.accumulostore.AccumuloStore;
import gaffer.accumulostore.SingleUseMockAccumuloStore;
import gaffer.accumulostore.operation.impl.GetElementsInRanges;
import gaffer.accumulostore.utils.AccumuloPropertyNames;
import gaffer.accumulostore.utils.Pair;
import gaffer.commonutil.StreamUtil;
import gaffer.commonutil.TestGroups;
import gaffer.data.element.Edge;
import gaffer.data.element.Element;
import gaffer.data.elementdefinition.view.View;
import gaffer.operation.GetOperation.IncludeEdgeType;
import gaffer.operation.GetOperation.IncludeIncomingOutgoingType;
import gaffer.operation.OperationException;
import gaffer.operation.data.ElementSeed;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.impl.add.AddElements;
import gaffer.store.StoreException;
import gaffer.store.schema.Schema;
import gaffer.user.User;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class GetElementsinRangesHandlerTest {

    private static View defaultView;
    private static AccumuloStore byteEntityStore;
    private static AccumuloStore gaffer1KeyStore;
    private static final Schema schema = Schema.fromJson(StreamUtil.schemas(GetElementsinRangesHandlerTest.class));
    private static final AccumuloProperties PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.storeProps(GetElementsinRangesHandlerTest.class));
    private static final AccumuloProperties CLASSIC_PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.openStream(GetElementsinRangesHandlerTest.class, "/accumuloStoreClassicKeys.properties"));

    private static final User user = new User();

    @BeforeClass
    public static void setup() throws StoreException, IOException {
        byteEntityStore = new SingleUseMockAccumuloStore();
        gaffer1KeyStore = new SingleUseMockAccumuloStore();
        defaultView = new View.Builder().edge(TestGroups.EDGE).entity(TestGroups.ENTITY).build();
    }

    @Before
    public void reInitialise() throws StoreException {
        byteEntityStore.initialise(schema, PROPERTIES);
        gaffer1KeyStore.initialise(schema, CLASSIC_PROPERTIES);
        setupGraph(byteEntityStore, 1000);
        setupGraph(gaffer1KeyStore, 1000);
    }

    @AfterClass
    public static void tearDown() {
        byteEntityStore = null;
        gaffer1KeyStore = null;
        defaultView = null;
    }

    @Test
    public void testNoSummarisationByteEntityStore() throws OperationException {
        shouldReturnElementsNoSummarisation(byteEntityStore);
    }

    @Test
    public void testNoSummarisationGaffer1Store() throws OperationException {
        shouldReturnElementsNoSummarisation(gaffer1KeyStore);
    }

    private void shouldReturnElementsNoSummarisation(final AccumuloStore store) throws OperationException {
        // Create set to query for
        final Set<Pair<ElementSeed>> simpleEntityRanges = new HashSet<>();
        final User user = new User();

        //get Everything between 0 and 1 (Note we are using strings and string serialisers, with this ordering 0999 is before 1)
        simpleEntityRanges.add(new Pair<ElementSeed>(new EntitySeed("0"), new EntitySeed("1")));
        final GetElementsInRanges<Pair<ElementSeed>, Element> operation = new GetElementsInRanges<>(defaultView, simpleEntityRanges);

        final GetElementsInRangesHandler handler = new GetElementsInRangesHandler();
        Iterable<Element> elementsInRanges = handler.doOperation(operation, user, store);
        final int elementsInRangesCount = Iterables.size(elementsInRanges);
        //Each Edge was put in 3 times with different col qualifiers, without summarisation we expect this number
        assertEquals(1000 * 3, elementsInRangesCount);

        simpleEntityRanges.clear();
        //This should get everything between 0 and 0799 (again being string ordering 0800 is more than 08)
        simpleEntityRanges.add(new Pair<ElementSeed>(new EntitySeed("0"), new EntitySeed("08")));
        final Iterable<Element> elements = handler.doOperation(operation, user, store);
        final int count = Iterables.size(elements);
        //Each Edge was put in 3 times with different col qualifiers, without summarisation we expect this number
        assertEquals(800 * 3, count);

    }

    @Test
    public void shouldSummariseByteEntityStore() throws OperationException {
        shouldSummarise(byteEntityStore);
    }

    @Test
    public void shouldSummariseGaffer2Store() throws OperationException {
        shouldSummarise(gaffer1KeyStore);
    }

    private void shouldSummarise(final AccumuloStore store) throws OperationException {
        // Create set to query for
        final Set<Pair<ElementSeed>> simpleEntityRanges = new HashSet<>();

        //get Everything between 0 and 1 (Note we are using strings and string serialisers, with this ordering 0999 is before 1)
        simpleEntityRanges.add(new Pair<ElementSeed>(new EntitySeed("0"), new EntitySeed("1")));
        final GetElementsInRanges<Pair<ElementSeed>, Element> operation = new GetElementsInRanges<>(defaultView, simpleEntityRanges);
        operation.setSummarise(true);
        final GetElementsInRangesHandler handler = new GetElementsInRangesHandler();
        final Iterable<Element> elementsInRange = handler.doOperation(operation, user, store);
        int count = 0;
        for (final Element elm : elementsInRange) {
            //Make sure every element has been summarised
            assertEquals(9, elm.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER));
            count++;
        }
        assertEquals(1000, count);

        simpleEntityRanges.clear();
        //This should get everything between 0 and 0799 (again being string ordering 0800 is more than 08)
        simpleEntityRanges.add(new Pair<ElementSeed>(new EntitySeed("0"), new EntitySeed("08")));
        final Iterable<Element> elements = handler.doOperation(operation, user, store);
        count = 0;
        for (final Element elm : elements) {
            //Make sure every element has been summarised
            assertEquals(9, elm.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER));
            count++;
        }
        assertEquals(800, count);

    }

    @Test
    public void shouldSummariseOutGoingEdgesOnlyByteEntityStore() throws OperationException {
        shouldSummariseOutGoingEdgesOnly(byteEntityStore);
    }

    @Test
    public void shouldSummariseOutGoingEdgesOnlyGaffer1Store() throws OperationException {
        shouldSummariseOutGoingEdgesOnly(gaffer1KeyStore);
    }

    private void shouldSummariseOutGoingEdgesOnly(final AccumuloStore store) throws OperationException {
        // Create set to query for
        final Set<Pair<ElementSeed>> simpleEntityRanges = new HashSet<>();

        //get Everything between 0 and 1 (Note we are using strings and string serialisers, with this ordering 0999 is before 1)
        simpleEntityRanges.add(new Pair<ElementSeed>(new EntitySeed("0"), new EntitySeed("C")));
        final GetElementsInRanges<Pair<ElementSeed>, Element> operation = new GetElementsInRanges<>(defaultView, simpleEntityRanges);
        operation.setSummarise(true);

        //All Edges stored should be outgoing from our provided seeds.
        operation.setIncludeIncomingOutGoing(IncludeIncomingOutgoingType.OUTGOING);
        final GetElementsInRangesHandler handler = new GetElementsInRangesHandler();
        final Iterable<Element> rangeElements = handler.doOperation(operation, user, store);
        int count = 0;
        for (final Element elm : rangeElements) {
            //Make sure every element has been summarised
            assertEquals(9, elm.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER));
            count++;
        }
        assertEquals(1000, count);

        simpleEntityRanges.clear();
        //This should get everything between 0 and 0799 (again being string ordering 0800 is more than 08)
        simpleEntityRanges.add(new Pair<ElementSeed>(new EntitySeed("0"), new EntitySeed("08")));
        final Iterable<Element> elements = handler.doOperation(operation, user, store);
        count = 0;
        for (Element elm : elements) {
            //Make sure every element has been summarised
            assertEquals(9, elm.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER));
            count++;
        }
        assertEquals(800, count);

    }

    @Test
    public void shouldHaveNoIncomingEdgesByteEntityStore() throws OperationException {
        shouldHaveNoIncomingEdges(byteEntityStore);
    }

    @Test
    public void shouldHaveNoIncomingEdgesGaffer1Store() throws OperationException {
        shouldHaveNoIncomingEdges(gaffer1KeyStore);
    }

    private void shouldHaveNoIncomingEdges(final AccumuloStore store) throws OperationException {
        // Create set to query for
        final Set<Pair<ElementSeed>> simpleEntityRanges = new HashSet<>();
        final User user = new User();

        //get Everything between 0 and 1 (Note we are using strings and string serialisers, with this ordering 0999 is before 1)
        simpleEntityRanges.add(new Pair<ElementSeed>(new EntitySeed("0"), new EntitySeed("1")));
        final GetElementsInRanges<Pair<ElementSeed>, Element> operation = new GetElementsInRanges<>(defaultView, simpleEntityRanges);

        //All Edges stored should be outgoing from our provided seeds.
        operation.setIncludeIncomingOutGoing(IncludeIncomingOutgoingType.INCOMING);
        operation.setSummarise(true);
        final GetElementsInRangesHandler handler = new GetElementsInRangesHandler();
        final Iterable<Element> elements = handler.doOperation(operation, user, store);
        final int count = Iterables.size(elements);
        //There should be no incoming edges to the provided range
        assertEquals(0, count);
    }

    @Test
    public void shouldReturnNothingWhenNoEdgesSetByteEntityStore() throws OperationException {
        shouldReturnNothingWhenNoEdgesSet(byteEntityStore);
    }

    @Test
    public void shouldReturnNothingWhenNoEdgesSetGaffer1Store() throws OperationException {
        shouldReturnNothingWhenNoEdgesSet(gaffer1KeyStore);
    }

    private void shouldReturnNothingWhenNoEdgesSet(final AccumuloStore store) throws OperationException {
        // Create set to query for
        final Set<Pair<ElementSeed>> simpleEntityRanges = new HashSet<>();

        //get Everything between 0 and 1 (Note we are using strings and string serialisers, with this ordering 0999 is before 1)
        simpleEntityRanges.add(new Pair<ElementSeed>(new EntitySeed("0"), new EntitySeed("1")));
        final GetElementsInRanges<Pair<ElementSeed>, Element> operation = new GetElementsInRanges<>(defaultView, simpleEntityRanges);

        //All Edges stored should be outgoing from our provided seeds.
        operation.setIncludeEdges(IncludeEdgeType.UNDIRECTED);
        operation.setSummarise(true);
        final GetElementsInRangesHandler handler = new GetElementsInRangesHandler();
        final Iterable<Element> elements = handler.doOperation(operation, user, store);
        final int count = Iterables.size(elements);
        //There should be no incoming edges to the provided range
        assertEquals(0, count);

    }

    private static void setupGraph(final AccumuloStore store, final int numEntries) {
        final List<Element> elements = new ArrayList<>();
        for (int i = 0; i < numEntries; i++) {

            String s = "" + i;
            while (s.length() < 4) {
                s = "0" + s;
            }

            final Edge edge = new Edge(TestGroups.EDGE);
            edge.setSource(s);

            edge.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 1);
            edge.setDestination("B");
            edge.setDirected(true);
            elements.add(edge);

            final Edge edge2 = new Edge(TestGroups.EDGE);
            edge2.setSource(s);
            edge2.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 3);
            edge2.setDestination("B");
            edge2.setDirected(true);
            elements.add(edge2);

            final Edge edge3 = new Edge(TestGroups.EDGE);
            edge3.setSource(s);
            edge3.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 5);
            edge3.setDestination("B");
            edge3.setDirected(true);
            elements.add(edge3);
        }

        try {
            store.execute(new AddElements(elements), user);
        } catch (OperationException e) {
            fail("Couldn't add element: " + e);
        }
    }

}
