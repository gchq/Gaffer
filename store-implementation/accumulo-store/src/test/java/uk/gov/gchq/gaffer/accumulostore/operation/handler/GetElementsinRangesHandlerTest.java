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

package uk.gov.gchq.gaffer.accumulostore.operation.handler;

import com.google.common.collect.Iterables;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.SingleUseMockAccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.operation.impl.GetElementsInRanges;
import uk.gov.gchq.gaffer.accumulostore.utils.AccumuloPropertyNames;
import uk.gov.gchq.gaffer.accumulostore.utils.Pair;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.operation.GetOperation.IncludeEdgeType;
import uk.gov.gchq.gaffer.operation.GetOperation.IncludeIncomingOutgoingType;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
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

    }

    @Before
    public void reInitialise() throws StoreException {
        defaultView = new View.Builder().edge(TestGroups.EDGE).entity(TestGroups.ENTITY).build();

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
        CloseableIterable<Element> elementsInRanges = handler.doOperation(operation, user, store);
        final int elementsInRangesCount = Iterables.size(elementsInRanges);
        //Each Edge was put in 3 times with different col qualifiers, without summarisation we expect this number
        assertEquals(1000 * 3, elementsInRangesCount);
        elementsInRanges.close();
        simpleEntityRanges.clear();
        //This should get everything between 0 and 0799 (again being string ordering 0800 is more than 08)
        simpleEntityRanges.add(new Pair<ElementSeed>(new EntitySeed("0"), new EntitySeed("08")));
        final CloseableIterable<Element> elements = handler.doOperation(operation, user, store);
        final int count = Iterables.size(elements);
        //Each Edge was put in 3 times with different col qualifiers, without summarisation we expect this number
        assertEquals(800 * 3, count);
        elements.close();

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
        final View view = new View.Builder(defaultView)
                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                        .groupBy()
                        .build())
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .groupBy()
                        .build())
                .build();
        final GetElementsInRanges<Pair<ElementSeed>, Element> operation = new GetElementsInRanges<>(view, simpleEntityRanges);
        final GetElementsInRangesHandler handler = new GetElementsInRangesHandler();
        final CloseableIterable<Element> elementsInRange = handler.doOperation(operation, user, store);
        int count = 0;
        for (final Element elm : elementsInRange) {
            //Make sure every element has been summarised
            assertEquals(9, elm.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER));
            count++;
        }
        assertEquals(1000, count);

        elementsInRange.close();
        simpleEntityRanges.clear();
        //This should get everything between 0 and 0799 (again being string ordering 0800 is more than 08)
        simpleEntityRanges.add(new Pair<ElementSeed>(new EntitySeed("0"), new EntitySeed("08")));
        final CloseableIterable<Element> elements = handler.doOperation(operation, user, store);
        count = 0;
        for (final Element elm : elements) {
            //Make sure every element has been summarised
            assertEquals(9, elm.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER));
            count++;
        }
        assertEquals(800, count);
        elements.close();

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

        final View view = new View.Builder(defaultView)
                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                        .groupBy()
                        .build())
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .groupBy()
                        .build())
                .build();
        final GetElementsInRanges<Pair<ElementSeed>, Element> operation = new GetElementsInRanges<>(view, simpleEntityRanges);

        //All Edges stored should be outgoing from our provided seeds.
        operation.setIncludeIncomingOutGoing(IncludeIncomingOutgoingType.OUTGOING);
        final GetElementsInRangesHandler handler = new GetElementsInRangesHandler();
        final CloseableIterable<Element> rangeElements = handler.doOperation(operation, user, store);
        int count = 0;
        for (final Element elm : rangeElements) {
            //Make sure every element has been summarised
            assertEquals(9, elm.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER));
            count++;
        }
        assertEquals(1000, count);
        rangeElements.close();
        simpleEntityRanges.clear();
        //This should get everything between 0 and 0799 (again being string ordering 0800 is more than 08)
        simpleEntityRanges.add(new Pair<ElementSeed>(new EntitySeed("0"), new EntitySeed("08")));
        final CloseableIterable<Element> elements = handler.doOperation(operation, user, store);
        count = 0;
        for (final Element elm : elements) {
            //Make sure every element has been summarised
            assertEquals(9, elm.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER));
            count++;
        }
        assertEquals(800, count);
        elements.close();
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
        final View view = new View.Builder(defaultView)
                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                        .groupBy()
                        .build())
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .groupBy()
                        .build())
                .build();
        final GetElementsInRanges<Pair<ElementSeed>, Element> operation = new GetElementsInRanges<>(view, simpleEntityRanges);

        //All Edges stored should be outgoing from our provided seeds.
        operation.setIncludeIncomingOutGoing(IncludeIncomingOutgoingType.INCOMING);
        final GetElementsInRangesHandler handler = new GetElementsInRangesHandler();
        final CloseableIterable<Element> elements = handler.doOperation(operation, user, store);
        final int count = Iterables.size(elements);
        //There should be no incoming edges to the provided range
        assertEquals(0, count);
        elements.close();
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
        final View view = new View.Builder(defaultView)
                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                        .groupBy()
                        .build())
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .groupBy()
                        .build())
                .build();
        final GetElementsInRanges<Pair<ElementSeed>, Element> operation = new GetElementsInRanges<>(view, simpleEntityRanges);

        //All Edges stored should be outgoing from our provided seeds.
        operation.setIncludeEdges(IncludeEdgeType.UNDIRECTED);
        final GetElementsInRangesHandler handler = new GetElementsInRangesHandler();
        final CloseableIterable<Element> elements = handler.doOperation(operation, user, store);
        final int count = Iterables.size(elements);
        //There should be no incoming edges to the provided range
        assertEquals(0, count);
        elements.close();
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
