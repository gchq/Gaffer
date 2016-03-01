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

import gaffer.accumulostore.AccumuloStore;
import gaffer.accumulostore.MockAccumuloStoreForTest;
import gaffer.accumulostore.key.core.impl.byteEntity.ByteEntityKeyPackage;
import gaffer.accumulostore.key.core.impl.classic.ClassicKeyPackage;
import gaffer.accumulostore.operation.impl.GetElementsInRanges;
import gaffer.accumulostore.utils.AccumuloPropertyNames;
import gaffer.accumulostore.utils.Pair;
import gaffer.commonutil.TestGroups;
import gaffer.data.element.Edge;
import gaffer.data.element.Element;
import gaffer.data.elementdefinition.view.View;
import gaffer.data.elementdefinition.view.ViewEdgeDefinition;
import gaffer.data.elementdefinition.view.ViewEntityDefinition;
import gaffer.operation.GetOperation.IncludeEdgeType;
import gaffer.operation.GetOperation.IncludeIncomingOutgoingType;
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

public class GetElementsinRangesHandlerTest {

    private static View defaultView;
    private static AccumuloStore byteEntityStore;
    private static AccumuloStore gaffer1KeyStore;

    @BeforeClass
    public static void setup() throws StoreException, IOException {
        byteEntityStore = new MockAccumuloStoreForTest(ByteEntityKeyPackage.class);
        gaffer1KeyStore = new MockAccumuloStoreForTest(ClassicKeyPackage.class);
        defaultView = new View.Builder().edge(TestGroups.EDGE, new ViewEdgeDefinition()).entity(TestGroups.ENTITY, new ViewEntityDefinition()).build();
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
    public void testNoSummarisation() throws OperationException {
        testNoSummarisation(byteEntityStore);
        testNoSummarisation(gaffer1KeyStore);
    }

    public void testNoSummarisation(final AccumuloStore store) throws OperationException {
        // Create set to query for
        Set<Pair<ElementSeed>> simpleEntityRanges = new HashSet<>();

        //get Everything between 0 and 1 (Note we are using strings and string serialisers, with this ordering 0999 is before 1)
        simpleEntityRanges.add(new Pair<ElementSeed>(new EntitySeed("0"), new EntitySeed("1")));
        GetElementsInRanges<ElementSeed, Element> operation = new GetElementsInRanges<>(defaultView, simpleEntityRanges);

        GetElementsInRangesHandler handler = new GetElementsInRangesHandler();
        Iterable<Element> elements = handler.doOperation(operation, store);
        int count = 0;
        for (@SuppressWarnings("unused") Element elm : elements) {
            count++;
        }
        //Each Edge was put in 3 times with different col qualifiers, without summarisation we expect this number
        assertEquals(1000 * 3, count);

        simpleEntityRanges.clear();
        //This should get everything between 0 and 0799 (again being string ordering 0800 is more than 08)
        simpleEntityRanges.add(new Pair<ElementSeed>(new EntitySeed("0"), new EntitySeed("08")));
        elements = handler.doOperation(operation, store);
        count = 0;
        for (@SuppressWarnings("unused") Element elm : elements) {
            count++;
        }
        //Each Edge was put in 3 times with different col qualifiers, without summarisation we expect this number
        assertEquals(800 * 3, count);

    }

    @Test
    public void testShouldSummarise() throws OperationException {
        testShouldSummarise(byteEntityStore);
        testShouldSummarise(gaffer1KeyStore);
    }

    public void testShouldSummarise(final AccumuloStore store) throws OperationException {
        // Create set to query for
        Set<Pair<ElementSeed>> simpleEntityRanges = new HashSet<>();

        //get Everything between 0 and 1 (Note we are using strings and string serialisers, with this ordering 0999 is before 1)
        simpleEntityRanges.add(new Pair<ElementSeed>(new EntitySeed("0"), new EntitySeed("1")));
        GetElementsInRanges<ElementSeed, Element> operation = new GetElementsInRanges<>(defaultView, simpleEntityRanges);
        operation.setSummarise(true);
        GetElementsInRangesHandler handler = new GetElementsInRangesHandler();
        Iterable<Element> elements = handler.doOperation(operation, store);
        int count = 0;
        for (Element elm : elements) {
            elm.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER);
            //Make sure every element has been summarised
            assertEquals(9, elm.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER));
            count++;
        }
        assertEquals(1000, count);

        simpleEntityRanges.clear();
        //This should get everything between 0 and 0799 (again being string ordering 0800 is more than 08)
        simpleEntityRanges.add(new Pair<ElementSeed>(new EntitySeed("0"), new EntitySeed("08")));
        elements = handler.doOperation(operation, store);
        count = 0;
        for (Element elm : elements) {
            elm.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER);
            //Make sure every element has been summarised
            assertEquals(9, elm.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER));
            count++;
        }
        assertEquals(800, count);

    }

    @Test
    public void testShouldSummariseOutGoingEdgesOnly() throws OperationException {
        testShouldSummariseOutGoingEdgesOnly(byteEntityStore);
        testShouldSummariseOutGoingEdgesOnly(gaffer1KeyStore);
    }

    public void testShouldSummariseOutGoingEdgesOnly(final AccumuloStore store) throws OperationException {
        // Create set to query for
        Set<Pair<ElementSeed>> simpleEntityRanges = new HashSet<>();

        //get Everything between 0 and 1 (Note we are using strings and string serialisers, with this ordering 0999 is before 1)
        simpleEntityRanges.add(new Pair<ElementSeed>(new EntitySeed("0"), new EntitySeed("C")));
        GetElementsInRanges<ElementSeed, Element> operation = new GetElementsInRanges<>(defaultView, simpleEntityRanges);
        operation.setSummarise(true);

        //All Edges stored should be outgoing from our provided seeds.
        operation.setIncludeIncomingOutGoing(IncludeIncomingOutgoingType.OUTGOING);
        GetElementsInRangesHandler handler = new GetElementsInRangesHandler();
        Iterable<Element> elements = handler.doOperation(operation, store);
        int count = 0;
        for (Element elm : elements) {
            elm.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER);
            //Make sure every element has been summarised
            assertEquals(9, elm.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER));
            count++;
        }
        assertEquals(1000, count);

        simpleEntityRanges.clear();
        //This should get everything between 0 and 0799 (again being string ordering 0800 is more than 08)
        simpleEntityRanges.add(new Pair<ElementSeed>(new EntitySeed("0"), new EntitySeed("08")));
        elements = handler.doOperation(operation, store);
        count = 0;
        for (Element elm : elements) {
            elm.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER);
            //Make sure every element has been summarised
            assertEquals(9, elm.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER));
            count++;
        }
        assertEquals(800, count);

    }

    @Test
    public void testShouldHaveNoIncomingEdges() throws OperationException {
        testShouldHaveNoIncomingEdges(byteEntityStore);
        testShouldHaveNoIncomingEdges(gaffer1KeyStore);
    }

    public void testShouldHaveNoIncomingEdges(final AccumuloStore store) throws OperationException {
        // Create set to query for
        Set<Pair<ElementSeed>> simpleEntityRanges = new HashSet<>();

        //get Everything between 0 and 1 (Note we are using strings and string serialisers, with this ordering 0999 is before 1)
        simpleEntityRanges.add(new Pair<ElementSeed>(new EntitySeed("0"), new EntitySeed("1")));
        GetElementsInRanges<ElementSeed, Element> operation = new GetElementsInRanges<>(defaultView, simpleEntityRanges);

        //All Edges stored should be outgoing from our provided seeds.
        operation.setIncludeIncomingOutGoing(IncludeIncomingOutgoingType.INCOMING);
        operation.setSummarise(true);
        GetElementsInRangesHandler handler = new GetElementsInRangesHandler();
        Iterable<Element> elements = handler.doOperation(operation, store);
        int count = 0;
        for (@SuppressWarnings("unused") Element elm : elements) {
            count++;
        }
        //There should be no incoming edges to the provided range
        assertEquals(0, count);
    }

    @Test
    public void testShouldReturnNothingWhenNoEdgesSet() throws OperationException {
        testShouldReturnNothingWhenNoEdgesSet(byteEntityStore);
        testShouldReturnNothingWhenNoEdgesSet(gaffer1KeyStore);
    }

    public void testShouldReturnNothingWhenNoEdgesSet(final AccumuloStore store) throws OperationException {
        // Create set to query for
        Set<Pair<ElementSeed>> simpleEntityRanges = new HashSet<>();

        //get Everything between 0 and 1 (Note we are using strings and string serialisers, with this ordering 0999 is before 1)
        simpleEntityRanges.add(new Pair<ElementSeed>(new EntitySeed("0"), new EntitySeed("1")));
        GetElementsInRanges<ElementSeed, Element> operation = new GetElementsInRanges<>(defaultView, simpleEntityRanges);

        //All Edges stored should be outgoing from our provided seeds.
        operation.setIncludeEdges(IncludeEdgeType.UNDIRECTED);
        operation.setSummarise(true);
        GetElementsInRangesHandler handler = new GetElementsInRangesHandler();
        Iterable<Element> elements = handler.doOperation(operation, store);
        int count = 0;
        for (@SuppressWarnings("unused") Element elm : elements) {
            count++;
        }
        //There should be no incoming edges to the provided range
        assertEquals(0, count);

    }

    private static void setupGraph(final AccumuloStore store, int numEntries) {
        List<Element> elements = new ArrayList<>();
        for (int i = 0; i < numEntries; i++) {
            Edge edge = new Edge(TestGroups.EDGE);
            Edge edge2 = new Edge(TestGroups.EDGE);
            Edge edge3 = new Edge(TestGroups.EDGE);

            String s = "" + i;
            while (s.length() < 4) {
                s = "0" + s;
            }
            edge.setSource(s);
            edge2.setSource(s);
            edge3.setSource(s);
            edge.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 1);
            edge.setDestination("B");
            edge.setDirected(true);
            edge2.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 3);
            edge2.setDestination("B");
            edge2.setDirected(true);
            edge3.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 5);
            edge3.setDestination("B");
            edge3.setDirected(true);

            elements.add(edge);
            elements.add(edge2);
            elements.add(edge3);
        }

        try {
            store.execute(new AddElements(elements));
        } catch (OperationException e) {
            fail("Couldn't add element: " + e);
        }
    }

}
