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

package uk.gov.gchq.gaffer.accumulostore.operation.handler;

import com.google.common.collect.Lists;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.SingleUseMockAccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.operation.impl.GetElementsInRanges;
import uk.gov.gchq.gaffer.accumulostore.utils.AccumuloPropertyNames;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.data.util.ElementUtil;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters.IncludeIncomingOutgoingType;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class GetElementsInRangesHandlerTest {
    private static final int NUM_ENTRIES = 1000;
    private static View defaultView;
    private static AccumuloStore byteEntityStore;
    private static AccumuloStore gaffer1KeyStore;
    private static final Schema SCHEMA = Schema.fromJson(StreamUtil.schemas(GetElementsInRangesHandlerTest.class));
    private static final AccumuloProperties PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.storeProps(GetElementsInRangesHandlerTest.class));
    private static final AccumuloProperties CLASSIC_PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.openStream(GetElementsInRangesHandlerTest.class, "/accumuloStoreClassicKeys.properties"));

    private static final Context CONTEXT = new Context();
    private OutputOperationHandler handler;

    @BeforeClass
    public static void setup() {
        byteEntityStore = new SingleUseMockAccumuloStore();
        gaffer1KeyStore = new SingleUseMockAccumuloStore();
    }

    @Before
    public void reInitialise() throws StoreException {
        handler = createHandler();
        defaultView = new View.Builder().edge(TestGroups.EDGE).entity(TestGroups.ENTITY).build();

        byteEntityStore.initialise("byteEntityGraph", SCHEMA, PROPERTIES);
        gaffer1KeyStore.initialise("gaffer1Graph", SCHEMA, CLASSIC_PROPERTIES);
        setupGraph(byteEntityStore, NUM_ENTRIES);
        setupGraph(gaffer1KeyStore, NUM_ENTRIES);
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
        // Given - everything between 0 and 1 (Note we are using strings and string serialisers, with this ordering 0999 is before 1)
        final Set<Pair<ElementId, ElementId>> simpleEntityRanges = new HashSet<>();
        simpleEntityRanges.add(new Pair<>(new EntitySeed("0"), new EntitySeed("1")));
        final Output operation = createOperation(simpleEntityRanges, defaultView, IncludeIncomingOutgoingType.EITHER, DirectedType.EITHER);

        // When
        List<Element> results = executeOperation(operation, store);

        // Then - each Edge was put in 3 times with different col qualifiers, without summarisation we expect this number
        ElementUtil.assertElementEquals(createElements(NUM_ENTRIES), results);

        // Given - this should get everything between 0 and 0799 (again being string ordering 0800 is more than 08)
        simpleEntityRanges.clear();
        simpleEntityRanges.add(new Pair<>(new EntitySeed("0"), new EntitySeed("08")));

        // When
        results = executeOperation(operation, store);

        // Then - each Edge was put in 3 times with different col qualifiers, without summarisation we expect this number
        ElementUtil.assertElementEquals(createElements(800), results);
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
        // Given - get everything between 0 and 1 (Note we are using strings and string serialisers, with this ordering 0999 is before 1)
        final Set<Pair<ElementId, ElementId>> simpleEntityRanges = new HashSet<>();
        simpleEntityRanges.add(new Pair<>(new EntitySeed("0"), new EntitySeed("1")));
        final View view = new View.Builder(defaultView)
                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                        .groupBy()
                        .build())
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .groupBy()
                        .build())
                .build();
        final Output operation = createOperation(simpleEntityRanges, view, IncludeIncomingOutgoingType.EITHER, DirectedType.EITHER);

        // When
        List<Element> results = executeOperation(operation, store);

        // Then
        ElementUtil.assertElementEquals(createSummarisedElements(NUM_ENTRIES), results);

        // Given - this should get everything between 0 and 0799 (again being string ordering 0800 is more than 08)
        simpleEntityRanges.clear();
        simpleEntityRanges.add(new Pair<>(new EntitySeed("0"), new EntitySeed("08")));

        // When
        results = executeOperation(operation, store);

        // Then
        ElementUtil.assertElementEquals(createSummarisedElements(800), results);
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
        // Given - get everything between 0 and 1 (Note we are using strings and string serialisers, with this ordering 0999 is before 1)
        final Set<Pair<ElementId, ElementId>> simpleEntityRanges = new HashSet<>();
        simpleEntityRanges.add(new Pair<>(new EntitySeed("0"), new EntitySeed("C")));
        final View view = new View.Builder(defaultView)
                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                        .groupBy()
                        .build())
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .groupBy()
                        .build())
                .build();
        // all Edges stored should be outgoing from our provided seeds.
        final Output operation = createOperation(simpleEntityRanges, view, IncludeIncomingOutgoingType.OUTGOING, DirectedType.EITHER);

        // When
        List<Element> results = executeOperation(operation, store);

        // Then
        ElementUtil.assertElementEquals(createSummarisedElements(NUM_ENTRIES), results);

        // Given - this should get everything between 0 and 0799 (again being string ordering 0800 is more than 08)
        simpleEntityRanges.clear();
        simpleEntityRanges.add(new Pair<>(new EntitySeed("0"), new EntitySeed("08")));

        // When
        results = executeOperation(operation, store);

        // Then
        ElementUtil.assertElementEquals(createSummarisedElements(800), results);
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
        // Given - get everything between 0 and 1 (Note we are using strings and string serialisers, with this ordering 0999 is before 1)
        final Set<Pair<ElementId, ElementId>> simpleEntityRanges = new HashSet<>();
        simpleEntityRanges.add(new Pair<>(new EntitySeed("0"), new EntitySeed("1")));
        final View view = new View.Builder(defaultView)
                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                        .groupBy()
                        .build())
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .groupBy()
                        .build())
                .build();
        // all Edges stored should be incoming from our provided seeds.
        final Output operation = createOperation(simpleEntityRanges, view, IncludeIncomingOutgoingType.INCOMING, DirectedType.EITHER);

        // When
        final List<Element> results = executeOperation(operation, store);

        // Then - should be no incoming edges in the provided range
        assertEquals(0, results.size());
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
        // Given - get everything between 0 and 1 (Note we are using strings and string serialisers, with this ordering 0999 is before 1)
        final Set<Pair<ElementId, ElementId>> simpleEntityRanges = new HashSet<>();
        simpleEntityRanges.add(new Pair<>(new EntitySeed("0"), new EntitySeed("1")));
        final View view = new View.Builder(defaultView)
                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                        .groupBy()
                        .build())
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .groupBy()
                        .build())
                .build();
        final Output operation = createOperation(simpleEntityRanges, view, IncludeIncomingOutgoingType.EITHER, DirectedType.UNDIRECTED);

        // When
        final List<Element> results = executeOperation(operation, store);

        // Then - there should be no undirected edges in the provided range
        assertEquals(0, results.size());
    }

    private static void setupGraph(final AccumuloStore store, final int numEntries) {
        final List<Element> elements = createElements(numEntries);

        try {
            store.execute(new AddElements.Builder().input(elements).build(), CONTEXT);
        } catch (final OperationException e) {
            fail("Couldn't add element: " + e);
        }
    }

    private static List<Element> createElements(final int numEntries) {
        final List<Element> elements = new ArrayList<>();
        for (int i = 0; i < numEntries; i++) {

            String s = "" + i;
            while (s.length() < 4) {
                s = "0" + s;
            }

            elements.add(new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source(s)
                    .dest("B")
                    .directed(true)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 1)
                    .build()
            );

            elements.add(new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source(s)
                    .dest("B")
                    .directed(true)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 3)
                    .build()
            );

            elements.add(new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source(s)
                    .dest("B")
                    .directed(true)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 5)
                    .build()
            );
        }
        return elements;
    }

    private static List<Element> createSummarisedElements(final int numEntries) {
        final List<Element> elements = new ArrayList<>();
        for (int i = 0; i < numEntries; i++) {

            String s = "" + i;
            while (s.length() < 4) {
                s = "0" + s;
            }

            elements.add(new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source(s)
                    .dest("B")
                    .directed(true)
                    .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 9)
                    .build()
            );
        }
        return elements;
    }

    protected OutputOperationHandler createHandler() {
        return new GetElementsInRangesHandler();
    }

    protected List<Element> executeOperation(final Output operation, final AccumuloStore store) throws OperationException {
        final Object results = handler.doOperation(operation, CONTEXT, store);
        return parseResults(results);
    }

    protected List<Element> parseResults(final Object results) {
        return Lists.newArrayList((Iterable) results);
    }

    protected Output createOperation(final Set<Pair<ElementId, ElementId>> simpleEntityRanges, final View view, final IncludeIncomingOutgoingType inOutType, final DirectedType directedType) {
        return new GetElementsInRanges.Builder()
                .input(simpleEntityRanges)
                .view(view)
                .directedType(directedType)
                .inOutType(inOutType)
                .build();
    }
}
