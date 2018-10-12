/*
 * Copyright 2017-2018. Crown Copyright
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
 * limitations under the License
 */

package uk.gov.gchq.gaffer.parquetstore.operation.handler;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestTypes;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.EmptyClosableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.SeedMatching;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.parquetstore.ParquetStoreProperties;
import uk.gov.gchq.gaffer.parquetstore.ParquetStorePropertiesTest;
import uk.gov.gchq.gaffer.parquetstore.testutils.TestUtils;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.impl.predicate.IsEqual;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.*;

public abstract class AbstractOperationsTest {
    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    protected static User USER = new User();

    protected abstract Schema getSchema();

    protected abstract List<Element> getInputDataForGetAllElementsTest();

    protected abstract List<ElementSeed> getSeeds();

    protected abstract List<ElementSeed> getSeedsThatWontAppear();

    protected abstract View getView();

    protected abstract List<Element> getResultsForGetAllElementsTest();

    protected abstract List<Element> getResultsForGetAllElementsWithViewTest();

    protected abstract List<Element> getResultsForGetAllElementsWithDirectedTypeTest();

    protected abstract List<Element> getResultsForGetAllElementsAfterTwoAdds();

    protected abstract List<Element> getResultsForGetElementsWithSeedsRelatedTest();

    protected abstract List<Element> getResultsForGetElementsWithSeedsEqualTest();

    protected abstract List<Element> getResultsForGetElementsWithSeedsAndViewTest();

    protected abstract List<Element> getResultsForGetElementsWithInOutTypeOutgoingTest();

    protected abstract List<Element> getResultsForGetElementsWithInOutTypeIncomingTest();

    protected abstract Edge getEdgeWithIdenticalSrcAndDst();

    protected Graph getGraph() throws IOException {
        return getGraph(TestUtils.getParquetStoreProperties(testFolder));
    }

    protected Graph getGraph(final ParquetStoreProperties storeProperties) {
        return new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("graphId")
                        .build())
                .addSchema(getSchema())
                .storeProperties(storeProperties)
                .build();
    }

    @Test
    public void getAllElementsTest() throws IOException, OperationException {
        // Given
        final Graph graph = getGraph();
        final List<Element> elements = getInputDataForGetAllElementsTest();
        graph.execute(new AddElements.Builder().input(elements).build(), USER);

        // When
        final CloseableIterable<? extends Element> results = graph.execute(
                new GetAllElements.Builder().build(), USER);

        // Then
        final List<Element> expected = getResultsForGetAllElementsTest();
        final List<Element> actual = StreamSupport.stream(results.spliterator(), false).collect(Collectors.toList());
        results.close();
        assertThat(expected, containsInAnyOrder(actual.toArray()));
    }

    @Test
    public void getAllElementsOnEmptyGraph() throws IOException, OperationException {
        // Given (test on a graph on which add has been called with an empty list and
        // on a graph on which add has never been called)
        final Graph graph1 = getGraph();
        final Graph graph2 = getGraph();
        final List<Element> elements = new ArrayList<>();
        graph1.execute(new AddElements.Builder().input(elements).build(), USER);

        // When
        final View view = getView();
        final CloseableIterable<? extends Element> results1 = graph1
                .execute(new GetAllElements.Builder().view(view).build(), USER);
        final CloseableIterable<? extends Element> results2 = graph2
                .execute(new GetAllElements.Builder().view(view).build(), USER);

        // Then
        assertFalse(results1.iterator().hasNext());
        assertFalse(results2.iterator().hasNext());
    }

    @Test
    public void getAllElementsWithViewTest() throws IOException, OperationException {
        // Given
        final Graph graph = getGraph();
        final List<Element> elements = getInputDataForGetAllElementsTest();
        graph.execute(new AddElements.Builder().input(elements).build(), USER);

        // When
        final View view = getView();
        final CloseableIterable<? extends Element> results = graph
                .execute(new GetAllElements.Builder().view(view).build(), USER);

        // Then
        final List<Element> expected = getResultsForGetAllElementsWithViewTest();
        final List<Element> actual = StreamSupport.stream(results.spliterator(), false).collect(Collectors.toList());
        results.close();
        assertThat(expected, containsInAnyOrder(actual.toArray()));
    }

    @Test
    public void getAllElementsWithDirectedTypeTest() throws IOException, OperationException {
        // Given
        final Graph graph = getGraph();
        final List<Element> elements = getInputDataForGetAllElementsTest();
        graph.execute(new AddElements.Builder().input(elements).build(), USER);

        // When
        final CloseableIterable<? extends Element> results = graph
                .execute(new GetAllElements.Builder().directedType(DirectedType.DIRECTED).build(), USER);

        // Then
        final List<Element> expected = getResultsForGetAllElementsWithDirectedTypeTest();
        final List<Element> actual = StreamSupport.stream(results.spliterator(), false).collect(Collectors.toList());
        results.close();
        assertThat(expected, containsInAnyOrder(actual.toArray()));
    }

    @Test
    public void getAllElementsAfterTwoAddElementsTest() throws IOException, OperationException {
        // Given
        final Graph graph = getGraph();
        final List<Element> elements = getInputDataForGetAllElementsTest();
        graph.execute(new AddElements.Builder().input(elements).build(), USER);
        graph.execute(new AddElements.Builder().input(elements).build(), USER);

        // When
        final CloseableIterable<? extends Element> results = graph.execute(new GetAllElements.Builder().build(), USER);

        // Then
        final List<Element> expected = getResultsForGetAllElementsAfterTwoAdds();
        final List<Element> actual = StreamSupport.stream(results.spliterator(), false).collect(Collectors.toList());
        results.close();
        assertThat(expected, containsInAnyOrder(actual.toArray()));
    }

    @Test
    public void getAllElementsAfterElementsAddedSeparatelyByGroup() throws IOException, OperationException {
        // Given
        final Graph graph = getGraph();
        final List<Element> elements = getInputDataForGetAllElementsTest();
        final List<Entity> entities = elements.stream().filter(e -> e instanceof Entity).map(e -> (Entity) e).collect(Collectors.toList());
        final List<Edge> edges = elements.stream().filter(e -> e instanceof Edge).map(e -> (Edge) e).collect(Collectors.toList());
        graph.execute(new AddElements.Builder().input(entities).build(), USER);
        graph.execute(new AddElements.Builder().input(edges).build(), USER);

        // When
        final CloseableIterable<? extends Element> results = graph.execute(new GetAllElements.Builder().build(), USER);

        // Then
        final List<Element> expected = getResultsForGetAllElementsTest();
        final List<Element> actual = StreamSupport.stream(results.spliterator(), false).collect(Collectors.toList());
        results.close();
        assertThat(expected, containsInAnyOrder(actual.toArray()));
    }

    @Test
    public void getAllElementsOnGraphRecreatedFromExistingGraph() throws IOException, OperationException {
        // Given
        final Graph graph = getGraph();
        final List<Element> elements = getInputDataForGetAllElementsTest();
        graph.execute(new AddElements.Builder().input(elements).build(), USER);

        // When
        final ParquetStoreProperties storeProperties = (ParquetStoreProperties) graph.getStoreProperties();
        final Graph graph2 = getGraph(storeProperties);
        final CloseableIterable<? extends Element> results = graph2.execute(
                new GetAllElements.Builder().build(), USER);

        // Then
        final List<Element> expected = getResultsForGetAllElementsTest();
        final List<Element> actual = StreamSupport.stream(results.spliterator(), false).collect(Collectors.toList());
        results.close();
        assertThat(expected, containsInAnyOrder(actual.toArray()));
    }

    @Test
    public void getElementsEmptySeedsTest() throws IOException, OperationException {
        // Given
        final Graph graph = getGraph();

        // When
        final CloseableIterable<? extends Element> results = graph
                .execute(new GetElements.Builder().input(new EmptyClosableIterable<>()).build(), USER);

        // Then
        assertFalse(results.iterator().hasNext());
        results.close();
    }

    @Test
    public void getElementsWithSeedsRelatedTest() throws IOException, OperationException {
        // Given
        final Graph graph = getGraph();
        final List<Element> elements = getInputDataForGetAllElementsTest();
        graph.execute(new AddElements.Builder().input(elements).build(), USER);

        // When
        final List<ElementSeed> seeds = getSeeds();
        final CloseableIterable<? extends Element> results = graph
                .execute(new GetElements.Builder()
                        .input(seeds)
                        .seedMatching(SeedMatching.SeedMatchingType.RELATED)
                        .build(), USER);

        // Then
        final List<Element> expected = getResultsForGetElementsWithSeedsRelatedTest();
        final List<Element> actual = StreamSupport.stream(results.spliterator(), false).collect(Collectors.toList());
        results.close();
        assertThat(expected, containsInAnyOrder(actual.toArray()));
    }

    @Test
    public void getElementsWithSeedsEqualTest() throws IOException, OperationException {
        // Given
        final Graph graph = getGraph();
        final List<Element> elements = getInputDataForGetAllElementsTest();
        graph.execute(new AddElements.Builder().input(elements).build(), USER);

        // When
        final List<ElementSeed> seeds = getSeeds();
        final CloseableIterable<? extends Element> results = graph
                .execute(new GetElements.Builder()
                        .input(seeds)
                        .seedMatching(SeedMatching.SeedMatchingType.EQUAL)
                        .build(), USER);

        // Then
        final List<Element> expected = getResultsForGetElementsWithSeedsEqualTest();
        final List<Element> actual = StreamSupport.stream(results.spliterator(), false).collect(Collectors.toList());
        results.close();
        assertThat(expected, containsInAnyOrder(actual.toArray()));
    }

    @Test
    public void getElementsWithMissingSeedsTest() throws IOException, OperationException {
        // Given
        final Graph graph = getGraph();
        final List<Element> elements = getInputDataForGetAllElementsTest();
        graph.execute(new AddElements.Builder().input(elements).build(), USER);

        // When
        final List<ElementSeed> seeds = getSeedsThatWontAppear();
        final CloseableIterable<? extends Element> results = graph
                .execute(new GetElements.Builder().input(seeds).seedMatching(SeedMatching.SeedMatchingType.EQUAL).build(), USER);

        // Then
        assertFalse(results.iterator().hasNext());
        results.close();
    }

    @Test
    public void getElementsWithSeedsAndViewTest() throws IOException, OperationException {
        // Given
        final Graph graph = getGraph();
        final List<Element> elements = getInputDataForGetAllElementsTest();
        graph.execute(new AddElements.Builder().input(elements).build(), USER);

        // When
        final List<ElementSeed> seeds = getSeeds();
        final View view = getView();
        final CloseableIterable<? extends Element> results = graph
                .execute(new GetElements.Builder().input(seeds).view(view).build(), USER);

        // Then
        final List<Element> expected = getResultsForGetElementsWithSeedsAndViewTest();
        final List<Element> actual = StreamSupport.stream(results.spliterator(), false).collect(Collectors.toList());
        results.close();
        assertThat(expected, containsInAnyOrder(actual.toArray()));
    }

    @Test
    public void getElementsWithPostAggregationFilterTest() throws IOException, OperationException {
        // Given
        final Graph graph = getGraph();
        final List<Element> elements = getInputDataForGetAllElementsTest();
        graph.execute(new AddElements.Builder().input(elements).build(), USER);
        final View view = new View.Builder().edge(TestGroups.EDGE,
                new ViewElementDefinition.Builder()
                        .postAggregationFilter(
                                new ElementFilter.Builder()
                                        .select("double")
                                        .execute(
                                                new IsEqual(2.0))
                                        .build())
                        .build())
                .build();

        // When / Then
        try {
            graph.execute(new GetElements.Builder().input(new EmptyClosableIterable<>()).view(view).build(), USER);
            fail("IllegalArgumentException Exception expected");
        } catch (final IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Operation chain"));
        } catch (final Exception e) {
            fail("IllegalArgumentException expected");
        }
    }

    @Test
    public void getElementsWithPostTransformFilterTest() throws IOException, OperationException {
        // Given
        final Graph graph = getGraph();
        final List<Element> elements = getInputDataForGetAllElementsTest();
        graph.execute(new AddElements.Builder().input(elements).build(), USER);
        final View view = new View.Builder().edge(TestGroups.EDGE,
                new ViewElementDefinition.Builder()
                        .postTransformFilter(
                                new ElementFilter.Builder()
                                        .select("double")
                                        .execute(
                                                new IsEqual(2.0))
                                        .build())
                        .build())
                .build();

        // When / Then
        try {
            graph.execute(new GetElements.Builder().input(new EmptyClosableIterable<>()).view(view).build(), USER);
            fail("IllegalArgumentException Exception expected");
        } catch (final IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Operation chain"));
        } catch (final Exception e) {
            fail("IllegalArgumentException expected");
        }
    }

    @Test
    public void getElementsWithInOutTypeTest() throws IOException, OperationException {
        // Given
        final Graph graph = getGraph();
        final List<Element> elements = getInputDataForGetAllElementsTest();
        graph.execute(new AddElements.Builder().input(elements).build(), USER);

        // When 1
        final List<ElementSeed> seeds = getSeeds().stream().filter(e -> e instanceof EntitySeed).collect(Collectors.toList());
        CloseableIterable<? extends Element> results = graph
                .execute(new GetElements.Builder()
                        .input(seeds)
                        .inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.OUTGOING)
                        .build(), USER);

        // Then 1
        List<Element> expected = getResultsForGetElementsWithInOutTypeOutgoingTest();
        List<Element> actual = StreamSupport.stream(results.spliterator(), false).collect(Collectors.toList());
        results.close();
        assertThat(expected, containsInAnyOrder(actual.toArray()));

        // When 2
        results = graph.execute(new GetElements.Builder()
                .input(seeds)
                .inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.INCOMING)
                .build(), USER);

        // Then 2
        expected = getResultsForGetElementsWithInOutTypeIncomingTest();
        actual = StreamSupport.stream(results.spliterator(), false).collect(Collectors.toList());
        results.close();
        assertThat(expected, containsInAnyOrder(actual.toArray()));
    }

    @Test
    public void deduplicateEdgeWhenSrcAndDstAreEqualTest() throws IOException, OperationException {
        // Given
        final Graph graph = getGraph();
        final Edge edge = getEdgeWithIdenticalSrcAndDst();
        graph.execute(new AddElements.Builder().input(edge).build(), USER);

        // When1
        CloseableIterable<? extends Element> results = graph.execute(
                new GetAllElements.Builder().build(), USER);

        // Then1
        Iterator<? extends Element> resultsIterator = results.iterator();
        assertTrue(resultsIterator.hasNext());
        assertEquals(edge, resultsIterator.next());
        assertFalse(resultsIterator.hasNext());
        results.close();

        // When2
        results = graph.execute(new GetElements.Builder().input(new EntitySeed(edge.getSource())).build(), USER);

        // Then2
        resultsIterator = results.iterator();
        assertTrue(resultsIterator.hasNext());
        assertEquals(edge, resultsIterator.next());
        assertFalse(resultsIterator.hasNext());
        results.close();
    }
}
