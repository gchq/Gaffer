/*
 * Copyright 2017-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.parquetstore.operation.handler;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.EmptyClosableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.data.util.ElementUtil;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.integration.StandaloneIT;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.SeedMatching;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.parquetstore.ParquetStoreProperties;
import uk.gov.gchq.gaffer.parquetstore.testutils.TestUtils;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.impl.predicate.IsEqual;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class AbstractOperationsTest extends StandaloneIT {
    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    protected User user = getUser();

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

    @Override
    public User getUser() {
        return new User();
    }

    @Override
    public StoreProperties createStoreProperties() {
        try {
            return TestUtils.getParquetStoreProperties(testFolder);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void shouldGetAllElementsTest() throws OperationException {
        // Given
        final Graph graph = createGraph();
        final List<Element> elements = getInputDataForGetAllElementsTest();
        graph.execute(new AddElements.Builder().input(elements).build(), user);

        // When
        final CloseableIterable<? extends Element> results = graph.execute(
                new GetAllElements.Builder().build(), user);

        // Then
        ElementUtil.assertElementEquals(getResultsForGetAllElementsTest(), results);
    }

    @Test
    public void shouldGetNoResultsFromGetAllElementsOnEmptyGraph() throws OperationException {
        // Given (test on a graph on which add has been called with an empty list and
        // on a graph on which add has never been called)
        final Graph graph1 = createGraph();
        final Graph graph2 = createGraph();
        final List<Element> elements = new ArrayList<>();
        graph1.execute(new AddElements.Builder().input(elements).build(), user);

        // When
        final View view = getView();
        final CloseableIterable<? extends Element> results1 = graph1
                .execute(new GetAllElements.Builder().view(view).build(), user);
        final CloseableIterable<? extends Element> results2 = graph2
                .execute(new GetAllElements.Builder().view(view).build(), user);

        // Then
        assertFalse(results1.iterator().hasNext());
        assertFalse(results2.iterator().hasNext());
    }

    @Test
    public void shouldGetAllElementsWithViewTest() throws OperationException {
        // Given
        final Graph graph = createGraph();
        final List<Element> elements = getInputDataForGetAllElementsTest();
        graph.execute(new AddElements.Builder().input(elements).build(), user);

        // When
        final View view = getView();
        final CloseableIterable<? extends Element> results = graph
                .execute(new GetAllElements.Builder().view(view).build(), user);

        // Then
        ElementUtil.assertElementEquals(getResultsForGetAllElementsWithViewTest(), results);
    }

    @Test
    public void shouldGetAllElementsWithDirectedTypeTest() throws OperationException {
        // Given
        final Graph graph = createGraph();
        final List<Element> elements = getInputDataForGetAllElementsTest();
        graph.execute(new AddElements.Builder().input(elements).build(), user);

        // When
        final CloseableIterable<? extends Element> results = graph
                .execute(new GetAllElements.Builder().directedType(DirectedType.DIRECTED).build(), user);

        // Then
        ElementUtil.assertElementEquals(getResultsForGetAllElementsWithDirectedTypeTest(), results);
    }

    @Test
    public void shouldGetAllElementsAfterTwoAddElementsTest() throws OperationException {
        // Given
        final Graph graph = createGraph();
        final List<Element> elements = getInputDataForGetAllElementsTest();
        graph.execute(new AddElements.Builder().input(elements).build(), user);
        graph.execute(new AddElements.Builder().input(elements).build(), user);

        // When
        final CloseableIterable<? extends Element> results = graph.execute(new GetAllElements.Builder().build(), user);

        // Then
        ElementUtil.assertElementEquals(getResultsForGetAllElementsAfterTwoAdds(), results);
    }

    @Test
    public void shouldGetAllElementsAfterElementsAddedSeparatelyByGroup() throws OperationException {
        // Given
        final Graph graph = createGraph();
        final List<Element> elements = getInputDataForGetAllElementsTest();
        final List<Entity> entities = elements.stream().filter(e -> e instanceof Entity).map(e -> (Entity) e).collect(Collectors.toList());
        final List<Edge> edges = elements.stream().filter(e -> e instanceof Edge).map(e -> (Edge) e).collect(Collectors.toList());
        graph.execute(new AddElements.Builder().input(entities).build(), user);
        graph.execute(new AddElements.Builder().input(edges).build(), user);

        // When
        final CloseableIterable<? extends Element> results = graph.execute(new GetAllElements.Builder().build(), user);

        // Then
        ElementUtil.assertElementEquals(getResultsForGetAllElementsTest(), results);
    }

    @Test
    public void shouldGetAllElementsOnGraphRecreatedFromExistingGraph() throws OperationException {
        // Given
        final Graph graph = createGraph();
        final List<Element> elements = getInputDataForGetAllElementsTest();
        graph.execute(new AddElements.Builder().input(elements).build(), user);

        // When
        final ParquetStoreProperties storeProperties = (ParquetStoreProperties) graph.getStoreProperties();
        final Graph graph2 = createGraph(storeProperties);
        final CloseableIterable<? extends Element> results = graph2.execute(
                new GetAllElements.Builder().build(), user);

        // Then
        ElementUtil.assertElementEquals(getResultsForGetAllElementsTest(), results);
    }

    @Test
    public void shouldNotGetElementsOnEmptyGraph() throws OperationException {
        // Given (test on a graph on which add has been called with an empty list and
        // on a graph on which add has never been called)
        final Graph graph1 = createGraph();
        final Graph graph2 = createGraph();
        final List<Element> elements = new ArrayList<>();
        graph1.execute(new AddElements.Builder().input(elements).build(), user);

        // When
        final CloseableIterable<? extends Element> results1 = graph1
                .execute(new GetElements.Builder().input(getSeeds()).build(), user);
        final CloseableIterable<? extends Element> results2 = graph2
                .execute(new GetElements.Builder().input(getSeeds()).build(), user);

        // Then
        assertFalse(results1.iterator().hasNext());
        assertFalse(results2.iterator().hasNext());
    }

    @Test
    public void shouldNotGetElementsWithEmptySeedsTest() throws OperationException {
        // Given
        final Graph graph = createGraph();

        // When
        final CloseableIterable<? extends Element> results = graph
                .execute(new GetElements.Builder().input(new EmptyClosableIterable<>()).build(), user);

        // Then
        assertFalse(results.iterator().hasNext());
    }

    @Test
    public void shouldGetElementsWithSeedsRelatedTest() throws OperationException {
        // Given
        final Graph graph = createGraph();
        final List<Element> elements = getInputDataForGetAllElementsTest();
        graph.execute(new AddElements.Builder().input(elements).build(), user);

        // When
        final List<ElementSeed> seeds = getSeeds();
        final CloseableIterable<? extends Element> results = graph
                .execute(new GetElements.Builder()
                        .input(seeds)
                        .seedMatching(SeedMatching.SeedMatchingType.RELATED)
                        .build(), user);

        // Then
        ElementUtil.assertElementEquals(getResultsForGetElementsWithSeedsRelatedTest(), results);
    }

    @Test
    public void shouldGetElementsWithSeedsEqualTest() throws OperationException {
        // Given
        final Graph graph = createGraph();
        final List<Element> elements = getInputDataForGetAllElementsTest();
        graph.execute(new AddElements.Builder().input(elements).build(), user);

        // When
        final List<ElementSeed> seeds = getSeeds();
        final CloseableIterable<? extends Element> results = graph
                .execute(new GetElements.Builder()
                        .input(seeds)
                        .seedMatching(SeedMatching.SeedMatchingType.EQUAL)
                        .build(), user);

        // Then
        ElementUtil.assertElementEquals(getResultsForGetElementsWithSeedsEqualTest(), results);
    }

    @Test
    public void shouldNotGetElementsWithMissingSeedsTest() throws OperationException {
        // Given
        final Graph graph = createGraph();
        final List<Element> elements = getInputDataForGetAllElementsTest();
        graph.execute(new AddElements.Builder().input(elements).build(), user);

        // When
        final List<ElementSeed> seeds = getSeedsThatWontAppear();
        final CloseableIterable<? extends Element> results = graph
                .execute(new GetElements.Builder().input(seeds).seedMatching(SeedMatching.SeedMatchingType.EQUAL).build(), user);

        // Then
        assertFalse(results.iterator().hasNext());
    }

    @Test
    public void shouldGetElementsWithSeedsAndViewTest() throws OperationException {
        // Given
        final Graph graph = createGraph();
        final List<Element> elements = getInputDataForGetAllElementsTest();
        graph.execute(new AddElements.Builder().input(elements).build(), user);

        // When
        final List<ElementSeed> seeds = getSeeds();
        final View view = getView();
        final CloseableIterable<? extends Element> results = graph
                .execute(new GetElements.Builder().input(seeds).view(view).build(), user);

        // Then
        ElementUtil.assertElementEquals(getResultsForGetElementsWithSeedsAndViewTest(), results);
    }

    @Test
    public void shouldThrowUnsupportedTraitExceptionWithPostAggregationFiltering() throws OperationException {
        // Given
        final Graph graph = createGraph();
        final List<Element> elements = getInputDataForGetAllElementsTest();
        graph.execute(new AddElements.Builder().input(elements).build(), user);
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
            graph.execute(new GetElements.Builder().input(new EmptyClosableIterable<>()).view(view).build(), user);
            fail("IllegalArgumentException Exception: POST_AGGREGATION_FILTERING expected");
        } catch (final IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("POST_AGGREGATION_FILTERING"));
        }
    }

    @Test
    public void shouldThrowUnsupportedTraitExceptionWithPostTransformFiltering() throws OperationException {
        // Given
        final Graph graph = createGraph();
        final List<Element> elements = getInputDataForGetAllElementsTest();
        graph.execute(new AddElements.Builder().input(elements).build(), user);
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
            graph.execute(new GetElements.Builder().input(new EmptyClosableIterable<>()).view(view).build(), user);
            fail("IllegalArgumentException Exception expected");
        } catch (final IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("POST_TRANSFORMATION_FILTERING"));
        }
    }

    @Test
    public void shouldGetElementsWithInOutTypeTest() throws OperationException {
        // Given
        final Graph graph = createGraph();
        final List<Element> elements = getInputDataForGetAllElementsTest();
        graph.execute(new AddElements.Builder().input(elements).build(), user);

        // When 1
        final List<ElementSeed> seeds = getSeeds().stream().filter(e -> e instanceof EntitySeed).collect(Collectors.toList());
        CloseableIterable<? extends Element> results = graph
                .execute(new GetElements.Builder()
                        .input(seeds)
                        .inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.OUTGOING)
                        .build(), user);

        // Then 1
        ElementUtil.assertElementEquals(getResultsForGetElementsWithInOutTypeOutgoingTest(), results);

        // When 2
        results = graph.execute(new GetElements.Builder()
                .input(seeds)
                .inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.INCOMING)
                .build(), user);

        // Then 2
        ElementUtil.assertElementEquals(getResultsForGetElementsWithInOutTypeIncomingTest(), results);
    }

    @Test
    public void shouldDeduplicateEdgeWhenSrcAndDstAreEqualTest() throws OperationException {
        // Given
        final Graph graph = createGraph();
        final Edge edge = getEdgeWithIdenticalSrcAndDst();
        graph.execute(new AddElements.Builder().input(edge).build(), user);

        // When1
        CloseableIterable<? extends Element> results = graph.execute(
                new GetAllElements.Builder().build(), user);

        // Then1
        Iterator<? extends Element> resultsIterator = results.iterator();
        assertTrue(resultsIterator.hasNext());
        assertEquals(edge, resultsIterator.next());
        assertFalse(resultsIterator.hasNext());
        results.close();

        // When2
        results = graph.execute(new GetElements.Builder().input(new EntitySeed(edge.getSource())).build(), user);

        // Then2
        resultsIterator = results.iterator();
        assertTrue(resultsIterator.hasNext());
        assertEquals(edge, resultsIterator.next());
        assertFalse(resultsIterator.hasNext());
        results.close();
    }
}
