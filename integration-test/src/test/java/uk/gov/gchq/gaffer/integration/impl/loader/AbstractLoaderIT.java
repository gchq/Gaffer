/*
 * Copyright 2018 Crown Copyright
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

package uk.gov.gchq.gaffer.integration.impl.loader;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.integration.AbstractStoreWithCustomGraphIT;
import uk.gov.gchq.gaffer.integration.TraitRequirement;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.koryphe.impl.predicate.IsEqual;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;
import static uk.gov.gchq.gaffer.data.util.ElementUtil.assertElementEquals;

public abstract class AbstractLoaderIT<T extends Operation> extends AbstractStoreWithCustomGraphIT {

    protected Iterable<? extends Element> input;

    @Override
    public void setup() throws Exception {
        super.setup();
        input = getInputElements();
        configure(input);
    }

    @TraitRequirement(StoreTrait.INGEST_AGGREGATION)
    @Test
    public void shouldGetAllElements1() throws Exception {
        // Given
        createGraph(getSchema());

        // When
        addElements();

        // Then
        for (final boolean includeEntities : Arrays.asList(true, false)) {
            for (final boolean includeEdges : Arrays.asList(true, false)) {
                if (!includeEntities && !includeEdges) {
                    // Cannot query for nothing!
                    continue;
                }
                for (final DirectedType directedType : DirectedType.values()) {
                    try {
                        getAllElements(includeEntities, includeEdges, directedType);
                    } catch (final AssertionError e) {
                        throw new AssertionError("GetAllElements failed with parameters: includeEntities=" + includeEntities
                                + ", includeEdges=" + includeEdges + ", directedType=" + directedType.name(), e);
                    }
                }
            }
        }
    }

    @TraitRequirement({StoreTrait.PRE_AGGREGATION_FILTERING, StoreTrait.INGEST_AGGREGATION})
    @Test
    public void shouldGetAllElementsFilteredOnGroup() throws Exception {
        // Given
        createGraph(getSchema());

        // When
        addElements();

        // Then
        final GetAllElements op = new GetAllElements.Builder()
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY)
                        .build())
                .build();

        final CloseableIterable<? extends Element> results = graph.execute(op, getUser());

        final List<Element> resultList = Lists.newArrayList(results);
        Assert.assertEquals(getEntities().size(), resultList.size());
        for (final Element element : resultList) {
            Assert.assertEquals(TestGroups.ENTITY, element.getGroup());
        }
    }

    @TraitRequirement(StoreTrait.PRE_AGGREGATION_FILTERING)
    @Test
    public void shouldGetAllFilteredElements() throws Exception {
        // Given
        createGraph(getSchema());

        // When
        addElements();

        // Then
        final GetAllElements op = new GetAllElements.Builder()
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                                .preAggregationFilter(new ElementFilter.Builder()
                                        .select(IdentifierType.VERTEX.name())
                                        .execute(new IsEqual("A1"))
                                        .build())
                                .build())
                        .build())
                .build();

        final CloseableIterable<? extends Element> results = graph.execute(op, getUser());

        final List<Element> resultList = Lists.newArrayList(results);
        Assert.assertEquals(1, resultList.size());
        Assert.assertEquals("A1", ((Entity) resultList.get(0)).getVertex());
    }

    @Test
    public void shouldGetAllElementsWithProvidedProperties() throws Exception {
        // Given
        createGraph(getSchema());

        // When
        addElements();

        // Then
        final GetAllElements op = new GetAllElements.Builder()
                .view(new View.Builder()
                        .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                                .properties(TestPropertyNames.COUNT)
                                .build())
                        .build())
                .build();

        final CloseableIterable<? extends Element> results = graph.execute(op, getUser());

        for (final Element result : results) {
            Assert.assertEquals(1, result.getProperties().size());
            Assert.assertEquals(1L, result.getProperties().get(TestPropertyNames.COUNT));
        }
    }

    @Test
    public void shouldGetAllElementsWithExcludedProperties() throws Exception {
        // Given
        createGraph(getSchema());

        // When
        addElements();

        // Then
        final GetAllElements op = new GetAllElements.Builder()
                .view(new View.Builder()
                        .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                                .properties(TestPropertyNames.COUNT)
                                .build())
                        .build())
                .build();

        final CloseableIterable<? extends Element> results = graph.execute(op, getUser());

        for (final Element result : results) {
            Assert.assertEquals(1, result.getProperties().size());
            Assert.assertEquals(1L, result.getProperties().get(TestPropertyNames.COUNT));
        }
    }

    protected void getAllElements() throws Exception {
        for (final boolean includeEntities : Arrays.asList(true, false)) {
            for (final boolean includeEdges : Arrays.asList(true, false)) {
                if (!includeEntities && !includeEdges) {
                    // Cannot query for nothing!
                    continue;
                }
                for (final DirectedType directedType : DirectedType.values()) {
                    try {
                        getAllElements(includeEntities, includeEdges, directedType);
                    } catch (final AssertionError e) {
                        throw new AssertionError("GetAllElements failed with parameters: includeEntities=" + includeEntities
                                + ", includeEdges=" + includeEdges + ", directedType=" + directedType.name(), e);
                    }
                }
            }
        }
    }

    private void getAllElements(final boolean includeEntities, final boolean includeEdges, final DirectedType directedType) throws Exception {
        // Given
        final List<Element> expectedElements = new ArrayList<>();
        if (includeEntities) {
            expectedElements.addAll(getEntities().values());
        }

        if (includeEdges) {
            for (final Edge edge : getEdges().values()) {
                if (DirectedType.EITHER == directedType
                        || (edge.isDirected() && DirectedType.DIRECTED == directedType)
                        || (!edge.isDirected() && DirectedType.UNDIRECTED == directedType)) {
                    expectedElements.add(edge);
                }
            }
        }

        final View.Builder viewBuilder = new View.Builder();
        if (includeEntities) {
            viewBuilder.entity(TestGroups.ENTITY);
        }
        if (includeEdges) {
            viewBuilder.edge(TestGroups.EDGE);
        }
        final GetAllElements op = new GetAllElements.Builder()
                .directedType(directedType)
                .view(viewBuilder.build())
                .build();

        // When
        final CloseableIterable<? extends Element> results = graph.execute(op, getUser());

        // Then
        assertElementEquals(expectedElements, results);
    }

    protected void addElements() throws OperationException {
        graph.execute(createOperation(input), getUser());
    }

    protected Iterable<? extends Element> getInputElements() {
        final Iterable<? extends Element> edges = getEdges().values();
        final Iterable<? extends Element> entities = getEntities().values();

        return Iterables.concat(edges, entities);
    }

    protected abstract void configure(final Iterable<? extends Element> elements) throws Exception ;

    protected abstract T createOperation(final Iterable<? extends Element> elements);

    protected abstract Schema getSchema();
}
