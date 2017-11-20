/*
 * Copyright 2017 Crown Copyright
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

package uk.gov.gchq.gaffer.store.operation.handler;

import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestTypes;
import uk.gov.gchq.gaffer.commonutil.exception.LimitExceededException;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.SampleElementsForSplitPoints;
import uk.gov.gchq.gaffer.serialisation.implementation.StringSerialiser;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class AbstractSampleElementsForSplitPointsHandlerTest<S extends Store> {
    protected Schema schema = new Schema.Builder()
            .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                    .vertex(TestTypes.ID_STRING)
                    .build())
            .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                    .source(TestTypes.ID_STRING)
                    .destination(TestTypes.ID_STRING)
                    .directed(TestTypes.DIRECTED_EITHER)
                    .build())
            .type(TestTypes.ID_STRING, new TypeDefinition.Builder()
                    .clazz(String.class)
                    .serialiser(new StringSerialiser())
                    .build())
            .type(TestTypes.DIRECTED_EITHER, Boolean.class)
            .vertexSerialiser(new StringSerialiser())
            .build();

    @Test
    public void shouldThrowExceptionForNullInput() throws OperationException {
        // Given
        final AbstractSampleElementsForSplitPointsHandler<?, S> handler = createHandler();
        final SampleElementsForSplitPoints operation = new SampleElementsForSplitPoints.Builder<>()
                .numSplits(1)
                .build();

        // When / Then
        try {

            handler.doOperation(operation, new Context(), createStore());
            fail("Exception expected");
        } catch (final OperationException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("input is required"));
        }
    }

    @Test
    public void shouldThrowExceptionIfNumSplitsIsNull() {
        // Given
        final AbstractSampleElementsForSplitPointsHandler<?, ?> handler = createHandler();
        final SampleElementsForSplitPoints operation = new SampleElementsForSplitPoints.Builder<>()
                .input(Collections.singletonList(new Entity(TestGroups.ENTITY, "vertex")))
                .numSplits(null)
                .build();

        // When / Then
        try {
            handler.doOperation(operation, new Context(), createStore());
            fail("Exception expected");
        } catch (final OperationException e) {
            assertTrue(e.getMessage(), e.getMessage().equals("Operation input is undefined - please specify an input."));
        }
    }

    @Test
    public void shouldThrowExceptionIfNumberOfSampledElementsIsMoreThanMaxAllowed() throws OperationException {
        // Given
        int maxSampledElements = 5;
        final AbstractSampleElementsForSplitPointsHandler<?, ?> handler = createHandler();
        handler.setMaxSampledElements(maxSampledElements);
        final List<Element> elements =
                IntStream.range(0, 6)
                        .mapToObj(i -> new Entity(TestGroups.ENTITY, "vertex_" + i))
                        .collect(Collectors.toList());
        final SampleElementsForSplitPoints operation = new SampleElementsForSplitPoints.Builder<>()
                .input(elements)
                .numSplits(3)
                .build();

        // When / Then
        try {
            handler.doOperation(operation, new Context(), createStore());
            fail("Exception expected");
        } catch (final LimitExceededException e) {
            assertTrue(e.getMessage(), e.getMessage().equals("Limit of " + maxSampledElements + " exceeded."));
        }
    }

    @Test
    public void shouldNotThrowExceptionIfNumberOfSampledElementsIsLessThanMaxAllowed() throws OperationException {
        // Given
        int maxSampledElements = 5;
        final AbstractSampleElementsForSplitPointsHandler<?, ?> handler = createHandler();
        handler.setMaxSampledElements(maxSampledElements);
        final List<Element> elements =
                IntStream.range(0, 5)
                        .mapToObj(i -> new Entity(TestGroups.ENTITY, "vertex_" + i))
                        .collect(Collectors.toList());
        elements.add(null);

        final SampleElementsForSplitPoints operation = new SampleElementsForSplitPoints.Builder<>()
                .input(elements)
                .numSplits(3)
                .build();

        // When
        handler.doOperation(operation, new Context(), createStore());

        // Then - no exception
    }

    @Test
    public void shouldReturnEmptyCollectionIfNumSplitsIsLessThan1() throws OperationException {
        // Given
        final AbstractSampleElementsForSplitPointsHandler<?, S> handler = createHandler();
        final SampleElementsForSplitPoints operation = new SampleElementsForSplitPoints.Builder<>()
                .input(new Entity(TestGroups.ENTITY, "vertex"))
                .numSplits(0)
                .build();

        // When
        final List<?> splits = handler.doOperation(operation, new Context(), createStore());

        // Then
        assertTrue(splits.isEmpty());
    }

    @Test
    public void shouldDeduplicateElements() throws OperationException {
        // Given
        final int numSplits = 3;
        final List<Element> elements = Collections.nCopies(10, new Entity(TestGroups.ENTITY, "vertex"));

        final AbstractSampleElementsForSplitPointsHandler<?, S> handler = createHandler();
        final SampleElementsForSplitPoints operation = new SampleElementsForSplitPoints.Builder<>()
                .input(elements)
                .numSplits(numSplits)
                .build();

        // When
        final List<?> splits = handler.doOperation(operation, new Context(), createStore());

        // Then
        assertEquals(1, splits.size());
        verifySplits(Collections.singletonList(0), elements, splits, handler);
    }

    @Test
    public void shouldUseFullSampleOfAllElementsByDefault() throws OperationException {
        // Given
        final int numSplits = 3;
        final List<Element> elements =
                IntStream.range(0, numSplits)
                        .mapToObj(i -> new Entity(TestGroups.ENTITY, "vertex_" + i))
                        .collect(Collectors.toList());

        final AbstractSampleElementsForSplitPointsHandler<?, S> handler = createHandler();
        final SampleElementsForSplitPoints operation = new SampleElementsForSplitPoints.Builder<>()
                .input(elements)
                .numSplits(numSplits)
                .build();

        // When
        final List<?> splits = handler.doOperation(operation, new Context(), createStore());

        // Then
        assertEquals(numSplits, splits.size());
        verifySplits(Arrays.asList(0, 1, 2), elements, splits, handler);
    }

    @Test
    public void shouldFilterOutNulls() throws OperationException {
        // Given
        final int numSplits = 3;
        final List<Element> elements =
                IntStream.range(0, numSplits)
                        .mapToObj(i -> new Entity(TestGroups.ENTITY, "vertex_" + i))
                        .collect(Collectors.toList());
        final List<Element> elementsWithNulls = new ArrayList<>();
        elementsWithNulls.add(null);
        elementsWithNulls.addAll(elements);
        elementsWithNulls.add(null);

        final AbstractSampleElementsForSplitPointsHandler<?, S> handler = createHandler();
        final SampleElementsForSplitPoints operation = new SampleElementsForSplitPoints.Builder<>()
                .input(elementsWithNulls)
                .numSplits(numSplits)
                .build();

        // When
        final List<?> splits = handler.doOperation(operation, new Context(), createStore());

        // Then
        assertEquals(numSplits, splits.size());
        verifySplits(Arrays.asList(0, 1, 2), elements, splits, handler);
    }

    @Test
    public void shouldSampleHalfOfElements() throws OperationException {
        // Given
        final int numSplits = 3;
        final List<Element> elements =
                IntStream.range(0, 1000 * numSplits)
                        .mapToObj(i -> new Entity(TestGroups.ENTITY, "vertex_" + i))
                        .collect(Collectors.toList());

        final AbstractSampleElementsForSplitPointsHandler<?, S> handler = createHandler();
        final SampleElementsForSplitPoints operation = new SampleElementsForSplitPoints.Builder<>()
                .input(elements)
                .numSplits(numSplits)
                .proportionToSample(0.5f)
                .build();

        // When
        final List<?> splits = handler.doOperation(operation, new Context(), createStore());

        // Then
        assertEquals(numSplits, splits.size());
    }

    @Test
    public void shouldCalculateRequiredNumberOfSplits() throws OperationException {
        // Given
        final int numSplits = 3;
        final List<Element> elements =
                IntStream.range(0, numSplits * 10)
                        .mapToObj(i -> new Entity(TestGroups.ENTITY, "vertex_" + i))
                        .collect(Collectors.toList());

        final AbstractSampleElementsForSplitPointsHandler<?, S> handler = createHandler();
        final SampleElementsForSplitPoints operation = new SampleElementsForSplitPoints.Builder<>()
                .input(elements)
                .numSplits(numSplits)
                .build();

        // When
        final List<?> splits = handler.doOperation(operation, new Context(), createStore());

        // Then
        verifySplits(Arrays.asList(6, 14, 21), elements, splits, handler);
    }

    protected abstract S createStore();

    protected abstract AbstractSampleElementsForSplitPointsHandler<?, S> createHandler();

    protected void verifySplits(final List<Integer> indexes, final List<Element> elements, final List<?> splits, final AbstractSampleElementsForSplitPointsHandler<?, S> handler) throws OperationException {
        final SampleElementsForSplitPoints operatation = new SampleElementsForSplitPoints.Builder<>()
                .input(elements)
                .numSplits(Integer.MAX_VALUE)
                .build();
        final List<?> allElementsAsSplits = handler.doOperation(operatation, new Context(), createStore());

        final List<Object> expectedSplits = indexes.stream()
                .map(allElementsAsSplits::get)
                .collect(Collectors.toList());

        assertEquals(expectedSplits, splits);
    }
}
