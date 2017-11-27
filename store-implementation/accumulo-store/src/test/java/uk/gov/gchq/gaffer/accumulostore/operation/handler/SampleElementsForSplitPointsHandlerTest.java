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

package uk.gov.gchq.gaffer.accumulostore.operation.handler;

import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.key.AccumuloElementConverter;
import uk.gov.gchq.gaffer.accumulostore.key.AccumuloKeyPackage;
import uk.gov.gchq.gaffer.accumulostore.key.core.impl.byteEntity.ByteEntityAccumuloElementConverter;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.SampleElementsForSplitPoints;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.operation.handler.AbstractSampleElementsForSplitPointsHandler;
import uk.gov.gchq.gaffer.store.operation.handler.AbstractSampleElementsForSplitPointsHandlerTest;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class SampleElementsForSplitPointsHandlerTest extends AbstractSampleElementsForSplitPointsHandlerTest<AccumuloStore> {
    public static final int NUM_TABLET_SERVERS = 4;

    private AccumuloStore store;

    @Before
    public void before() throws StoreException {
        store = mock(AccumuloStore.class);
        final AccumuloKeyPackage keyPackage = mock(AccumuloKeyPackage.class);
        final AccumuloElementConverter converter = new ByteEntityAccumuloElementConverter(schema);
        final List<String> tabletServers = Collections.nCopies(NUM_TABLET_SERVERS, null);

        given(store.getKeyPackage()).willReturn(keyPackage);
        given(keyPackage.getKeyConverter()).willReturn(converter);
        given(store.getTabletServers()).willReturn(tabletServers);
    }

    @Override
    public void shouldThrowExceptionIfNumSplitsIsNull() {
        // Num splits can never be null
    }

    @Test
    public void shouldUseTheNumberOfTabletServersToCalculateNumSplits() throws OperationException {
        // Given
        final Integer numSplits = null;
        final List<Element> elements = IntStream.range(0, 30)
                .mapToObj(i -> new Edge(TestGroups.EDGE, "source_" + i, "dest_" + i, true))
                .collect(Collectors.toList());

        final AbstractSampleElementsForSplitPointsHandler<String, AccumuloStore> handler = createHandler();
        final SampleElementsForSplitPoints<String> operation = new SampleElementsForSplitPoints.Builder<String>()
                .input(elements)
                .numSplits(numSplits)
                .build();

        // When
        final List<String> splits = createHandler().doOperation(operation, new Context(), createStore());

        // Then
        assertEquals(NUM_TABLET_SERVERS, splits.size() + 1);
        verifySplits(Arrays.asList(14, 29, 44), elements, splits, handler);
    }

    @Test
    public void shouldCalculateRequiredNumberOfSplitsFromEdges() throws OperationException {
        // Given
        final int numSplits = 3;
        final List<Element> elements = IntStream.range(0, numSplits * 10)
                .mapToObj(i -> new Edge(TestGroups.EDGE, "source_" + i, "dest_" + i, true))
                .collect(Collectors.toList());

        final AbstractSampleElementsForSplitPointsHandler<String, AccumuloStore> handler = createHandler();
        final SampleElementsForSplitPoints<String> operation = new SampleElementsForSplitPoints.Builder<String>()
                .input(elements)
                .numSplits(numSplits)
                .build();

        // When
        final List<String> splits = createHandler().doOperation(operation, new Context(), createStore());

        // Then
        verifySplits(Arrays.asList(14, 29, 44), elements, splits, handler);
    }

    @Override
    protected AccumuloStore createStore() {
        return store;
    }

    @Override
    protected AbstractSampleElementsForSplitPointsHandler<String, AccumuloStore> createHandler() {
        return new SampleElementsForSplitPointsHandler();
    }
}
