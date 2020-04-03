/*
 * Copyright 2020 Crown Copyright
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
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.SampleToSplitPoints;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.operation.handler.AbstractSampleToSplitPointsHandler;
import uk.gov.gchq.gaffer.store.operation.handler.AbstractSampleToSplitPointsHandlerTest;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class SampleToSplitPointsHanderTest extends AbstractSampleToSplitPointsHandlerTest<AccumuloStore> {

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
    protected AccumuloStore createStore() {
        return store;
    }

    @Override
    protected AbstractSampleToSplitPointsHandler<String, AccumuloStore> createHandler() {
        return new SampleToSplitPointsHandler();
    }

    @Test
    public void shouldUseTheNumberOfTabletServersToCalculateNumSplits() throws OperationException {

        // Given
        final Integer numSplits = null;
        final List<String> sample = createSampleOfSize(100);

        final SampleToSplitPoints<String> operation = new SampleToSplitPoints.Builder<String>()
                .input(sample)
                .numSplits(numSplits)
                .build();

        // When
        final List<String> splits = createHandler().doOperation(operation, new Context(), store);

        // Then
        final int expectedNumSplits = NUM_TABLET_SERVERS - 1;
        assertEquals(expectedNumSplits, splits.size());
    }

}
