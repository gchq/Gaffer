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
package uk.gov.gchq.gaffer.mapstore;

import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.mapstore.impl.CountElementsOperationChainOptimiser;
import uk.gov.gchq.gaffer.mapstore.operation.CountAllElementsDefaultView;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.impl.Count;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.schema.Schema;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class CountElementsOperationChainOptimiserTest {

    @Test
    public void testOptimisationIsApplied() throws StoreException {
        // Given
        final MapStoreProperties mapStoreProperties = new MapStoreProperties();
        final MapStore mapStore = new MapStore();
        mapStore.initialise(new Schema(), mapStoreProperties);
        final OperationChain<Long> chain = new OperationChain.Builder()
                .first(new GetAllElements<>())
                .then(new Count<>())
                .build();

        // When
        final OperationChain<Long> optimised = new CountElementsOperationChainOptimiser().optimise(chain);

        // Then
        assertEquals(1L, optimised.getOperations().size());
        assertEquals(CountAllElementsDefaultView.class, optimised.getOperations().get(0).getClass());
    }

    @Test
    public void testOptimisationIsNotAppliedWhenCantBe() throws StoreException {
        // Given
        final MapStoreProperties mapStoreProperties = new MapStoreProperties();
        final MapStore mapStore = new MapStore();
        mapStore.initialise(new Schema(), mapStoreProperties);

        // When
        final OperationChain<CloseableIterable<Element>> chain = new OperationChain.Builder()
                .first(new GetAllElements<>())
                .build();
        OperationChain<?> optimised = new CountElementsOperationChainOptimiser().optimise(chain);

        // Then
        assertEquals(1L, optimised.getOperations().size());
        assertEquals(GetAllElements.class, optimised.getOperations().get(0).getClass());

        // When
        final GetAllElements<Element> getAllElements = new GetAllElements.Builder<>()
                .view(new View.Builder()
                    .edge("TEST")
                    .build())
                .build();
        final OperationChain<Long> chain2 = new OperationChain.Builder()
                .first(getAllElements)
                .then(new Count<>())
                .build();
        optimised = new CountElementsOperationChainOptimiser().optimise(chain2);

        // Then
        assertEquals(2L, optimised.getOperations().size());
        assertEquals(GetAllElements.class, optimised.getOperations().get(0).getClass());
        assertEquals(Count.class, optimised.getOperations().get(1).getClass());
    }
}
