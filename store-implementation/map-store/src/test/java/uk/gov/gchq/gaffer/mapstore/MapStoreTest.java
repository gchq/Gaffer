/*
 * Copyright 2017-2020 Crown Copyright
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

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.mapstore.optimiser.CountAllElementsOperationChainOptimiser;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MapStoreTest {

    @Test
    public void testTraits() throws StoreException {
        final MapStore mapStore = new MapStore();
        mapStore.initialise("graphId", new Schema(), new MapStoreProperties());
        final Set<StoreTrait> expectedTraits = new HashSet<>(Arrays.asList(
                StoreTrait.INGEST_AGGREGATION,
                StoreTrait.PRE_AGGREGATION_FILTERING,
                StoreTrait.POST_AGGREGATION_FILTERING,
                StoreTrait.TRANSFORMATION,
                StoreTrait.POST_TRANSFORMATION_FILTERING,
                StoreTrait.MATCHED_VERTEX));
        assertEquals(expectedTraits, mapStore.getTraits());
    }

    @Test
    public void shouldConfigureCountAllElementsOperationChainOptimiser() throws Exception {
        // Given
        final MapStore mapStore = new MapStore();

        // When
        mapStore.initialise("graphId", new Schema(), new MapStoreProperties());

        // Then
        assertEquals(1, mapStore.getOperationChainOptimisers().size());
        assertTrue(mapStore.getOperationChainOptimisers().contains(new CountAllElementsOperationChainOptimiser()));
    }
}
