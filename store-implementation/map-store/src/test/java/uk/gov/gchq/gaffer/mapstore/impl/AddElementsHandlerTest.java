/*
 * Copyright 2018-2024 Crown Copyright
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

package uk.gov.gchq.gaffer.mapstore.impl;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.mapstore.MapStore;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.mapstore.SingleUseMapStore;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

class AddElementsHandlerTest {

    @Test
    void shouldAddWithNoGroupByProperties() throws StoreException {
        // Given
        final AddElements addElements = mock(AddElements.class);
        given(addElements.getInput()).willReturn((Iterable) Arrays.asList(new Edge("group1")));
        final Context context = mock(Context.class);
        final MapStore store = new SingleUseMapStore();
        store.initialise("graphId1", new Schema(), new MapStoreProperties());
        final AddElementsHandler handler = new AddElementsHandler();

        // When / Then - should not throw NPE
        assertThatNoException().isThrownBy(() -> handler.doOperation(addElements, context, store));
    }
}
