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
package uk.gov.gchq.gaffer.mapstore.utils;

import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.commonutil.stream.Streams;
import uk.gov.gchq.gaffer.mapstore.MapStore;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.mapstore.impl.GetAllElementsHandlerTest;
import uk.gov.gchq.gaffer.store.StoreException;

import static org.junit.Assert.assertEquals;

public class ElementClonerTest {

    @Test
    public void testElementCloner() throws StoreException {
        // Given
        final ElementCloner cloner = new ElementCloner();
        final MapStore mapStore = new MapStore();
        mapStore.initialise("graphId", GetAllElementsHandlerTest.getSchema(), new MapStoreProperties());

        // Then
        Streams.toStream(GetAllElementsHandlerTest.getElements())
                .map(element -> new Pair<>(element, cloner.cloneElement(element, mapStore.getSchema())))
                .forEach(pair -> assertEquals(pair.getFirst(), pair.getSecond()));
    }
}
