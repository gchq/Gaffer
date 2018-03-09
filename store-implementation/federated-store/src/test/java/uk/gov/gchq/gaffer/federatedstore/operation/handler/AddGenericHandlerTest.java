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

package uk.gov.gchq.gaffer.federatedstore.operation.handler;

import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.federatedstore.FederatedStore;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedAddGraphHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedOperationIterableHandler;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;

import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class AddGenericHandlerTest {

    private FederatedStore store;
    private Graph graph;

    @Before
    public void setUp() throws Exception {
        store = mock(FederatedStore.class);

        graph = new Graph.Builder()
                .addStoreProperties(StoreProperties.loadStoreProperties("properties/singleUseMockAccStore.properties"))
                .config(new GraphConfig("TestGraph"))
                .addSchema(new Schema())
                .build();
    }

    @Test
    public void shouldHandleGetAllElements() throws Exception {
        given(store.isSupported(any())).willReturn(true);
        given(store.isSupported(GetAllElements.class)).willReturn(false);

        FederatedAddGraphHandler federatedAddGraphHandler = new FederatedAddGraphHandler();
        federatedAddGraphHandler.addGenericHandler(store, graph);

        verify(store, times(1)).addOperationHandler(eq(GetAllElements.class), any(FederatedOperationIterableHandler.class));
    }
 @Test
    public void shouldNotHandleAnything() throws Exception {
        given(store.isSupported(any())).willReturn(true);

        FederatedAddGraphHandler federatedAddGraphHandler = new FederatedAddGraphHandler();
        federatedAddGraphHandler.addGenericHandler(store, graph);

        verify(store, never()).addOperationHandler(any(), any(FederatedOperationIterableHandler.class));
    }


}
