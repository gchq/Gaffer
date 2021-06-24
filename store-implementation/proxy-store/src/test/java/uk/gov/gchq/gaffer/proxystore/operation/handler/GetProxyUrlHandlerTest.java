/*
 * Copyright 2021-2021 Crown Copyright
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
package uk.gov.gchq.gaffer.proxystore.operation.handler;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import uk.gov.gchq.gaffer.proxystore.operation.GetProxyUrl;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreProperties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static uk.gov.gchq.gaffer.proxystore.ProxyProperties.DEFAULT_GAFFER_CONTEXT_ROOT;
import static uk.gov.gchq.gaffer.proxystore.ProxyProperties.GAFFER_CONTEXT_ROOT;
import static uk.gov.gchq.gaffer.proxystore.ProxyProperties.GAFFER_HOST;
import static uk.gov.gchq.gaffer.proxystore.ProxyProperties.GAFFER_PORT;

public class GetProxyUrlHandlerTest {


    @Test
    public void shouldGetURl() throws Exception {
        //given
        String host = "non-default.com";
        String port = "1234";
        Store store = Mockito.mock(Store.class);
        StoreProperties storeProperties = new StoreProperties();
        storeProperties.set(GAFFER_PORT, port);
        storeProperties.set(GAFFER_HOST, host);
        storeProperties.set(GAFFER_CONTEXT_ROOT, DEFAULT_GAFFER_CONTEXT_ROOT);
        Mockito.when(store.getProperties()).thenReturn(storeProperties);
        String expected = String.format("http://%s:%s/rest/v2", host, port, DEFAULT_GAFFER_CONTEXT_ROOT);

        //when
        String url = new GetProxyUrlHandler().doOperation(new GetProxyUrl(), new Context(), store);

        //then
        assertEquals(expected, url);
    }

}
