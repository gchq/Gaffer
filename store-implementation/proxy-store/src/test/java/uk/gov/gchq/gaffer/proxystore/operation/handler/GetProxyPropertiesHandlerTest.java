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

import uk.gov.gchq.gaffer.proxystore.ProxyProperties;
import uk.gov.gchq.gaffer.proxystore.operation.GetProxyProperties;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static uk.gov.gchq.gaffer.proxystore.ProxyProperties.DEFAULT_GAFFER_CONTEXT_ROOT;
import static uk.gov.gchq.gaffer.proxystore.ProxyProperties.GAFFER_CONTEXT_ROOT;
import static uk.gov.gchq.gaffer.proxystore.ProxyProperties.GAFFER_HOST;
import static uk.gov.gchq.gaffer.proxystore.ProxyProperties.GAFFER_PORT;
import static uk.gov.gchq.gaffer.proxystore.operation.handler.GetProxyPropertiesHandler.URL_INFERRED;

public class GetProxyPropertiesHandlerTest {


    private static final String EXTRA_KEY = "extra_key";
    private static final String PORT = "1234";
    private static final String HOST = "non-default.com";

    @Test
    public void shouldGetURl() throws Exception {
        //given
        Store store = Mockito.mock(Store.class);
        ProxyProperties properties = new ProxyProperties();
        properties.set(GAFFER_PORT, PORT);
        properties.set(GAFFER_HOST, HOST);
        properties.set(GAFFER_CONTEXT_ROOT, DEFAULT_GAFFER_CONTEXT_ROOT);
        properties.setConnectTimeout(999);
        properties.set(EXTRA_KEY, "value");
        Mockito.when(store.getProperties()).thenReturn(properties);

        HashMap expected = getExpected(properties);

        //when
        Map<String, Object> actual = new GetProxyPropertiesHandler().doOperation(new GetProxyProperties(), new Context(), store);

        //then
        assertFalse(actual.containsKey(EXTRA_KEY));
        assertTrue(actual.containsKey(URL_INFERRED));
        assertEquals(expected, actual);
    }

    private HashMap getExpected(final ProxyProperties properties) {
        HashMap expected = new HashMap();
        expected.put(ProxyProperties.GAFFER_CONTEXT_ROOT, properties.getGafferContextRoot());
        expected.put(ProxyProperties.GAFFER_HOST, properties.getGafferHost());
        expected.put(ProxyProperties.GAFFER_PORT, String.valueOf(properties.getGafferPort()));
        expected.put(ProxyProperties.CONNECT_TIMEOUT, String.valueOf(properties.getConnectTimeout()));
        expected.put(ProxyProperties.READ_TIMEOUT, String.valueOf(ProxyProperties.DEFAULT_READ_TIMEOUT));
        expected.put(URL_INFERRED, String.format("http://%s:%s/rest/v2", HOST, PORT, DEFAULT_GAFFER_CONTEXT_ROOT));
        return expected;
    }

}
