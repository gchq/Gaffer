package uk.gov.gchq.gaffer.proxystore.operation.handler;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import uk.gov.gchq.gaffer.proxystore.operation.GetUrlOperation;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreProperties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static uk.gov.gchq.gaffer.proxystore.ProxyProperties.DEFAULT_GAFFER_CONTEXT_ROOT;
import static uk.gov.gchq.gaffer.proxystore.ProxyProperties.GAFFER_CONTEXT_ROOT;
import static uk.gov.gchq.gaffer.proxystore.ProxyProperties.GAFFER_HOST;
import static uk.gov.gchq.gaffer.proxystore.ProxyProperties.GAFFER_PORT;

class GetUrlHandlerTest {


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
        String url = new GetUrlHandler().doOperation(new GetUrlOperation(), new Context(), store);
        assertEquals(expected, url);
    }

}