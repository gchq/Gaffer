/*
 * Copyright 2019 Crown Copyright
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

package uk.gov.gchq.gaffer.store.operation.handler;

import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Test;
import org.mockserver.integration.ClientAndServer;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.get.GetFromEndpoint;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.declaration.OperationDeclarations;
import uk.gov.gchq.gaffer.user.User;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

public class GetFromEndpointHandlerTest {
    private static final String ENDPOINT_BASE_PATH = "http://127.0.0.1:";
    private static final String ENDPOINT_PATH = "/jsonEndpoint";
    private static final String RESPONSE = String.format("[%n" +
            "  {%n" +
            "    \"class\": \"uk.gov.gchq.gaffer.data.element.Entity\",%n" +
            "    \"group\": \"group1\",%n" +
            "    \"vertex\": \"vertex2\"%n" +
            "  },%n" +
            "  {%n" +
            "    \"class\": \"uk.gov.gchq.gaffer.data.element.Entity\",%n" +
            "    \"group\": \"group1\",%n" +
            "    \"vertex\": \"vertex3\"%n" +
            "  }%n" +
            "]");
    private final Store store = mock(Store.class);
    private final int port = 1080;
    private final Context context = new Context(new User());
    private final GetFromEndpointHandler handler = new GetFromEndpointHandler();
    private ClientAndServer mockServer = ClientAndServer.startClientAndServer(port);

    @After
    public void tearDown() {
        mockServer.stop();
        assertFalse(mockServer.isRunning());
    }

    @Test
    public void shouldLoadOperationDeclarations() throws IOException {
        // When
        InputStream stream = StreamUtil.openStream(GetFromEndpointHandler.class, "GetFromEndpointOperationDeclarations.json");

        // Given
        OperationDeclarations opDeclarations = JSONSerialiser.deserialise(IOUtils.toByteArray(stream), OperationDeclarations.class);

        // Then
        assertEquals(1, opDeclarations.getOperations().size());
        assertEquals(GetFromEndpoint.class, opDeclarations.getOperations().get(0).getOperation());
        assertEquals(GetFromEndpointHandler.class, opDeclarations.getOperations().get(0).getHandler().getClass());
    }

    @Test
    public void shouldGetElementsFromEndpoint() throws OperationException {
        // Given
        mockServer.when(request()
                .withMethod("GET")
                .withPath(ENDPOINT_PATH))
                .respond(response()
                        .withStatusCode(200)
                        .withBody(RESPONSE));
        final String endpointString = ENDPOINT_BASE_PATH + port + ENDPOINT_PATH;
        GetFromEndpoint op = new GetFromEndpoint.Builder()
                .endpoint(endpointString)
                .build();

        // When
        String result = handler.doOperation(op, context, store);

        // Then
        assertEquals(RESPONSE, result);
    }

    @Test
    public void shouldThrowExceptionWithMalformedEndpoint() {
        final String endpointString = "malformedUrl:" + port + ENDPOINT_PATH;
        GetFromEndpoint op = new GetFromEndpoint.Builder()
                .endpoint(endpointString)
                .build();

        // When / Then
        try {
            handler.doOperation(op, context, store);
            fail("Exception expected");
        } catch (final OperationException e) {
            assertTrue(e.getCause().getClass().equals(MalformedURLException.class));
        }
    }
}
