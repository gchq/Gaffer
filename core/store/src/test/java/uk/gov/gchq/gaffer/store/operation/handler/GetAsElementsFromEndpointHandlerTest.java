/*
 * Copyright 2018-2019 Crown Copyright
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

import com.google.gson.JsonSyntaxException;
import org.junit.After;
import org.junit.Test;
import org.mockserver.integration.ClientAndServer;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.generator.JsonToElementGenerator;
import uk.gov.gchq.gaffer.data.util.ElementUtil;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.get.GetAsElementsFromEndpoint;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.user.User;

import java.net.MalformedURLException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

public class GetAsElementsFromEndpointHandlerTest {
    private static final String ENDPOINT_BASE_PATH = "http://127.0.0.1:";
    private static final String ENDPOINT_PATH = "/jsonEndpoint";
    private static final String INVALID_ENDPOINT_PATH = "/invalidJsonEndpoint";
    private final Store store = mock(Store.class);
    private final int port = 1080;
    private final Context context = new Context(new User());
    private final GetAsElementsFromEndpointHandler handler = new GetAsElementsFromEndpointHandler();
    private final List<Element> expected = Arrays.asList(new Entity.Builder()
                    .group("group1")
                    .vertex("vertex2")
                    .build(),
            new Entity.Builder()
                    .group("group1")
                    .vertex("vertex3")
                    .build());

    private ClientAndServer mockServer = ClientAndServer.startClientAndServer(port);

    @After
    public void tearDown() {
        mockServer.stop();
        assertFalse(mockServer.isRunning());
    }

    @Test
    public void shouldGetElementsFromEndpoint() throws OperationException {
        // Given
        mockServer.when(request()
                .withMethod("GET")
                .withPath(ENDPOINT_PATH))
                .respond(response()
                        .withStatusCode(200)
                        .withBody("[\n" +
                                "  {\n" +
                                "    \"class\": \"uk.gov.gchq.gaffer.data.element.Entity\",\n" +
                                "    \"group\": \"group1\",\n" +
                                "    \"vertex\": \"vertex2\"\n" +
                                "  },\n" +
                                "  {\n" +
                                "    \"class\": \"uk.gov.gchq.gaffer.data.element.Entity\",\n" +
                                "    \"group\": \"group1\",\n" +
                                "    \"vertex\": \"vertex3\"\n" +
                                "  }\n" +
                                "]"));
        final String endpointString = ENDPOINT_BASE_PATH + port + ENDPOINT_PATH;
        GetAsElementsFromEndpoint op = new GetAsElementsFromEndpoint.Builder()
                .endpoint(endpointString)
                .generator(JsonToElementGenerator.class)
                .build();

        // When
        Iterable<? extends Element> result = handler.doOperation(op, context, store);

        // Then
        ElementUtil.assertElementEquals(expected, result);
    }

    @Test
    public void shouldThrowExceptionWithMalformedEndpoint() {
        final String endpointString = "malformedUrl:" + port + ENDPOINT_PATH;
        GetAsElementsFromEndpoint op = new GetAsElementsFromEndpoint.Builder()
                .endpoint(endpointString)
                .generator(JsonToElementGenerator.class)
                .build();

        // When / Then
        try {
            handler.doOperation(op, context, store);
            fail("Exception expected");
        } catch (final OperationException e) {
            assertTrue(e.getCause().getClass().equals(MalformedURLException.class));
        }
    }

    @Test
    public void shouldThrowExceptionWhenEndpointHasIncorrectJson() {
        mockServer.when(request()
                .withMethod("GET")
                .withPath(INVALID_ENDPOINT_PATH))
                .respond(response()
                        .withStatusCode(200)
                        .withBody("[\n" +
                                "  {\n" +
                                "    \"class\": \"uk.gov.gchq.gaffer.data.element.Entity\",\n" +
                                "    \"group\": \"group1\",\n" +
                                "    \"incorrect: \"withWrongValue\"\n" +
                                "  },\n" +
                                "  {\n" +
                                "    \"class\": \"uk.gov.gchq.gaffer.data.incorrect.Entity\",\n" +
                                "    \"group\": \"group1\",\n" +
                                "    \"vertex\": \"vertex3\"\n" +
                                "  }\n" +
                                "]"));

        final String endpointString = ENDPOINT_BASE_PATH + port + INVALID_ENDPOINT_PATH;
        GetAsElementsFromEndpoint op = new GetAsElementsFromEndpoint.Builder()
                .endpoint(endpointString)
                .generator(JsonToElementGenerator.class)
                .build();

        // When / Then
        try {
            handler.doOperation(op, context, store);
        } catch (final Exception e) {
            assertTrue(e.getClass().equals(JsonSyntaxException.class));
        }
    }
}
