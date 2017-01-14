/*
 * Copyright 2016 Crown Copyright
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

package uk.gov.gchq.gaffer.rest.service;

import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.client.ChunkedInput;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import uk.gov.gchq.gaffer.rest.application.ApplicationResourceConfig;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.net.URI;

public class SimpleOperationServiceTest {

    private final static URI BASE_URI = URI.create("http://localhost:8081/test");

    private HttpServer server;

    @Before
    public void before() {
        final ResourceConfig rc = new ApplicationResourceConfig();
        server = GrizzlyHttpServerFactory.createHttpServer(BASE_URI, rc);
    }

    @Test
    @Ignore
    public void shouldProvideChunkedOutput() throws IOException {
        final String opChainJson = "{\n" +
                "  \"operations\": [\n" +
                "    {\n" +
                "      \"class\": \"uk.gov.gchq.gaffer.operation.impl.add.AddElements\",\n" +
                "      \"validate\": true,\n" +
                "      \"skipInvalidElements\": false,\n" +
                "      \"elements\": [\n" +
                "        {\n" +
                "          \"properties\": {},\n" +
                "          \"group\": \"entity\",\n" +
                "          \"vertex\": \"1\",\n" +
                "          \"class\": \"uk.gov.gchq.gaffer.data.element.Entity\"\n" +
                "        },\n" +
                "        {" +
                "          \"properties\": {},\n" +
                "          \"group\": \"entity\",\n" +
                "          \"vertex\": \"2\",\n" +
                "          \"class\": \"uk.gov.gchq.gaffer.data.element.Entity\"\n" +
                "        }\n" +
                "      ]\n" +
                "    },\n" +
                "    {\n" +
                "      \"class\": \"uk.gov.gchq.gaffer.operation.impl.get.GetAllElements\"\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        final Client client = ClientBuilder.newClient();

        final Response status = client.target(BASE_URI).path("/v1/status").request().get();

        System.out.println("Status: " + status.readEntity(String.class));

        final Response response = client.target(BASE_URI)
                                        .path("/v1/graph/doOperation/chunked")
                                        .request()
                                        .post(Entity.entity(opChainJson, MediaType.APPLICATION_JSON_TYPE));

        System.out.println(response.getStatus());

        final ChunkedInput<String> input = response.readEntity(new GenericType<ChunkedInput<String>>(){});

        String chunk;
        while((chunk = input.read())!= null) {
            System.out.println("Element: " + chunk);
        }
    }

}
