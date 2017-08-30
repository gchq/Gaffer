/*
 * Copyright 2016-2017 Crown Copyright
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

package uk.gov.gchq.gaffer.rest.service.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.rest.RestApiTestClient;
import uk.gov.gchq.gaffer.rest.SystemStatus;
import uk.gov.gchq.gaffer.rest.application.ApplicationConfigV2;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import java.io.IOException;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;
import static org.junit.Assert.assertEquals;

public class RestApiV2TestClient extends RestApiTestClient {

    public RestApiV2TestClient() {
        super("http://localhost:8080/rest/v2", new ApplicationConfigV2());
    }

    @Override
    public Response executeOperation(final Operation operation) throws IOException {
        startServer();
        return client.target(uri)
                     .path("/graph/operations")
                     .request()
                     .post(Entity.entity(JSON_SERIALISER.serialise(operation), APPLICATION_JSON_TYPE));
    }

    @Override
    public Response executeOperationChain(final OperationChain opChain) throws IOException {
        startServer();

        final ObjectMapper mapper = JSONSerialiser.createDefaultMapper();
        System.out.println(mapper.writeValueAsString(opChain));

        return client.target(uri)
                     .path("/graph/operations")
                     .request()
                     .post(Entity.entity(JSON_SERIALISER.serialise(opChain), APPLICATION_JSON_TYPE));
    }

    @Override
    public Response executeOperationChainChunked(final OperationChain opChain) throws IOException {
        return executeOperationChunked(opChain);
    }

    @Override
    public Response executeOperationChunked(final Operation operation) throws IOException {
        startServer();
        return client.target(uri)
                     .path("/graph/operations/chunked")
                     .request()
                     .post(Entity.entity(JSON_SERIALISER.serialise(operation), APPLICATION_JSON_TYPE));
    }

    @Override
    public void checkRestServiceStatus() {
        // Given
        final Response response = client.target(uri)
                                        .path("/graph/status")
                                        .request()
                                        .get();

        // When
        final String statusMsg = response.readEntity(SystemStatus.class)
                                         .getStatus()
                                         .getDescription();

        // Then
        assertEquals("The system is working normally.", statusMsg);
    }
}
