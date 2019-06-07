/*
 * Copyright 2017-2019 Crown Copyright
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
package uk.gov.gchq.gaffer.rest.service.v1;

import org.junit.Test;

import uk.gov.gchq.gaffer.rest.AbstractRestApiIT;
import uk.gov.gchq.gaffer.rest.RestApiTestClient;
import uk.gov.gchq.gaffer.rest.SystemStatus;
import uk.gov.gchq.gaffer.store.schema.Schema;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Response;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class RestApiV1IT extends AbstractRestApiIT {

    private static final Client CLIENT = ClientBuilder.newClient();

    @Test
    public void shouldReturnOkStatusMessage() {
        // When
        final SystemStatus status = CLIENT.target("http://localhost:8080/rest/v1")
                .path("status")
                .request()
                .get(SystemStatus.class);

        // Then
        assertEquals("The system is working normally.", status.getStatus().getDescription());
    }

    @Test
    public void shouldRetrieveSchema() {
        // Given
        final Response response = CLIENT.target("http://localhost:8080/rest/v1")
                .path("graph/schema")
                .request()
                .get();

        // When
        final Schema schema = response.readEntity(Schema.class);

        // Then
        assertNotNull(schema);
    }


    @Override
    protected RestApiTestClient getClient() {
        return new RestApiV1TestClient();
    }
}
