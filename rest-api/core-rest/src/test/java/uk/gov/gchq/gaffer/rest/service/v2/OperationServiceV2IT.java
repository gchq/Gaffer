/*
 * Copyright 2016-2018 Crown Copyright
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

import org.junit.Test;

import uk.gov.gchq.gaffer.named.operation.AddNamedOperation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.rest.ServiceConstants;
import uk.gov.gchq.gaffer.rest.service.impl.OperationServiceIT;

import javax.ws.rs.core.Response;

import java.io.IOException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class OperationServiceV2IT extends OperationServiceIT {

    @Test
    public void shouldReturnJobIdHeader() throws IOException {
        // When
        final Response response = client.executeOperation(new GetAllElements());

        // Then
        assertNotNull(response.getHeaderString(ServiceConstants.JOB_ID_HEADER));
    }

    @Test
    public void shouldReturnOperationDetailFieldsWithClass() throws IOException {
        // Given
        String expectedFields = "\"fields\":[{\"name\":\"input\",\"className\":\"java.lang.Object[]\",\"required\":false}," +
                "{\"name\":\"view\",\"className\":\"uk.gov.gchq.gaffer.data.elementdefinition.view.View\",\"required\":false}," +
                "{\"name\":\"includeIncomingOutGoing\",\"className\":\"java.lang.String\",\"required\":false}," +
                "{\"name\":\"seedMatching\",\"className\":\"java.lang.String\",\"required\":false}," +
                "{\"name\":\"options\",\"className\":\"java.util.Map<java.lang.String,java.lang.String>\",\"required\":false}," +
                "{\"name\":\"directedType\",\"className\":\"java.lang.String\",\"required\":false}," +
                "{\"name\":\"views\",\"className\":\"java.util.List<uk.gov.gchq.gaffer.data.elementdefinition.view.View>\",\"required\":false}]";

        // When
        Response response = client.getOperationDetails(GetElements.class);

        // Then
        assertTrue(response.readEntity(String.class).contains(expectedFields));
    }

    @Test
    public void shouldReturnNamedOperationDetailWithInputType() throws IOException {
        // Given
        AddNamedOperation addNamedOperation = new AddNamedOperation.Builder()
                .name("exampleOp")
                .description("standard operation")
                .operationChain(new OperationChain.Builder().first(new AddElements()).build())
                .build();

        client.executeOperation(addNamedOperation);
        // When
        Response response = getClient().getNamedOperationDetail("exampleOp");

        System.out.println(response.readEntity(String.class));
    }

    @Override
    protected RestApiV2TestClient getClient() {
        return new RestApiV2TestClient();
    }
}
