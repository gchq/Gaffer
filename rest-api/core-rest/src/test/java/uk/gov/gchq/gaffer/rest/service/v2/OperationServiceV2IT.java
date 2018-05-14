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

import com.fasterxml.jackson.core.type.TypeReference;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.rest.ServiceConstants;
import uk.gov.gchq.gaffer.rest.service.impl.OperationServiceIT;
import uk.gov.gchq.gaffer.store.schema.Schema;

import javax.ws.rs.core.Response;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
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
    public void shouldReturn403WhenUnauthorised() throws IOException {
        // Given
        Graph graph = new Graph.Builder()
                .config(StreamUtil.graphConfig(this.getClass()))
                .storeProperties(StreamUtil.STORE_PROPERTIES)
                .addSchema(new Schema())
                .build();
        client.reinitialiseGraph(graph);

        // When
        final Response response = client.executeOperation(new GetAllElements());

        // Then
        assertEquals(403, response.getStatus());
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
    public void shouldReturnAllOperationsAsOperationDetails() throws IOException, ClassNotFoundException {
        // Given
        final Set<Class<? extends Operation>> expectedOperations = client.getDefaultGraphFactory().getGraph().getSupportedOperations();

        // When
        final Response response = ((RestApiV2TestClient) client).getAllOperationsAsOperationDetails();

        // Then
        byte[] json = response.readEntity(byte[].class);
        List<OperationDetailPojo> opDetails = JSONSerialiser.deserialise(json, new TypeReference<List<OperationDetailPojo>>() {
        });
        final Set<String> opDetailClasses = opDetails.stream().map(OperationDetailPojo::getName).collect(Collectors.toSet());
        for (final Class<? extends Operation> clazz : expectedOperations) {
            assertTrue(opDetailClasses.contains(clazz.getName()));
        }
    }

    @Test
    public void shouldReturnOperationDetailSummaryOfClass() throws Exception {
        // Given
        final String expectedSummary = "\"summary\":\"Gets elements related to provided seeds\"";

        // When
        Response response = client.getOperationDetails(GetElements.class);

        // Then
        assertTrue(response.readEntity(String.class).contains(expectedSummary));

    }

    @Override
    protected RestApiV2TestClient getClient() {
        return new RestApiV2TestClient();
    }

    private static class OperationDetailPojo {
        private String name;
        private String summary;
        private List<OperationFieldPojo> fields;
        private Set<Class<? extends Operation>> next;
        private Operation exampleJson;

        public void setName(final String name) {
            this.name = name;
        }

        public void setSummary(final String summary) {
            this.summary = summary;
        }

        public void setFields(final List<OperationFieldPojo> fields) {
            this.fields = fields;
        }

        public void setNext(final Set<Class<? extends Operation>> next) {
            this.next = next;
        }

        public void setExampleJson(final Operation exampleJson) {
            this.exampleJson = exampleJson;
        }

        public String getName() {
            return name;
        }

        public String getSummary() {
            return summary;
        }

        public List<OperationFieldPojo> getFields() {
            return fields;
        }

        public Set<Class<? extends Operation>> getNext() {
            return next;
        }

        public Operation getExampleJson() {
            return exampleJson;
        }
    }

    private static class OperationFieldPojo {
        private String name;
        private String className;
        private boolean required;

        public void setName(final String name) {
            this.name = name;
        }

        public void setRequired(final boolean required) {
            this.required = required;
        }

        public void setClassName(final String className) {
            this.className = className;
        }

        public String getName() {
            return name;
        }

        public boolean isRequired() {
            return required;
        }

        public String getClassName() {
            return className;
        }
    }
}
