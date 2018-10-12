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
import com.google.common.collect.Sets;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.impl.DiscardOutput;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.impl.job.GetAllJobDetails;
import uk.gov.gchq.gaffer.operation.impl.output.ToSingletonList;
import uk.gov.gchq.gaffer.rest.ServiceConstants;
import uk.gov.gchq.gaffer.rest.SystemProperty;
import uk.gov.gchq.gaffer.rest.factory.UserFactory;
import uk.gov.gchq.gaffer.rest.service.impl.OperationServiceIT;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
    public void shouldReturnSameJobIdInHeaderAsGetAllJobDetailsOperation() throws IOException {
        // Given
        Graph graph = new Graph.Builder()
                .config(StreamUtil.graphConfig(this.getClass()))
                .storeProperties(StreamUtil.STORE_PROPERTIES)
                .addSchema(new Schema())
                .build();

        client.reinitialiseGraph(graph);

        // When
        final Response response = client.executeOperation(new GetAllJobDetails());

        // Then
        assertTrue(response.readEntity(String.class).contains(response.getHeaderString("job-id")));
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

    @Test
    public void shouldReturnOutputClassForOperationWithOutput() throws Exception {
        // Given
        final String expectedOutputString = "\"outputClassName\":\"uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable<uk.gov.gchq.gaffer.data.element.Element>\"";

        // When
        Response response = client.getOperationDetails(GetElements.class);

        // Then
        assertTrue(response.readEntity(String.class).contains(expectedOutputString));
    }

    @Test
    public void shouldNotIncludeAnyOutputClassForOperationWithoutOutput() throws Exception {
        // Given
        final String outputClassNameString = "\"outputClassName\"";

        // When
        Response response = client.getOperationDetails(DiscardOutput.class);

        // Then
        assertFalse(response.readEntity(String.class).contains(outputClassNameString));
    }

    @Test
    public void shouldReturnOptionsAndSummariesForEnumFields() throws Exception {
        // Given

        // When
        Response response = client.getOperationDetails(GetElements.class);

        // Then
        final byte[] json = response.readEntity(byte[].class);
        final OperationDetailPojo opDetails = JSONSerialiser.deserialise(json, OperationDetailPojo.class);
        final List<OperationFieldPojo> fields = Arrays.asList(
                new OperationFieldPojo("input", "java.lang.Object[]", false, null, null),
                new OperationFieldPojo("view", "uk.gov.gchq.gaffer.data.elementdefinition.view.View", false, null, null),
                new OperationFieldPojo("includeIncomingOutGoing", "java.lang.String", false, "Should the edges point towards, or away from your seeds", Sets.newHashSet("INCOMING", "EITHER", "OUTGOING")),
                new OperationFieldPojo("seedMatching", "java.lang.String", false, "How should the seeds be matched?", Sets.newHashSet("RELATED", "EQUAL")),
                new OperationFieldPojo("options", "java.util.Map<java.lang.String,java.lang.String>", false, null, null),
                new OperationFieldPojo("directedType", "java.lang.String", false, "Is the Edge directed?", Sets.newHashSet("DIRECTED", "UNDIRECTED", "EITHER")),
                new OperationFieldPojo("views", "java.util.List<uk.gov.gchq.gaffer.data.elementdefinition.view.View>", false, null, null)
        );
        assertEquals(fields, opDetails.getFields());
    }

    @Test
    public void shouldAllowUserWithAuthThroughHeaders() throws IOException {
        System.setProperty(SystemProperty.USER_FACTORY_CLASS, TestUserFactory.class.getName());
        client.stopServer();
        client.startServer();

        Graph graph = new Graph.Builder()
                .config(StreamUtil.graphConfig(this.getClass()))
                .storeProperties(StreamUtil.STORE_PROPERTIES)
                .addSchema(new Schema())
                .build();
        client.reinitialiseGraph(graph);

        final OperationChain opChain = new OperationChain.Builder().first(new ToSingletonList.Builder<>().input("test").build()).build();
        Response response = ((RestApiV2TestClient) client).executeOperationChainChunkedWithHeaders(opChain, "ListUser");

        assertEquals(200, response.getStatus());
    }

    @Test
    public void shouldNotAllowUserWithNoAuthThroughHeaders() throws IOException {
        System.setProperty(SystemProperty.USER_FACTORY_CLASS, TestUserFactory.class.getName());
        client.stopServer();
        client.startServer();

        Graph graph = new Graph.Builder()
                .config(StreamUtil.graphConfig(this.getClass()))
                .storeProperties(StreamUtil.STORE_PROPERTIES)
                .addSchema(new Schema())
                .build();
        client.reinitialiseGraph(graph);

        final OperationChain opChain = new OperationChain.Builder().first(new ToSingletonList.Builder<>().input("test").build()).build();

        Response response = ((RestApiV2TestClient) client).executeOperationChainChunkedWithHeaders(opChain, "BasicUser");

        assertEquals(500, response.getStatus());
    }

    @Override
    protected RestApiV2TestClient getClient() {
        return new RestApiV2TestClient();
    }

    public static class OperationDetailPojo {
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

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final OperationDetailPojo that = (OperationDetailPojo) o;

            return new EqualsBuilder()
                    .append(name, that.name)
                    .append(summary, that.summary)
                    .append(fields, that.fields)
                    .append(next, that.next)
                    .append(exampleJson, that.exampleJson)
                    .isEquals();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37)
                    .append(name)
                    .append(summary)
                    .append(fields)
                    .append(next)
                    .append(exampleJson)
                    .toHashCode();
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this)
                    .append("name", name)
                    .append("summary", summary)
                    .append("fields", fields)
                    .append("next", next)
                    .append("exampleJson", exampleJson)
                    .toString();
        }
    }

    public static class OperationFieldPojo {
        private String name;
        private String className;
        private boolean required;
        private String summary;
        private Set<String> options;

        public OperationFieldPojo() {
        }

        public OperationFieldPojo(final String name, final String className, final boolean required, final String summary, final Set<String> options) {
            this.name = name;
            this.className = className;
            this.required = required;
            this.summary = summary;
            this.options = options;
        }

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

        public String getSummary() {
            return summary;
        }

        public void setSummary(final String summary) {
            this.summary = summary;
        }

        public Set<String> getOptions() {
            return options;
        }

        public void setOptions(final Set<String> options) {
            this.options = options;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final OperationFieldPojo that = (OperationFieldPojo) o;

            return new EqualsBuilder()
                    .append(required, that.required)
                    .append(name, that.name)
                    .append(className, that.className)
                    .append(summary, that.summary)
                    .append(options, that.options)
                    .isEquals();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37)
                    .append(name)
                    .append(className)
                    .append(required)
                    .append(summary)
                    .append(options)
                    .toHashCode();
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this)
                    .append("name", name)
                    .append("className", className)
                    .append("required", required)
                    .append("summary", summary)
                    .append("options", options)
                    .toString();
        }
    }

    public static class TestUserFactory implements UserFactory {

        @javax.ws.rs.core.Context
        private HttpHeaders httpHeaders;

        @Override
        public User createUser() {
            final String headerAuthVal = httpHeaders.getHeaderString(HttpHeaders.AUTHORIZATION);
            return new User.Builder()
                    .userId("unknown")
                    .opAuth(headerAuthVal)
                    .build();
        }
    }
}
