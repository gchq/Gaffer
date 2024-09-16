/*
 * Copyright 2020-2024 Crown Copyright
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

package uk.gov.gchq.gaffer.rest.controller;

import com.google.common.collect.Sets;
import org.apache.commons.lang3.exception.CloneFailedException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import uk.gov.gchq.gaffer.core.exception.GafferRuntimeException;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.DiscardOutput;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.rest.factory.ExamplesFactory;
import uk.gov.gchq.gaffer.rest.factory.GraphFactory;
import uk.gov.gchq.gaffer.rest.factory.spring.AbstractUserFactory;
import uk.gov.gchq.gaffer.rest.model.OperationDetail;
import uk.gov.gchq.gaffer.rest.model.OperationField;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.util.ReflectionUtil;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static uk.gov.gchq.gaffer.core.exception.Status.BAD_REQUEST;
import static uk.gov.gchq.gaffer.core.exception.Status.INTERNAL_SERVER_ERROR;
import static uk.gov.gchq.gaffer.core.exception.Status.NOT_FOUND;

public class OperationControllerTest {

    private Store store;
    private GraphFactory graphFactory;
    private AbstractUserFactory userFactory;
    private ExamplesFactory examplesFactory;
    private OperationController operationController;

    @BeforeEach
    public void setUpController() {
        store = mock(Store.class);
        graphFactory = mock(GraphFactory.class);
        userFactory = mock(AbstractUserFactory.class);
        examplesFactory = mock(ExamplesFactory.class);

        operationController = new OperationController(graphFactory, userFactory, examplesFactory);

        when(store.getSchema()).thenReturn(new Schema());
        when(store.getProperties()).thenReturn(new StoreProperties());

        final Graph graph = new Graph.Builder()
                .config(new GraphConfig("id"))
                .store(store)
                .addSchema(new Schema())
                .build();

        when(graphFactory.getGraph()).thenReturn(graph);
    }

    @SuppressWarnings({"unchecked"})
    @Test
    public void shouldReturnAllSupportedOperations() {
        // Given
        when(store.getSupportedOperations()).thenReturn(Sets.newHashSet(AddElements.class, GetElements.class));

        // When
        final Set<Class<? extends Operation>> allOperations = operationController.getOperations();

        // Then
        assertThat(allOperations).hasSize(2);
        assertThat(allOperations).contains(AddElements.class, GetElements.class);
    }

    @SuppressWarnings({"unchecked"})
    @Test
    public void shouldReturnAllOperations() {
        // Given / When
        final Set<Class> allOperations = (Set) operationController.getOperationsIncludingUnsupported();

        // Then
        final Set<Class> allExpectedOperations = ReflectionUtil.getSubTypes(Operation.class);
        assertThat(allOperations).containsExactlyInAnyOrderElementsOf(allExpectedOperations);
        assertThat(allOperations).isNotEmpty();
    }

    @SuppressWarnings({"unchecked"})
    @Test
    public void shouldReturnAllSupportedOperationsAsOperationDetails() {
        // Given
        when(store.getSupportedOperations()).thenReturn(Sets.newHashSet(AddElements.class, GetElements.class));

        // When
        final Set<OperationDetail> allOperationDetails = operationController.getAllOperationDetails();
        final Set<String> allOperationDetailClasses = allOperationDetails.stream().map(OperationDetail::getName).collect(Collectors.toSet());

        // Then
        assertThat(allOperationDetails).hasSize(2);
        assertThat(allOperationDetailClasses).contains(AddElements.class.getName(), GetElements.class.getName());
    }

    @Test
    public void shouldReturnAllOperationsAsOperationDetails() {
        // Given / When
        final Set<OperationDetail> allOperationDetails = operationController.getAllOperationDetailsIncludingUnsupported();
        final Set<String> allOperationDetailClasses = allOperationDetails.stream().map(OperationDetail::getName).collect(Collectors.toSet());

        // Then
        final Set<String> expectedOperationClasses = ReflectionUtil.getSubTypes(Operation.class).stream().map(Class::getName).collect(Collectors.toSet());
        assertThat(allOperationDetailClasses).containsExactlyInAnyOrderElementsOf(expectedOperationClasses);
        assertThat(allOperationDetails).isNotEmpty();
    }

    @Test
    public void shouldThrowBadRequestExceptionIfUserRequestsNextOperationsOfNonOperationClass() {
        // Then
        assertThatExceptionOfType(GafferRuntimeException.class)
                .isThrownBy(() -> operationController.getNextOperations("java.util.HashSet"))
                .extracting(GafferRuntimeException::getStatus)
                .isEqualTo(BAD_REQUEST);
    }

    @Test
    public void shouldThrowNotFoundExceptionWhenUserRequestsNextOperationOfNonExistentClass() {
        // Then
        assertThatExceptionOfType(GafferRuntimeException.class)
                .isThrownBy(() -> operationController.getNextOperations("non.existent.class"))
                .extracting(GafferRuntimeException::getStatus)
                .isEqualTo(NOT_FOUND);
    }

    @Test
    public void shouldThrowBadRequestExceptionIfUserRequestsOperationDetailsOfNonOperationClass() {
        // Then
        assertThatExceptionOfType(GafferRuntimeException.class)
                .isThrownBy(() -> operationController.getOperationDetails("java.util.HashSet"))
                .extracting(GafferRuntimeException::getStatus)
                .isEqualTo(BAD_REQUEST);
    }

    @Test
    public void shouldThrowNotFoundExceptionWhenUserRequestsOperationDetailsOfNonExistentClass() {
        // Then
        assertThatExceptionOfType(GafferRuntimeException.class)
                .isThrownBy(() -> operationController.getOperationDetails("non.existent.class"))
                .extracting(GafferRuntimeException::getStatus)
                .isEqualTo(NOT_FOUND);
    }

    @Test
    public void shouldThrowInternalServerExceptionIfUserRequestsOperationDetailsAboutUninstantiatableOperation() {
        // Then
        assertThatExceptionOfType(GafferRuntimeException.class)
                .isThrownBy(() -> operationController.getOperationDetails(UninstantiatableOperation.class.getName()))
                .extracting(GafferRuntimeException::getStatus)
                .isEqualTo(INTERNAL_SERVER_ERROR);
    }

    @Test
    public void shouldThrowBadRequestExceptionIfUserRequestsExampleOfNonOperationClass() {
        // Then
        assertThatExceptionOfType(GafferRuntimeException.class)
                .isThrownBy(() -> operationController.getOperationExample("java.util.HashSet"))
                .extracting(GafferRuntimeException::getStatus)
                .isEqualTo(BAD_REQUEST);
    }

    @Test
    public void shouldThrowNotFoundExceptionWhenUserRequestsExampleOfNonExistentClass() {
        // Then
        assertThatExceptionOfType(GafferRuntimeException.class)
                .isThrownBy(() -> operationController.getOperationExample("non.existent.class"))
                .extracting(GafferRuntimeException::getStatus)
                .isEqualTo(NOT_FOUND);
    }

    @Test
    public void shouldThrowInternalServerExceptionIfUserRequestsExampleUninstantiatableOperation() throws InstantiationException, IllegalAccessException {
        // When
        when(examplesFactory.generateExample(UninstantiatableOperation.class)).thenThrow(new InstantiationException());

        // Then
        assertThatExceptionOfType(GafferRuntimeException.class)
                .isThrownBy(() -> operationController.getOperationExample(UninstantiatableOperation.class.getName()))
                .extracting(GafferRuntimeException::getStatus)
                .isEqualTo(INTERNAL_SERVER_ERROR);
    }

    @SuppressWarnings({"unchecked"})
    @Test
    public void shouldReturnOperationDetailSummaryOfClass() {
        // Given
        when(store.getSupportedOperations()).thenReturn(Sets.newHashSet(GetElements.class));

        // When
        final OperationDetail operationDetail = operationController.getOperationDetails(GetElements.class.getName());

        // Then
        final String expectedSummary = "Gets elements related to provided seeds";
        assertThat(operationDetail.getSummary()).isEqualTo(expectedSummary);
    }

    @SuppressWarnings({"unchecked"})
    @Test
    public void shouldReturnOutputClassForOperationWithOutput() throws Exception {
        // Given
        when(store.getSupportedOperations()).thenReturn(Sets.newHashSet(GetElements.class));
        when(examplesFactory.generateExample(GetElements.class)).thenReturn(new GetElements());

        // When
        final OperationDetail operationDetails = operationController.getOperationDetails(GetElements.class.getName());

        // Then
        final String expectedOutputString = "java.lang.Iterable<uk.gov.gchq.gaffer.data.element.Element>";
        assertThat(operationDetails.getOutputClassName()).isEqualTo(expectedOutputString);
    }

    @SuppressWarnings({"unchecked"})
    @Test
    public void shouldNotIncludeAnyOutputClassForOperationWithoutOutput() throws Exception {
        // Given
        when(store.getSupportedOperations()).thenReturn(Sets.newHashSet(DiscardOutput.class));
        when(examplesFactory.generateExample(GetElements.class)).thenReturn(new DiscardOutput());

        // When
        final OperationDetail operationDetail = operationController.getOperationDetails(DiscardOutput.class.getName());
        final byte[] serialised = JSONSerialiser.serialise(operationDetail);

        // Then
        assertThat(new String(serialised)).doesNotContain("outputClassName");
    }

    @SuppressWarnings({"unchecked"})
    @Test
    public void shouldReturnOptionsAndSummariesForEnumFields() throws Exception {
        // Given
        when(store.getSupportedOperations()).thenReturn(Sets.newHashSet(GetElements.class));
        when(examplesFactory.generateExample(GetElements.class)).thenReturn(new GetElements());

        // When
        final OperationDetail operationDetails = operationController.getOperationDetails(GetElements.class.getName());
        final List<OperationField> operationFields = operationDetails.getFields();

        // Then
        final List<OperationField> fields = Arrays.asList(
                new OperationField("input", null, "java.lang.Object[]", null, false),
                new OperationField("view", null, "uk.gov.gchq.gaffer.data.elementdefinition.view.View", null, false),
                new OperationField("includeIncomingOutGoing", "Should the edges point towards, or away from your seeds", "java.lang.String", Sets.newHashSet("INCOMING", "EITHER", "OUTGOING"), false),
                new OperationField("options", null, "java.util.Map<java.lang.String,java.lang.String>", null, false),
                new OperationField("directedType", "Is the Edge directed?", "java.lang.String", Sets.newHashSet("DIRECTED", "UNDIRECTED", "EITHER"), false),
                new OperationField("views", null, "java.util.List<uk.gov.gchq.gaffer.data.elementdefinition.view.View>", null, false));

        assertThat(operationFields).isEqualTo(fields);
    }

    @SuppressWarnings({"unchecked"})
    @Test
    public void shouldCorrectlyChunkIterables() throws IOException, OperationException {
        // Given
        when(userFactory.createContext()).thenReturn(new Context(new User()));
        when(store.execute(any(Output.class), any(Context.class))).thenReturn(Arrays.asList(1, 2, 3));

        // When
        final ResponseEntity<StreamingResponseBody> response = operationController.executeChunked(mock(HttpHeaders.class), new GetAllElements());
        try (final OutputStream output = new ByteArrayOutputStream()) {
            response.getBody().writeTo(output);
            // Then
            assertThat(output.toString()).isEqualTo("1\r\n2\r\n3\r\n");

        }

    }

    private static class UninstantiatableOperation implements Operation {

        @Override
        public Operation shallowClone() throws CloneFailedException {
            return null;
        }

        @Override
        public Map<String, String> getOptions() {
            return null;
        }

        @Override
        public void setOptions(final Map<String, String> options) {

        }
    }

}
