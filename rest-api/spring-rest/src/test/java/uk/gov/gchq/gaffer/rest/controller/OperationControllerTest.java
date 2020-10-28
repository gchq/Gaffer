/*
 * Copyright 2020 Crown Copyright
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

import uk.gov.gchq.gaffer.core.exception.GafferRuntimeException;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.impl.DiscardOutput;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.rest.factory.ExamplesFactory;
import uk.gov.gchq.gaffer.rest.factory.GraphFactory;
import uk.gov.gchq.gaffer.rest.factory.UserFactory;
import uk.gov.gchq.gaffer.rest.model.OperationDetail;
import uk.gov.gchq.gaffer.rest.model.OperationField;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static uk.gov.gchq.gaffer.core.exception.Status.BAD_REQUEST;
import static uk.gov.gchq.gaffer.core.exception.Status.INTERNAL_SERVER_ERROR;
import static uk.gov.gchq.gaffer.core.exception.Status.NOT_FOUND;

public class OperationControllerTest {

    private Store store;
    private GraphFactory graphFactory;
    private UserFactory userFactory;
    private ExamplesFactory examplesFactory;
    private OperationController operationController;

    @BeforeEach
    public void setUpController() {
        store = mock(Store.class);
        graphFactory = mock(GraphFactory.class);
        userFactory = mock(UserFactory.class);
        examplesFactory = mock(ExamplesFactory.class);

        operationController = new OperationController(graphFactory, userFactory, examplesFactory);

        when(store.getSchema()).thenReturn(new Schema());
        when(store.getProperties()).thenReturn(new StoreProperties());

        Graph graph = new Graph.Builder()
                .config(new GraphConfig("id"))
                .store(store)
                .addSchema(new Schema())
                .build();

        when(graphFactory.getGraph()).thenReturn(graph);
    }

    @Test
    public void shouldReturnAllSupportedOperationsAsOperationDetails() {
        // Given
        when(store.getSupportedOperations()).thenReturn(
                Sets.newHashSet(AddElements.class, GetElements.class)
        );

        // When
        Set<OperationDetail> allOperationDetails = operationController.getAllOperationDetails();
        Set<String> allOperationDetailClasses = allOperationDetails.stream().map(OperationDetail::getName).collect(Collectors.toSet());

        // Then
        assertEquals(2, allOperationDetails.size());
        assertTrue(allOperationDetailClasses.contains(AddElements.class.getName()));
        assertTrue(allOperationDetailClasses.contains(GetElements.class.getName()));
    }

    @Test
    public void shouldThrowBadRequestExceptionIfUserRequestsNextOperationsOfNonOperationClass() {
        // Then
        GafferRuntimeException e = assertThrows(GafferRuntimeException.class, () -> {
            operationController.getNextOperations("java.util.HashSet");
        });

        assertEquals(BAD_REQUEST, e.getStatus());
    }

    @Test
    public void shouldThrowNotFoundExceptionWhenUserRequestsNextOperationOfNonExistentClass() {
        // Then
        GafferRuntimeException e = assertThrows(GafferRuntimeException.class, () -> {
            operationController.getNextOperations("non.existent.class");
        });

        assertEquals(NOT_FOUND, e.getStatus());
    }

    @Test
    public void shouldThrowBadRequestExceptionIfUserRequestsOperationDetailsOfNonOperationClass() {
        // Then
        GafferRuntimeException e = assertThrows(GafferRuntimeException.class, () -> {
            operationController.getOperationDetails("java.util.HashSet");
        });

        assertEquals(BAD_REQUEST, e.getStatus());
    }

    @Test
    public void shouldThrowNotFoundExceptionWhenUserRequestsOperationDetailsOfNonExistentClass() {
        // Then
        GafferRuntimeException e = assertThrows(GafferRuntimeException.class, () -> {
            operationController.getOperationDetails("non.existent.class");
        });

        assertEquals(NOT_FOUND, e.getStatus());
    }

    @Test
    public void shouldThrowInternalServerExceptionIfUserRequestsOperationDetailsAboutUninstantiatableOperation() {
        // Then
        GafferRuntimeException e = assertThrows(GafferRuntimeException.class, () -> {
            operationController.getOperationDetails(UninstantiatableOperation.class.getName());
        });

        assertEquals(INTERNAL_SERVER_ERROR, e.getStatus());
    }

    @Test
    public void shouldThrowBadRequestExceptionIfUserRequestsExampleOfNonOperationClass() {
        // Then
        GafferRuntimeException e = assertThrows(GafferRuntimeException.class, () -> {
            operationController.getOperationExample("java.util.HashSet");
        });

        assertEquals(BAD_REQUEST, e.getStatus());
    }

    @Test
    public void shouldThrowNotFoundExceptionWhenUserRequestsExampleOfNonExistentClass() {
        // Then
        GafferRuntimeException e = assertThrows(GafferRuntimeException.class, () -> {
            operationController.getOperationExample("non.existent.class");
        });

        assertEquals(NOT_FOUND, e.getStatus());
    }

    @Test
    public void shouldThrowInternalServerExceptionIfUserRequestsExampleUninstantiatableOperation() throws InstantiationException, IllegalAccessException {
        // When
        when(examplesFactory.generateExample(UninstantiatableOperation.class)).thenThrow(new InstantiationException());

        // Then
        GafferRuntimeException e = assertThrows(GafferRuntimeException.class, () -> {
            operationController.getOperationExample(UninstantiatableOperation.class.getName());
        });

        assertEquals(INTERNAL_SERVER_ERROR, e.getStatus());
    }

    @Test
    public void shouldReturnOperationDetailSummaryOfClass() {
        // Given
        when(store.getSupportedOperations()).thenReturn(Sets.newHashSet(GetElements.class));

        // When
        OperationDetail operationDetail = operationController.getOperationDetails(GetElements.class.getName());

        // Then
        final String expectedSummary = "Gets elements related to provided seeds";
        assertEquals(expectedSummary, operationDetail.getSummary());
    }

    @Test
    public void shouldReturnOutputClassForOperationWithOutput() throws Exception {
        // Given
        when(store.getSupportedOperations()).thenReturn(Sets.newHashSet(GetElements.class));
        when(examplesFactory.generateExample(GetElements.class)).thenReturn(new GetElements());

        // When
        OperationDetail operationDetails = operationController.getOperationDetails(GetElements.class.getName());

        // Then
        final String expectedOutputString = "uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable<uk.gov.gchq.gaffer.data.element.Element>";
        assertEquals(expectedOutputString, operationDetails.getOutputClassName());
    }

    @Test
    public void shouldNotIncludeAnyOutputClassForOperationWithoutOutput() throws Exception {
        // Given
        when(store.getSupportedOperations()).thenReturn(Sets.newHashSet(DiscardOutput.class));
        when(examplesFactory.generateExample(GetElements.class)).thenReturn(new DiscardOutput());

        // When
        OperationDetail operationDetail = operationController.getOperationDetails(DiscardOutput.class.getName());
        byte[] serialised = JSONSerialiser.serialise(operationDetail);

        // Then
        assertFalse(new String(serialised).contains("outputClassName"));
    }

    @Test
    public void shouldReturnOptionsAndSummariesForEnumFields() throws Exception {
        // Given
        when(store.getSupportedOperations()).thenReturn(Sets.newHashSet(GetElements.class));
        when(examplesFactory.generateExample(GetElements.class)).thenReturn(new GetElements());

        // When
        OperationDetail operationDetails = operationController.getOperationDetails(GetElements.class.getName());
        List<OperationField> operationFields = operationDetails.getFields();


        // Then
        final List<OperationField> fields = Arrays.asList(
                new OperationField("input", null, "java.lang.Object[]", null, false),
                new OperationField("view", null, "uk.gov.gchq.gaffer.data.elementdefinition.view.View", null, false),
                new OperationField("includeIncomingOutGoing", "Should the edges point towards, or away from your seeds", "java.lang.String", Sets.newHashSet("INCOMING", "EITHER", "OUTGOING"), false),
                new OperationField("seedMatching", "How should the seeds be matched?", "java.lang.String", Sets.newHashSet("RELATED", "EQUAL"), false),
                new OperationField("options", null, "java.util.Map<java.lang.String,java.lang.String>", null, false),
                new OperationField("directedType", "Is the Edge directed?", "java.lang.String", Sets.newHashSet("DIRECTED", "UNDIRECTED", "EITHER"), false),
                new OperationField("views", null, "java.util.List<uk.gov.gchq.gaffer.data.elementdefinition.view.View>", null, false)
        );

        assertEquals(fields, operationFields);
    }

    private static class UninstantiatableOperation implements Operation {

        UninstantiatableOperation(final String str) {
            // No default constructor
        }

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
