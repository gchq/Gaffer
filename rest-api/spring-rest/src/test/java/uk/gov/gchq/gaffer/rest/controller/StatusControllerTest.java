/*
 * Copyright 2020-2021 Crown Copyright
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


import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import uk.gov.gchq.gaffer.core.exception.GafferRuntimeException;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.rest.SystemStatus;
import uk.gov.gchq.gaffer.rest.factory.GraphFactory;
import uk.gov.gchq.gaffer.store.schema.Schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static uk.gov.gchq.gaffer.core.exception.Status.INTERNAL_SERVER_ERROR;

public class StatusControllerTest {

    @Mock
    private GraphFactory graphFactory;

    @BeforeEach
    public void initialiseMocks() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void shouldReturn503WhenGraphFactoryDoesNotReturnGraph() {
        // Given
        Mockito.when(graphFactory.getGraph()).thenReturn(null);

        // When
        StatusController statusController = new StatusController(graphFactory);

        // Then
        SystemStatus status = statusController.getStatus();
        assertEquals(SystemStatus.DOWN, status);
    }

    @Test
    public void shouldWrapExcpeptionsWithGREWhenGraphFactoryErrors() {
        // Given
        Mockito.when(graphFactory.getGraph()).thenThrow(new RuntimeException("err"));

        // When
        StatusController statusController = new StatusController(graphFactory);

        // Then
        assertThatExceptionOfType(GafferRuntimeException.class)
                .isThrownBy(statusController::getStatus)
                .satisfies(ex -> {
                    assertThat(ex.getMessage()).isNotNull();
                    assertThat(ex.getStatus()).isEqualTo(INTERNAL_SERVER_ERROR);
                });
    }

    @Test
    public void shouldReturnOkayStatusIfGraphFactoryReturnsGraph() {
        // Given
        Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder().graphId("id").build())
                .storeProperties(new MapStoreProperties())
                .addSchema(new Schema())
                .build();
        Mockito.when(graphFactory.getGraph()).thenReturn(graph);

        // When
        StatusController statusController = new StatusController(graphFactory);

        // Then
        assertEquals(SystemStatus.UP, statusController.getStatus());
    }

}
