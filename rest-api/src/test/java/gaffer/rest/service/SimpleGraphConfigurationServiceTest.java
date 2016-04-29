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

package gaffer.rest.service;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

import gaffer.graph.Graph;
import gaffer.operation.Operation;
import gaffer.operation.impl.add.AddElements;
import gaffer.rest.GraphFactory;
import gaffer.store.Store;
import gaffer.store.schema.Schema;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;


public class SimpleGraphConfigurationServiceTest {
    private SimpleGraphConfigurationService service;

    @Before
    public void setup() {
        final GraphFactory graphFactory = mock(GraphFactory.class);
        final Store store = mock(Store.class);
        final Schema schema = mock(Schema.class);
        given(store.getSchema()).willReturn(schema);
        final Graph graph = new Graph.Builder().store(store).build();
        final Collection<Class<? extends Operation>> operations = new LinkedList<>();
        operations.add(AddElements.class);
        given(graphFactory.getGraph()).willReturn(graph);
        given(graph.getSupportedOperations()).willReturn(operations);
        given(graph.isSupported(AddElements.class)).willReturn(true);

        service = new SimpleGraphConfigurationService(graphFactory);
    }

    @Test
    public void shouldGetFilterFunctions() throws IOException {
        // When
        final List<Class> classes = service.getFilterFunctions();

        // Then
        assertTrue(classes.size() > 0);
    }

    @Test
    public void shouldGetTransformFunctions() throws IOException {
        // When
        final List<Class> classes = service.getTransformFunctions();

        // Then
        assertTrue(classes.size() > 0);
    }

    @Test
    public void shouldGetGenerators() throws IOException {
        // When
        final List<Class> classes = service.getGenerators();

        // Then
        assertTrue(classes.size() > 0);
    }

    @Test
    public void shouldGetAllAvailableOperations() throws IOException {
        // When
        final List<Class<? extends Operation>> supportedOperations = service.getOperations();

        // Then
        assertTrue(supportedOperations.size() > 0);
        assertEquals(1, supportedOperations.size());
    }

    @Test
    public void shouldValidateWhetherOperationIsSupported() throws IOException {
        // When
        final List<Class<? extends Operation>> supportedOperations = service.getOperations();

        for (final Class<? extends Operation> operationClass : supportedOperations) {
            // Then
            assertTrue(service.isOperationSupported(operationClass));
        }
    }
}
