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

import static gaffer.store.StoreTrait.AGGREGATION;
import static gaffer.store.StoreTrait.FILTERING;
import static gaffer.store.StoreTrait.STORE_VALIDATION;
import static gaffer.store.StoreTrait.TRANSFORMATION;
import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

import gaffer.graph.Graph;
import gaffer.jsonserialisation.JSONSerialiser;
import gaffer.operation.Operation;
import gaffer.operation.impl.add.AddElements;
import gaffer.rest.GraphFactory;
import gaffer.store.Store;
import gaffer.store.StoreTrait;
import gaffer.store.schema.Schema;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class SimpleGraphConfigurationServiceTest {
    private SimpleGraphConfigurationService service;
    private static final JSONSerialiser serialiser = new JSONSerialiser();

    @Before
    public void setup() {
        final GraphFactory graphFactory = mock(GraphFactory.class);
        final Store store = mock(Store.class);
        final Schema schema = mock(Schema.class);
        final Set<StoreTrait> traits = new HashSet<>(Arrays.asList(AGGREGATION, FILTERING, TRANSFORMATION, STORE_VALIDATION));
        given(store.getSchema()).willReturn(schema);
        final Graph graph = new Graph.Builder().store(store).build();
        final Set<Class<? extends Operation>> operations = new HashSet<>();
        operations.add(AddElements.class);
        given(graphFactory.getGraph()).willReturn(graph);
        given(graph.getSupportedOperations()).willReturn(operations);
        given(graph.isSupported(AddElements.class)).willReturn(true);

        given(graph.getStoreTraits()).willReturn(traits);
        service = new SimpleGraphConfigurationService(graphFactory);
    }

    @Test
    public void shouldGetFilterFunctions() throws IOException {
        // When
        final List<Class> classes = service.getFilterFunctions(null);

        // Then
        assertTrue(classes.size() > 0);
    }

    @Test
    public void shouldGetFilterFunctionsWithInputClass() throws IOException {
        // When
        final List<Class> classes = service.getFilterFunctions(String.class.getName());

        // Then
        assertTrue(classes.size() > 0);
    }

    @Test
    public void shouldGetStoreTraits() throws IOException {
        // When
        final Set<StoreTrait> traits = service.getStoreTraits();
        // Then
        assertNotNull(traits);
        assertTrue("Collection size should be 4", traits.size() == 4);
        assertTrue("Collection should contain AGGREGATION trait", traits.contains(AGGREGATION));
        assertTrue("Collection should contain FILTERING trait", traits.contains(FILTERING));
        assertTrue("Collection should contain TRANSFORMATION trait", traits.contains(TRANSFORMATION));
        assertTrue("Collection should contain STORE_VALIDATION trait", traits.contains(STORE_VALIDATION));

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
        final Set<Class<? extends Operation>> supportedOperations = service.getOperations();

        // Then
        assertTrue(supportedOperations.size() > 0);
        assertEquals(1, supportedOperations.size());
    }

    @Test
    public void shouldValidateWhetherOperationIsSupported() throws IOException {
        // When
        final Set<Class<? extends Operation>> supportedOperations = service.getOperations();

        for (final Class<? extends Operation> operationClass : supportedOperations) {
            // Then
            assertTrue(service.isOperationSupported(operationClass));
        }
    }


    @Test
    public void shouldSerialiseAndDeserialiseGetStoreTraits() throws IOException {
        // When
        byte[] bytes = serialiser.serialise(service.getStoreTraits());
        final Set<StoreTrait> traits = serialiser.deserialise(bytes, Set.class);

        // Then
        assertNotNull(traits);
        assertTrue("Collection size should be 4", traits.size() == 4);
        assertTrue("Collection should contain AGGREGATION trait", traits.contains(AGGREGATION.name()));
        assertTrue("Collection should contain FILTERING trait", traits.contains(FILTERING.name()));
        assertTrue("Collection should contain TRANSFORMATION trait", traits.contains(TRANSFORMATION.name()));
        assertTrue("Collection should contain STORE_VALIDATION trait", traits.contains(STORE_VALIDATION.name()));
    }


}
