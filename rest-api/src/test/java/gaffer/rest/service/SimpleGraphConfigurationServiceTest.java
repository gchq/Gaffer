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

import gaffer.data.elementdefinition.schema.DataSchema;
import gaffer.graph.Graph;
import gaffer.rest.GraphFactory;
import gaffer.store.Store;
import java.io.IOException;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;


public class SimpleGraphConfigurationServiceTest {
    private SimpleGraphConfigurationService service;

    @Before
    public void setup() {
        final GraphFactory graphFactory = mock(GraphFactory.class);
        final Store store = mock(Store.class);
        final DataSchema dataSchema = mock(DataSchema.class);
        given(store.getDataSchema()).willReturn(dataSchema);
        final Graph graph = new Graph(store);
        given(graphFactory.getGraph()).willReturn(graph);

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
    public void shouldGetOperations() throws IOException {
        // When
        final List<Class> classes = service.getOperations();

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
}
