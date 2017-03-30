/*
 * Copyright 2016-2017 Crown Copyright
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

package uk.gov.gchq.gaffer.rest.factory;

import org.junit.Test;
import uk.gov.gchq.gaffer.graph.hook.OperationAuthoriser;
import uk.gov.gchq.gaffer.rest.GraphFactoryForTest;
import uk.gov.gchq.gaffer.rest.SystemProperty;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public class GraphFactoryTest {
    @Test
    public void shouldCreateDefaultGraphFactoryWhenNoSystemProperty() {
        // Given
        System.clearProperty(SystemProperty.GRAPH_FACTORY_CLASS);

        // When
        final GraphFactory graphFactory = GraphFactory.createGraphFactory();

        // Then
        assertEquals(DefaultGraphFactory.class, graphFactory.getClass());
    }

    @Test
    public void shouldCreateGraphFactoryFromSystemPropertyClassName() {
        // Given
        System.setProperty(SystemProperty.GRAPH_FACTORY_CLASS, GraphFactoryForTest.class.getName());

        // When
        final GraphFactory graphFactory = GraphFactory.createGraphFactory();

        // Then
        assertEquals(GraphFactoryForTest.class, graphFactory.getClass());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionFromInvalidSystemPropertyClassName() {
        // Given
        System.setProperty(SystemProperty.GRAPH_FACTORY_CLASS, "InvalidClassName");

        // When
        final GraphFactory graphFactory = GraphFactory.createGraphFactory();

        // Then
        fail();
    }

    @Test
    public void shouldReturnNullWhenCreateOpAuthoriserWithNoSystemPropertyPath() {
        // Given
        System.clearProperty(SystemProperty.OP_AUTHS_PATH);
        final GraphFactory factory = new DefaultGraphFactory();

        // When
        final OperationAuthoriser opAuthoriser = factory.createOpAuthoriser();

        // Then
        assertNull(opAuthoriser);
    }

    @Test
    public void shouldDefaultToSingletonGraph() {
        // Given
        final DefaultGraphFactory factory = new DefaultGraphFactory();

        // When
        final boolean isSingleton = factory.isSingletonGraph();

        // Then
        assertTrue(isSingleton);
    }
}
