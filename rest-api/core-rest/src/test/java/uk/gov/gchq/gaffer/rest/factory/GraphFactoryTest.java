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

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.hook.AddOperationsToChain;
import uk.gov.gchq.gaffer.graph.hook.OperationAuthoriser;
import uk.gov.gchq.gaffer.graph.hook.OperationChainLimiter;
import uk.gov.gchq.gaffer.rest.GraphFactoryForTest;
import uk.gov.gchq.gaffer.rest.SystemProperty;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public class GraphFactoryTest {
    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    @Before
    @After
    public void cleanUp() {
        System.clearProperty(SystemProperty.GRAPH_CONFIG_PATH);
        System.clearProperty(SystemProperty.SCHEMA_PATHS);
        System.clearProperty(SystemProperty.STORE_PROPERTIES_PATH);
    }

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
    public void shouldNotAddGraphConfigWhenSystemPropertyNotSet() throws IOException {
        // Given
        System.setProperty(SystemProperty.STORE_PROPERTIES_PATH, "store.properties");

        final File schemaFile = testFolder.newFile("schema.json");
        FileUtils.writeLines(schemaFile, IOUtils.readLines(StreamUtil.openStream(getClass(), "/schema/schema.json")));
        System.setProperty(SystemProperty.SCHEMA_PATHS, schemaFile.getAbsolutePath());

        System.clearProperty(SystemProperty.GRAPH_CONFIG_PATH);
        final GraphFactory factory = new DefaultGraphFactory();

        // When / Then
        try {
            factory.createGraph();
        } catch (final IllegalArgumentException e) {
            assertEquals("graphId is required", e.getMessage());
        }
    }

    @Test
    public void shouldAddGraphConfigHooksWhenSystemPropertySet() throws IOException {
        // Given
        final File storePropertiesFile = testFolder.newFile("store.properties");
        FileUtils.writeLines(storePropertiesFile, IOUtils.readLines(StreamUtil.openStream(getClass(), "store.properties")));
        System.setProperty(SystemProperty.STORE_PROPERTIES_PATH, storePropertiesFile.getAbsolutePath());

        final File schemaFile = testFolder.newFile("schema.json");
        FileUtils.writeLines(schemaFile, IOUtils.readLines(StreamUtil.openStream(getClass(), "/schema/schema.json")));
        System.setProperty(SystemProperty.SCHEMA_PATHS, schemaFile.getAbsolutePath());

        final File graphConfigFile = testFolder.newFile("graphConfig.json");
        FileUtils.writeLines(graphConfigFile, IOUtils.readLines(StreamUtil.openStream(getClass(), "graphConfigWithHooks.json")));
        System.setProperty(SystemProperty.GRAPH_CONFIG_PATH, graphConfigFile.getAbsolutePath());

        final GraphFactory factory = new DefaultGraphFactory();

        // When
        final Graph graph = factory.createGraph();

        // Then
        assertEquals(Arrays.asList(
                OperationChainLimiter.class,
                AddOperationsToChain.class,
                OperationAuthoriser.class
        ), graph.getGraphHooks());

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
