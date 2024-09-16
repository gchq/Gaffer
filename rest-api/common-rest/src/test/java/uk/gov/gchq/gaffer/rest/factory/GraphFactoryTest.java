/*
 * Copyright 2016-2023 Crown Copyright
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.hook.AddOperationsToChain;
import uk.gov.gchq.gaffer.graph.hook.FunctionAuthoriser;
import uk.gov.gchq.gaffer.graph.hook.NamedOperationResolver;
import uk.gov.gchq.gaffer.graph.hook.NamedViewResolver;
import uk.gov.gchq.gaffer.graph.hook.OperationAuthoriser;
import uk.gov.gchq.gaffer.graph.hook.OperationChainLimiter;
import uk.gov.gchq.gaffer.rest.SystemProperty;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GraphFactoryTest {

    @BeforeEach
    @AfterEach
    public void cleanUp() {
        System.clearProperty(SystemProperty.GRAPH_CONFIG_PATH);
        System.clearProperty(SystemProperty.SCHEMA_PATHS);
        System.clearProperty(SystemProperty.STORE_PROPERTIES_PATH);
        System.clearProperty(SystemProperty.GRAPH_FACTORY_CLASS);
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

    @Test
    public void shouldThrowExceptionFromInvalidSystemPropertyClassName() {
        // Given
        System.setProperty(SystemProperty.GRAPH_FACTORY_CLASS, "InvalidClassName");

        // When
        assertThatIllegalArgumentException().isThrownBy(() -> GraphFactory.createGraphFactory());
    }

    @Test
    public void shouldNotAddGraphConfigWhenSystemPropertyNotSet(@TempDir Path tempDir)
            throws IOException {
        // Given
        System.setProperty(SystemProperty.STORE_PROPERTIES_PATH, "store.properties");

        final File schemaFile = Files.createFile(tempDir.resolve("schema.json")).toFile();
        FileUtils.writeLines(schemaFile, IOUtils.readLines(StreamUtil.openStream(getClass(), "/schema/schema.json"), StandardCharsets.UTF_8));
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
    public void shouldAddGraphConfigHooksWhenSystemPropertySet(@TempDir Path tempDir)
            throws IOException {
        // Given
        final File storePropertiesFile = Files.createFile(tempDir.resolve("store.properties")).toFile();
        FileUtils.writeLines(storePropertiesFile, IOUtils.readLines(StreamUtil.openStream(getClass(), "store.properties"), StandardCharsets.UTF_8));
        System.setProperty(SystemProperty.STORE_PROPERTIES_PATH, storePropertiesFile.getAbsolutePath());

        final File schemaFile = Files.createFile(tempDir.resolve("schema.json")).toFile();
        FileUtils.writeLines(schemaFile, IOUtils.readLines(StreamUtil.openStream(getClass(), "/schema/schema.json"), StandardCharsets.UTF_8));
        System.setProperty(SystemProperty.SCHEMA_PATHS, schemaFile.getAbsolutePath());

        final File graphConfigFile = Files.createFile(tempDir.resolve("graphConfig.json")).toFile();
        FileUtils.writeLines(graphConfigFile, IOUtils.readLines(StreamUtil.openStream(getClass(), "graphConfigWithHooks.json"), StandardCharsets.UTF_8));
        System.setProperty(SystemProperty.GRAPH_CONFIG_PATH, graphConfigFile.getAbsolutePath());

        final GraphFactory factory = new DefaultGraphFactory();

        // When
        final Graph graph = factory.createGraph();

        // Then
        assertThat(graph.getGraphHooks())
            .containsExactlyInAnyOrder(
                NamedOperationResolver.class,
                NamedViewResolver.class,
                OperationChainLimiter.class,
                AddOperationsToChain.class,
                OperationAuthoriser.class,
                FunctionAuthoriser.class);
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
