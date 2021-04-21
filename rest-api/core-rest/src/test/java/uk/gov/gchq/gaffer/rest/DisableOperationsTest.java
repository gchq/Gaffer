/*
 * Copyright 2016-2020 Crown Copyright
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

package uk.gov.gchq.gaffer.rest;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.impl.add.AddElementsFromFile;
import uk.gov.gchq.gaffer.rest.factory.DefaultGraphFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class DisableOperationsTest {

    @TempDir
    protected static Path tempDir;

    protected File graphConfigPath;
    protected File storePropsPath;
    protected File schemaPath;

    @BeforeEach
    public void before() throws IOException {
        Path temp = Files.createDirectories(tempDir.resolve(UUID.randomUUID().toString()));
        graphConfigPath = Files.createFile(temp.resolve("tmpGraphConfig.json")).toFile();
        storePropsPath = Files.createFile(temp.resolve("tmpStore.properties")).toFile();
        schemaPath = Files.createFile(temp.resolve("tmpSchema.json")).toFile();
        FileUtils.copyURLToFile(getClass().getResource("/graphConfig.json"), graphConfigPath);
        FileUtils.copyURLToFile(getClass().getResource("/store.properties"), storePropsPath);
        FileUtils.copyURLToFile(getClass().getResource("/schema/schema.json"), schemaPath);
    }

    @Test
    public void shouldDisableOperationsUsingOperationDeclarations() {
        Class<? extends Operation>[] disabledOperations = getDisabledOperations();
        // Given
        System.setProperty(SystemProperty.STORE_PROPERTIES_PATH, storePropsPath.getAbsolutePath());
        System.setProperty(SystemProperty.SCHEMA_PATHS, schemaPath.getAbsolutePath());
        System.setProperty(SystemProperty.GRAPH_CONFIG_PATH, graphConfigPath.getAbsolutePath());
        final DefaultGraphFactory factory = new DefaultGraphFactory();

        // When
        final Graph graph = factory.createGraph();

        // Then
        for (final Class<? extends Operation> disabledOperation : disabledOperations) {
            assertFalse(graph.isSupported(disabledOperation),
                    disabledOperation.getSimpleName() + " should not be supported");
        }
    }

    @Test
    public void shouldNotDisableOperationsWhenNotUsingRestApi() {
        Class<? extends Operation>[] disabledOperations = getDisabledOperations();
        // Given
        System.setProperty(SystemProperty.GRAPH_CONFIG_PATH, graphConfigPath.getAbsolutePath());
        System.setProperty(SystemProperty.STORE_PROPERTIES_PATH, storePropsPath.getAbsolutePath());
        System.setProperty(SystemProperty.SCHEMA_PATHS, schemaPath.getAbsolutePath());

        // When
        final Graph graph = new Graph.Builder()
                .config(graphConfigPath.toURI())
                .storeProperties(storePropsPath.toURI())
                .addSchema(schemaPath.toURI())
                .build();

        // Then
        for (final Class<? extends Operation> disabledOperation : disabledOperations) {
            assertTrue(graph.isSupported(disabledOperation),
                    disabledOperation.getSimpleName() + " should be supported");
        }
    }

    protected Class<? extends Operation>[] getDisabledOperations() {
        return new Class[] {AddElementsFromFile.class};
    }
}
