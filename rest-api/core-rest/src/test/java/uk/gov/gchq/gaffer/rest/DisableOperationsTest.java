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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.impl.add.AddElementsFromFile;
import uk.gov.gchq.gaffer.rest.factory.DefaultGraphFactory;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class DisableOperationsTest {
    @TempDir
    public File tempFolder;

    protected final Class<? extends Operation>[] disabledOperations;
    protected File graphConfigPath;
    protected File storePropsPath;
    protected File schemaPath;

    static String originalGraphConfigPath;
    static String originalStorePropsPath;
    static String originalSchemaPath;

    public DisableOperationsTest() throws IOException {
        this(AddElementsFromFile.class);
    }

    @SafeVarargs
    protected DisableOperationsTest(final Class<? extends Operation>... disabledOperations) throws IOException {
        this.disabledOperations = disabledOperations;
    }

    @BeforeAll
    public static void beforeAll() {
        originalGraphConfigPath = System.getProperty(SystemProperty.GRAPH_CONFIG_PATH);
        originalSchemaPath = System.getProperty(SystemProperty.SCHEMA_PATHS);
        originalStorePropsPath = System.getProperty(SystemProperty.STORE_PROPERTIES_PATH);
    }

    @AfterAll
    public static void afterAll() {
        setSystemPaths(originalStorePropsPath, originalGraphConfigPath, originalSchemaPath);
    }

    @BeforeEach
    public void before() throws IOException {
        graphConfigPath = new File(tempFolder, "tmpGraphConfig.json");
        storePropsPath = new File(tempFolder, "tmpStore.properties");
        schemaPath = new File(tempFolder, "tmpSchema.json");
        FileUtils.copyURLToFile(getClass().getResource("/graphConfig.json"), graphConfigPath);
        FileUtils.copyURLToFile(getClass().getResource("/store.properties"), storePropsPath);
        FileUtils.copyURLToFile(getClass().getResource("/schema/schema.json"), schemaPath);
    }

    @Test
    public void shouldDisableOperationsUsingOperationDeclarations() {
        // Given
        setSystemPaths(storePropsPath.getAbsolutePath(), graphConfigPath.getAbsolutePath(), schemaPath.getAbsolutePath());
        final DefaultGraphFactory factory = new DefaultGraphFactory();

        // When
        final Graph graph = factory.createGraph();

        // Then
        for (final Class<? extends Operation> disabledOperation : disabledOperations) {
            assertFalse(graph.isSupported(disabledOperation), disabledOperation.getSimpleName() + " should not be supported");
        }
    }

    @Test
    public void shouldNotDisableOperationsWhenNotUsingRestApi() {
        // Given
        setSystemPaths(storePropsPath.getAbsolutePath(), graphConfigPath.getAbsolutePath(), schemaPath.getAbsolutePath());

        // When
        final Graph graph = new Graph.Builder()
                .config(graphConfigPath.toURI())
                .storeProperties(storePropsPath.toURI())
                .addSchema(schemaPath.toURI())
                .build();

        // Then
        for (final Class<? extends Operation> disabledOperation : disabledOperations) {
            assertTrue(graph.isSupported(disabledOperation), disabledOperation.getSimpleName() + " should be supported");
        }
    }

    private static void setSystemPaths(final String storeProps, final String graphConfig, final String schema) {

        if (null == storeProps || storeProps.isEmpty()) {
            System.clearProperty(SystemProperty.STORE_PROPERTIES_PATH);
        } else {

            System.setProperty(SystemProperty.STORE_PROPERTIES_PATH, storeProps);
        }

        if (null == graphConfig || graphConfig.isEmpty()) {
            System.clearProperty(SystemProperty.GRAPH_CONFIG_PATH);
        } else {

            System.setProperty(SystemProperty.GRAPH_CONFIG_PATH, graphConfig);
        }

        if (null == schema || schema.isEmpty()) {
            System.clearProperty(SystemProperty.SCHEMA_PATHS);
        } else {

            System.setProperty(SystemProperty.SCHEMA_PATHS, schema);
        }
    }
}
