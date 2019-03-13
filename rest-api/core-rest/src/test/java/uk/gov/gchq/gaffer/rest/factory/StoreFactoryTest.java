/*
 * Copyright 2016-2019 Crown Copyright
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
import uk.gov.gchq.gaffer.graph.hook.AddOperationsToChain;
import uk.gov.gchq.gaffer.graph.hook.NamedOperationResolver;
import uk.gov.gchq.gaffer.graph.hook.NamedViewResolver;
import uk.gov.gchq.gaffer.graph.hook.OperationAuthoriser;
import uk.gov.gchq.gaffer.graph.hook.OperationChainLimiter;
import uk.gov.gchq.gaffer.rest.SystemProperty;
import uk.gov.gchq.gaffer.store.Store;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public class StoreFactoryTest {
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
    public void shouldCreateDefaultStoreFactoryWhenNoSystemProperty() {
        // Given
        System.clearProperty(SystemProperty.STORE_FACTORY_CLASS);

        // When
        final StoreFactory storeFactory = StoreFactory.createStoreFactory();

        // Then
        assertEquals(DefaultStoreFactory.class, storeFactory.getClass());
    }

    @Test
    public void shouldCreateGraphFactoryFromSystemPropertyClassName() {
        // Given
        System.setProperty(SystemProperty.STORE_FACTORY_CLASS,
                StoreFactoryForTest.class.getName());

        // When
        final StoreFactory storeFactory = StoreFactory.createStoreFactory();

        // Then
        assertEquals(StoreFactoryForTest.class, storeFactory.getClass());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionFromInvalidSystemPropertyClassName() {
        // Given
        System.setProperty(SystemProperty.STORE_FACTORY_CLASS, "InvalidClassName");

        // When
        final StoreFactory storeFactory = StoreFactory.createStoreFactory();

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
        final StoreFactory factory = new DefaultStoreFactory();

        // When / Then
        try {
            factory.createStore();
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

        final StoreFactory factory = new DefaultStoreFactory();

        // When
        final Store store = factory.createStore();

        // Then
        assertEquals(Arrays.asList(
                NamedOperationResolver.class,
                NamedViewResolver.class,
                OperationChainLimiter.class,
                AddOperationsToChain.class,
                OperationAuthoriser.class
        ), store.getConfig().getHooks());

    }

    @Test
    public void shouldDefaultToSingletonGraph() {
        // Given
        final DefaultStoreFactory factory = new DefaultStoreFactory();

        // When
        final boolean isSingleton = factory.isSingletonStore();

        // Then
        assertTrue(isSingleton);
    }
}
