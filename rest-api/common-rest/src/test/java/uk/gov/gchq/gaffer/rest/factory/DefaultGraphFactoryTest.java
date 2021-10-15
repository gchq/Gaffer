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

package uk.gov.gchq.gaffer.rest.factory;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.rest.SystemProperty;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class DefaultGraphFactoryTest {

    @BeforeEach
    @AfterEach
    public void clearSystemProperties() {
        System.clearProperty(SystemProperty.SCHEMA_PATHS);
        System.clearProperty(SystemProperty.STORE_PROPERTIES_PATH);
        System.clearProperty(SystemProperty.GRAPH_CONFIG_PATH);
        System.clearProperty(SystemProperty.GRAPH_LIBRARY_CLASS);
    }

    @Test
    public void shouldThrowRuntimeExceptionIfGraphLibraryClassDoesNotExist() {
        // Given
        String schemaPath = getClass().getResource("/schema").getPath();
        String storePropsPath = getClass().getResource("/store.properties").getPath();
        String graphConfigPath = getClass().getResource("/graphConfigWithHooks.json").getPath();

        System.setProperty(SystemProperty.SCHEMA_PATHS, schemaPath);
        System.setProperty(SystemProperty.STORE_PROPERTIES_PATH, storePropsPath);
        System.setProperty(SystemProperty.GRAPH_CONFIG_PATH, graphConfigPath);

        // When
        GraphFactory graphFactory = DefaultGraphFactory.createGraphFactory();
        System.setProperty(SystemProperty.GRAPH_LIBRARY_CLASS, "invalid.class.name");

        // Then
        assertThatExceptionOfType(RuntimeException.class)
                .isThrownBy(graphFactory::getGraph)
                .withMessage("Error creating GraphLibrary class");
    }
}
