/*
 * Copyright 2020-2022 Crown Copyright
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

import java.io.File;

class DefaultGraphFactoryTest {

    @BeforeEach
    @AfterEach
    void clearSystemProperties() {
        System.clearProperty(SystemProperty.SCHEMA_PATHS);
        System.clearProperty(SystemProperty.STORE_PROPERTIES_PATH);
        System.clearProperty(SystemProperty.GRAPH_CONFIG_PATH);
    }

    @Test
    void shouldThrowRuntimeExceptionIfGraphLibraryClassDoesNotExist() {
        // Given
        // Need to use getAbsolutePath so the test works on Windows
        File schemaFile = new File(getClass().getResource("/schema").getFile());
        String schemaPath = schemaFile.getAbsolutePath();
        File propsFile = new File(getClass().getResource("/store.properties").getFile());
        String storePropsPath = propsFile.getAbsolutePath();

        System.setProperty(SystemProperty.SCHEMA_PATHS, schemaPath);
        System.setProperty(SystemProperty.STORE_PROPERTIES_PATH, storePropsPath);

        // When
        GraphFactory graphFactory = DefaultGraphFactory.createGraphFactory();

        File graphFile = new File(getClass().getResource("/graphConfigIncorrectLibrary.json").getFile());
        String graphConfigPath = graphFile.getAbsolutePath();
        System.setProperty(SystemProperty.GRAPH_CONFIG_PATH, graphConfigPath);
        
        // Then
        assertThatExceptionOfType(RuntimeException.class)
                .isThrownBy(graphFactory::getGraph)
                .withMessage("Unable to deserialise graph config");
    }
}
