/*
 * Copyright 2017-2020 Crown Copyright
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

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.reflections.Reflections;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.rest.factory.DefaultGraphFactory;
import uk.gov.gchq.gaffer.rest.factory.GraphFactory;
import uk.gov.gchq.gaffer.rest.service.v2.example.DefaultExamplesFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class ExampleGeneratorTest {

    private final DefaultExamplesFactory generator = new DefaultExamplesFactory();
    private final GraphFactory graphFactory = new DefaultGraphFactory();

    public static Collection<Object[]> instancesToTest() {
        final Reflections reflections = new Reflections("uk.gov.gchq");
        final Set<Class<? extends Operation>> clazzes = reflections.getSubTypesOf(Operation.class);

        return clazzes.stream()
                .filter(clazz -> !clazz.isInterface() && !Modifier.isAbstract(clazz.getModifiers()))
                .map(clazz -> new Object[]{clazz})
                .collect(Collectors.toList());
    }

    @BeforeEach
    public void before(@TempDir Path tempDir) throws IllegalAccessException, NoSuchFieldException, IOException {
        final File storePropertiesFile = Files.createFile(tempDir.resolve("store.properties")).toFile();
        FileUtils.writeLines(storePropertiesFile, IOUtils.readLines(StreamUtil.openStream(ExampleGeneratorTest.class, "store.properties")));
        System.setProperty(SystemProperty.STORE_PROPERTIES_PATH, storePropertiesFile.getAbsolutePath());

        final File schemaFile = Files.createFile(tempDir.resolve("schema.json")).toFile();
        FileUtils.writeLines(schemaFile, IOUtils.readLines(StreamUtil.openStream(ExampleGeneratorTest.class, "/schema/schema.json")));
        System.setProperty(SystemProperty.SCHEMA_PATHS, schemaFile.getAbsolutePath());

        System.setProperty(SystemProperty.GRAPH_ID, "graphId");

        // Manually inject GraphFactory
        final Field field = generator.getClass().getDeclaredField("graphFactory");

        field.setAccessible(true);
        field.set(generator, graphFactory);
    }

    @ParameterizedTest
    @MethodSource("instancesToTest")
    public void shouldBuildOperation(final Class<? extends Operation> opClass) throws InstantiationException, IllegalAccessException, JsonProcessingException {
        // Given
        final Operation operation = generator.generateExample(opClass);

        // Then
        assertThat(operation, notNullValue());
    }

    @ParameterizedTest
    @MethodSource("instancesToTest")
    public void shouldHandleCharField(final Class<? extends Operation> opClass) throws InstantiationException, IllegalAccessException {
        // Given
        final Operation operation = generator.generateExample(ExampleCharOperation.class);

        //Then
        assertThat(operation, notNullValue());
    }

}
