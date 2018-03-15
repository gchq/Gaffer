/*
 * Copyright 2017-2018 Crown Copyright
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
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.reflections.Reflections;

import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.rest.factory.DefaultGraphFactory;
import uk.gov.gchq.gaffer.rest.factory.GraphFactory;
import uk.gov.gchq.gaffer.rest.service.v2.example.DefaultExamplesFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@RunWith(Parameterized.class)
public class ExampleGeneratorTest {

    private final DefaultExamplesFactory generator = new DefaultExamplesFactory();
    private final GraphFactory graphFactory = new DefaultGraphFactory();
    private final Class<? extends Operation> opClass;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    public ExampleGeneratorTest(final Class<? extends Operation> opClass) {
        this.opClass = opClass;
    }

    @Parameters
    public static Collection<Object[]> instancesToTest() {
        final Reflections reflections = new Reflections("uk.gov.gchq");
        final Set<Class<? extends Operation>> clazzes = reflections.getSubTypesOf(Operation.class);

        return clazzes.stream()
                .filter(clazz -> !clazz.isInterface() && !Modifier.isAbstract(clazz.getModifiers()))
                .map(clazz -> new Object[]{clazz})
                .collect(Collectors.toList());
    }

    @Before
    public void before() throws IllegalAccessException, NoSuchFieldException, IOException {
        final File storePropertiesFile = tempFolder.newFile("store.properties");
        FileUtils.writeLines(storePropertiesFile, IOUtils.readLines(StreamUtil.openStream(ExampleGeneratorTest.class, "store.properties")));
        System.setProperty(SystemProperty.STORE_PROPERTIES_PATH, storePropertiesFile.getAbsolutePath());

        final File schemaFile = tempFolder.newFile("schema.json");
        FileUtils.writeLines(schemaFile, IOUtils.readLines(StreamUtil.openStream(ExampleGeneratorTest.class, "/schema/schema.json")));
        System.setProperty(SystemProperty.SCHEMA_PATHS, schemaFile.getAbsolutePath());

        System.setProperty(SystemProperty.GRAPH_ID, "graphId");

        // Manually inject GraphFactory
        final Field field = generator.getClass().getDeclaredField("graphFactory");

        field.setAccessible(true);
        field.set(generator, graphFactory);
    }

    @Test
    public void shouldBuildOperation() throws InstantiationException, IllegalAccessException, JsonProcessingException {
        // Given
        final Operation operation = generator.generateExample(opClass);

        // Then
        assertThat(operation, notNullValue());
    }

}
