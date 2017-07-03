/*
 * Copyright 2017 Crown Copyright
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

package uk.gov.gchq.gaffer.flink.operation.handler;

import com.google.common.collect.Sets;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import uk.gov.gchq.gaffer.cache.exception.CacheOperationException;
import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.commonutil.TestTypes;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.generator.OneToOneElementGenerator;
import uk.gov.gchq.gaffer.flink.operation.AddElementsFromFile;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.impl.binaryoperator.Sum;
import java.io.File;
import java.io.IOException;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class AddElementsFromFileHandlerTest {
    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);
    private File file;

    @Before
    public void before() throws IOException {
        file = testFolder.newFile("inputFile.txt");
        FileUtils.write(file, "1\n2\n3");
    }

    @Test
    public void shouldAddElementsFromFile() throws CacheOperationException, OperationException {
        // Given
        final boolean validate = true;
        final boolean skipInvalid = false;
        final Graph graph = new Graph.Builder()
                .addSchemas(new Schema.Builder()
                        .type(TestTypes.ID_STRING, new TypeDefinition.Builder()
                                .clazz(String.class)
                                .build())
                        .type(TestTypes.PROP_COUNT, new TypeDefinition.Builder()
                                .clazz(Long.class)
                                .aggregateFunction(new Sum())
                                .build())
                        .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                                .vertex(TestTypes.ID_STRING)
                                .property(TestPropertyNames.COUNT, TestTypes.PROP_COUNT)
                                .build())
                        .build())
                .storeProperties("store.properties")
                .build();

        final AddElementsFromFile op = new AddElementsFromFile.Builder()
                .filename(file.getAbsolutePath())
                .jobName("test import from file")
                .generator(BasicGenerator.class)
                .parallelism(1)
                .validate(validate)
                .skipInvalidElements(skipInvalid)
                .build();

        // When
        graph.execute(op, new User());

        // Then
        final Set<Element> allElements = Sets.newHashSet(graph.execute(new GetAllElements(), new User()));
        assertEquals(Sets.newHashSet(
                new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex("1")
                        .property(TestPropertyNames.COUNT, 1L)
                        .build(),
                new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex("2")
                        .property(TestPropertyNames.COUNT, 1L)
                        .build(),
                new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex("3")
                        .property(TestPropertyNames.COUNT, 1L)
                        .build()
        ), allElements);
    }

    public static final class BasicGenerator implements OneToOneElementGenerator<String> {
        @Override
        public Element _apply(final String domainObject) {
            System.out.println("got item: " + domainObject);
            return new Entity.Builder()
                    .group(TestGroups.ENTITY)
                    .vertex(domainObject)
                    .property(TestPropertyNames.COUNT, 1L)
                    .build();
        }
    }
}
