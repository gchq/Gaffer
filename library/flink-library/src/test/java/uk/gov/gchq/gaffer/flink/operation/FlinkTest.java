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

package uk.gov.gchq.gaffer.flink.operation;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.junit.jupiter.api.io.TempDir;

import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;
import uk.gov.gchq.gaffer.commonutil.StringUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.util.ElementUtil;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.TestTypes;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.koryphe.impl.binaryoperator.Sum;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toList;

public abstract class FlinkTest {
    @TempDir
    public final File testFolder = CommonTestConstants.TMP_DIRECTORY;

    public static final Schema SCHEMA = new Schema.Builder()
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
            .entity(TestGroups.ENTITY_2, new SchemaEntityDefinition.Builder()
                    .vertex(TestTypes.ID_STRING)
                    .property(TestPropertyNames.COUNT, TestTypes.PROP_COUNT)
                    .aggregate(false)
                    .build())
            .build();

    public static final String[] DATA_VALUES = {"1", "1", "2", "3", "1", "2"};
    public static final String DATA = StringUtils.join(DATA_VALUES, "\n");
    public static final byte[] DATA_BYTES = StringUtil.toBytes(DATA);

    public Graph createGraph() {
        return new Graph.Builder()
                .store(createStore())
                .build();
    }

    public abstract Store createStore();

    public <T> boolean waitForElements(
            final Class<T> consumeAs,
            final ElementFileStore elementFileStore,
            final Class<? extends Function<Iterable<? extends T>, Iterable<? extends Element>>> generator) throws Exception {
        return elementFileStore.getFileIds().size() == listOfExpectedElements(consumeAs, generator).size();
    }

    public <T> void verifyElements(
            final Class<T> consumeAs,
            final ElementFileStore elementFileStore,
            final Class<? extends Function<Iterable<? extends T>, Iterable<? extends Element>>> generator) throws Exception {
        ElementUtil.assertElementEquals(generator.newInstance().apply(dataValuesAsListOfType(consumeAs)), elementFileStore.getElements());
    }

    private <T> List<? extends Element> listOfExpectedElements(
            final Class<T> consumeAs,
            final Class<? extends Function<Iterable<? extends T>, Iterable<? extends Element>>> generator) throws Exception {
        return StreamSupport.stream(generator.newInstance().apply(dataValuesAsListOfType(consumeAs)).spliterator(), false).collect(toList());
    }

    private <T> List<T> dataValuesAsListOfType(final Class<T> consumeAs) {
        return consumeAs == String.class ? (List<T>) Stream.of(DATA_VALUES).collect(toList()) : (List<T>) (Stream.of(DATA_VALUES).map(String::getBytes).collect(toList()));
    }

    protected File createTemporaryDirectory(final String directoryName) throws IOException {
        File directory = new File(testFolder, directoryName);
        if (directory.exists()) {
            FileUtils.forceDelete(directory);
        }
        if (!directory.mkdir()) {
            throw new IOException("Error creating temp directory '" + directoryName);
        }
        return directory;
    }

    protected TestFileSink createTestFileSink() throws IOException {
        return new TestFileSink(createTemporaryDirectory("testFileSink").toPath().toString());
    }
}
