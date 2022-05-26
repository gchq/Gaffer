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

package uk.gov.gchq.gaffer.flink.integration.operation.handler;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.flink.operation.FlinkTest;
import uk.gov.gchq.gaffer.flink.operation.TestFileOutput;
import uk.gov.gchq.gaffer.flink.operation.handler.AddElementsFromFileHandler;
import uk.gov.gchq.gaffer.generator.TestGeneratorImpl;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.mapstore.MapStore;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.operation.impl.add.AddElementsFromFile;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.user.User;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class AddElementsFromFileHandlerIT extends FlinkTest {
    private File file;
    private TestFileOutput testFileOutput;

    @BeforeEach
    public void before() throws IOException {
        String filename = "inputFile.txt";
        file = new File(testFolder.getAbsolutePath(), filename);
        file.delete();
        file.createNewFile();

        FileUtils.write(file, DATA, StandardCharsets.UTF_8);
        MapStore.resetStaticMap();
        testFileOutput = createTestFileOutput();
    }

    @Test
    public void shouldAddElements() throws Exception {
        // Given
        final Graph graph = createGraph();
        final boolean validate = true;
        final boolean skipInvalid = false;

        final AddElementsFromFile op = new AddElementsFromFile.Builder()
                .filename(file.getAbsolutePath())
                .generator(TestGeneratorImpl.class)
                .parallelism(1)
                .validate(validate)
                .skipInvalidElements(skipInvalid)
                .build();

        // When
        graph.execute(op, new User());

        // Then
        verifyElements(String.class, testFileOutput, TestGeneratorImpl.class);
    }

    @Override
    public Store createStore() {
        final Store store = Store.createStore("graphId", SCHEMA, MapStoreProperties.loadStoreProperties("store.properties"));
        store.addOperationHandler(AddElementsFromFile.class, new AddElementsFromFileHandler(testFileOutput));
        return store;
    }

    private TestFileOutput createTestFileOutput() throws IOException {
        return new TestFileOutput(createTemporaryDirectory("testFileOutput").toPath().toString());
    }
}
