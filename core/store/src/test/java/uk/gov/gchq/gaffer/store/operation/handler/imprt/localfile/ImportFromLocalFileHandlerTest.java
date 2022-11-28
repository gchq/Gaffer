/*
 * Copyright 2022 Crown Copyright
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

package uk.gov.gchq.gaffer.store.operation.handler.imprt.localfile;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.mockito.junit.jupiter.MockitoExtension;

import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.export.localfile.ImportFromLocalFile;
import uk.gov.gchq.gaffer.operation.impl.export.localfile.LocalFileExporter;

import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.operation.handler.export.localfile.ImportFromLocalFileHandler;

import java.io.IOException;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
class ImportFromLocalFileHandlerTest {
    private static final String FILE_PATH = ImportFromLocalFileHandlerTest.class.getResource("/openCypherCsv/openCypherBasicEntitiesAndEdges.csv").getPath();

    @Test
    public void shouldGetInputData() throws IOException, OperationException {
        // Given
        List<String> expectedData = Arrays.asList(
                ":ID,:LABEL,:START_ID,:END_ID,:TYPE",
                "v1,person,,,",
                "v2,software,,,",
                "e1,,v1,v2,created"
        );

        final ImportFromLocalFile importFromLocalFile = new ImportFromLocalFile.Builder()
                .key(FILE_PATH)
                .build();
        final Context context = new Context();
        context.addExporter(new LocalFileExporter());

        ImportFromLocalFileHandler handler = new ImportFromLocalFileHandler();

        // When
        Object dataFromFile =  handler.doOperation(importFromLocalFile, context, null);

        // Then
        assertThat(dataFromFile)
                .usingRecursiveComparison()
                .isEqualTo(expectedData);
    }
}

