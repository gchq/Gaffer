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

package uk.gov.gchq.gaffer.store.operation.handler.add;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;

import org.mockito.junit.jupiter.MockitoExtension;

import uk.gov.gchq.gaffer.data.generator.OpenCypherCsvElementGenerator;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.ImportCsv;

import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;

import java.io.IOException;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class ImportCsvHandlerTest {
    private static final String FILE_NAME = ImportCsvHandlerTest.class.getResource("/openCypherCsv/openCypherBasicEntitiesAndEdges.csv").getPath();
    public static final String HEADER = ":ID,:LABEL,:START_ID,:END_ID,:TYPE";

    @Test
    public void shouldAddElements(@Mock final Store store) throws Exception {
        // Given
        Context context = new Context();
        final ImportCsv importCsvOp = new ImportCsv.Builder()
                .filename(FILE_NAME)
                .build();

        // When
        ImportCsvHandler handler = new ImportCsvHandler();
        handler.doOperation(importCsvOp, context, store);

        // Then
        verify(store).execute(any(OperationChain.class), eq(context));
    }

    @Test
    public void shouldGetInputData() throws IOException {
        // Given
        List<String> data = Arrays.asList(
                HEADER,
                "v1,person,,,",
                "v2,software,,,",
                "e1,,v1,v2,created"
                );

        ImportCsvHandler handler = new ImportCsvHandler();

        // When
        Iterable<String> lines = handler.getInputData(FILE_NAME);

        // Then
        assertThat(data)
                .usingRecursiveComparison()
                .isEqualTo(lines);
    }

    @Test
    public void shouldCreateCorrectGenerator() {
        // Given
        OpenCypherCsvElementGenerator expectedGenerator = new OpenCypherCsvElementGenerator.Builder()
                .header(HEADER)
                .build();
        ImportCsvHandler handler = new ImportCsvHandler();
        List<String> header = Arrays.asList(HEADER);

        // When
        OpenCypherCsvElementGenerator actualGenerator = handler.createGenerator(header, false, ',', "");

        // Then
        assertThat(expectedGenerator)
                .usingRecursiveComparison()
                .isEqualTo(actualGenerator);
    }

    @Test
    void shouldThrowErrorUnsupportedHeaderType(@Mock final Store store) throws IOException {
        //Given
        Context context = new Context();
        final ImportCsv importCsvOp = new ImportCsv.Builder()
                .filename("nonExistentFile.csv")
                .build();
        ImportCsvHandler handler = new ImportCsvHandler();

        //When / Then
        assertThatExceptionOfType(OperationException.class)
                .isThrownBy(() -> handler.doOperation(importCsvOp, context, store));
    }
}
