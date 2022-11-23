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

import uk.gov.gchq.gaffer.data.generator.CsvElementGenerator;
import uk.gov.gchq.gaffer.data.generator.CsvFormat;
import uk.gov.gchq.gaffer.data.generator.NeptuneFormat;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.CsvToElements;

import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(MockitoExtension.class)
class CsvToElementsHandlerTest {

    private List<String> getInputCsv() {
        return Arrays.asList(
                ":ID,:LABEL,:START_ID,:END_ID,:TYPE",
                "v1,person,,,",
                "v2,software,,,",
                "e1,,v1,v2,created"
        );
    }

    @Test
    public void shouldCreateCorrectGenerator() {
        // Given
        CsvElementGenerator expectedGenerator = new CsvElementGenerator.Builder()
                .csvFormat(new NeptuneFormat())
                .nullString("")
                .trim(true)
                .delimiter(',')
                .header(getInputCsv().get(0))
                .build();
        CsvToElementsHandler handler = new CsvToElementsHandler();
        CsvFormat csvFormat = new NeptuneFormat();

        // When
        CsvElementGenerator actualGenerator = handler.createGenerator(getInputCsv(), true, ',', "", csvFormat);

        // Then
        assertThat(expectedGenerator)
                .usingRecursiveComparison()
                .isEqualTo(actualGenerator);
    }

    @Test
    void shouldThrowErrorNotSupplingSupportedCsvFormat(@Mock final Store store) throws IllegalArgumentException, OperationException {
        Context context = new Context();
        final CsvToElements csvToElementsOp = new CsvToElements.Builder()
                .trim(true)
                .delimiter(',')
                .nullString("")
                .input(getInputCsv())
                .build();
        CsvToElementsHandler handler = new CsvToElementsHandler();

        // When Then
        assertThatThrownBy(() -> {
            handler.doOperation(csvToElementsOp, new Context(), store);})
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("CsvToElements operation requires the user to provide a supported CsvFormat");
    }
}
