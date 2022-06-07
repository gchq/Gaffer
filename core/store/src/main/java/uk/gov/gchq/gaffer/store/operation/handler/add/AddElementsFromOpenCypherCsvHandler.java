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

import org.apache.commons.io.FileUtils;

import uk.gov.gchq.gaffer.data.generator.OpenCypherCsvElementGenerator;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.add.AddElementsFromOpenCypherCsv;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.stream.Collectors;


public class AddElementsFromOpenCypherCsvHandler implements OperationHandler<AddElementsFromOpenCypherCsv> {

    @Override
    public Void doOperation(final AddElementsFromOpenCypherCsv operation,
                            final Context context,
                            final Store store) throws OperationException {
        Iterable<String> data;
        try {
            data = getInputData(operation.getFilename());
        } catch (final IOException e) {
            throw new OperationException(e.getMessage());
        }

        OpenCypherCsvElementGenerator generator = createGenerator(data, operation);

        return store.execute(new OperationChain.Builder()
                .first(new GenerateElements.Builder<String>()
                        .input(data)
                        .generator(generator)
                        .build())
                .then(new AddElements())
                .build(), context);
    }

     Iterable<String> getInputData(final String filename) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(FileUtils.openInputStream(new File(filename))))) {
            return reader.lines().collect(Collectors.toList());
        }
    }

    OpenCypherCsvElementGenerator createGenerator(final Iterable<String> lines, final boolean trim, final char delimiter, final String nullString) {
        String header = lines.iterator().next();
        return new OpenCypherCsvElementGenerator.Builder()
                .header(header)
                .delimiter(delimiter)
                .trim(trim)
                .nullString(nullString)
                .build();
    }

    OpenCypherCsvElementGenerator createGenerator(final Iterable<String> lines, final AddElementsFromOpenCypherCsv operation) {
        return createGenerator(lines, operation.isTrim(), operation.getDelimiter(), operation.getNullString());
    }
}
