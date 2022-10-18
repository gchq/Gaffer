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

import uk.gov.gchq.gaffer.data.generator.CsvElementGenerator;
import uk.gov.gchq.gaffer.data.generator.CsvFormat;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.add.CsvToElements;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;


/**
 * An {@code CsvToElementsHandler} handles the {@link CsvToElements} operation.
 *
 * It takes an iterable of Strings, generating from them elements using the
 * {@link CsvElementGenerator}.
 *
 * @see <a href="https://docs.aws.amazon.com/neptune/latest/userguide/bulk-load-tutorial-format-opencypher.html">openCypher</a>
 */
public class CsvToElementsHandler implements OperationHandler<CsvToElements> {

    @Override
    public Void doOperation(final CsvToElements operation,
                            final Context context,
                            final Store store) throws OperationException {

        CsvElementGenerator generator = createGenerator(operation);
        if (generator.getCsvFormat() == null || generator.getCsvFormat().getClass().getSuperclass() != CsvFormat.class) {
            throw new IllegalArgumentException("CsvToElements operation requires the user to provide a supported CsvFormat");
        }
        final Iterable<? extends String> input = operation.getInput();

        return store.execute(new OperationChain.Builder()
                .first(new GenerateElements.Builder<String>()
                        .input(input)
                        .generator(generator)
                        .build())
                .then(new AddElements())
                .build(), context);
    }



    CsvElementGenerator createGenerator(final Iterable<? extends String> lines, final boolean trim, final char delimiter, final String nullString, final CsvFormat csvFormat) {
        String header = lines.iterator().next();
        return new CsvElementGenerator.Builder()
                .header(header)
                .delimiter(delimiter)
                .trim(trim)
                .nullString(nullString)
                .csvFormat(csvFormat)
                .build();
    }

    CsvElementGenerator createGenerator(final CsvToElements operation) {
        return createGenerator(operation.getInput(), operation.isTrim(), operation.getDelimiter(), operation.getNullString(), operation.getCsvFormat());
    }
}

