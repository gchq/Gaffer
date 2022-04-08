/*
 * Copyright 2016-2021 Crown Copyright
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

import uk.gov.gchq.gaffer.data.generator.CsvElementGenerator;
import uk.gov.gchq.gaffer.data.generator.ElementGenerator;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.add.AddElementsFromCsv;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

public class AddElementsFromCsvHandler implements OperationHandler<AddElementsFromCsv> {

    @Override
    public Void doOperation(final AddElementsFromCsv operation,
                            final Context context,
                            final Store store) throws OperationException {
        ElementGenerator<String> generator = null;

        if (null != operation.getElementGeneratorClassName()) {
            try {
                generator = (ElementGenerator<String>) Class.forName(operation.getElementGeneratorClassName()).newInstance();
            } catch (final Exception e) {
                throw new OperationException("Failed to create CsvElementGenerator with name: " + operation.getElementGeneratorClassName(), e);
            }
        } else {
            CsvElementGenerator csvElementGenerator;
            if (null != operation.getElementGeneratorFilePath()) {
                try {
                    csvElementGenerator = JSONSerialiser.deserialise(
                            FileUtils.openInputStream(new File(operation.getElementGeneratorFilePath())),
                            CsvElementGenerator.class
                    );
                } catch (final IOException e) {
                    throw new OperationException("Failed to create CsvElementGenerator from file: " + operation.getElementGeneratorFilePath(), e);
                }
            } else if (null != operation.getElementGeneratorJson()) {
                try {
                    csvElementGenerator = JSONSerialiser.deserialise(operation.getElementGeneratorJson(), CsvElementGenerator.class);
                } catch (final SerialisationException e) {
                    throw new OperationException("Failed to create CsvElementGenerator from json: " + operation.getElementGeneratorJson(), e);
                }
            } else {
                throw new IllegalArgumentException("You must specify either an element generator classname, an element generator file path or some element generator json");
            }
            csvElementGenerator.setDelimiter(operation.getDelimiter());
            csvElementGenerator.setQuoted(operation.isQuoted());
            csvElementGenerator.setQuoteChar(operation.getQuoteChar());
            generator = csvElementGenerator;
        }

        Iterator<String> lines;
        try {
            lines = FileUtils.lineIterator(new File(operation.getFilename()));
        } catch (final IOException e) {
            throw new OperationException("Failed to get csv lines from file: " + operation.getFilename(), e);
        }
        final Iterable<String> data = () -> {
            return lines;
        };

        return store.execute(new OperationChain.Builder()
                .first(new GenerateElements.Builder<String>()
                        .input(data)
                        .generator(generator)
                        .build())
                .then(new AddElements.Builder()
                        .validate(operation.isValidate())
                        .skipInvalidElements(operation.isSkipInvalidElements())
                        .build())
                .build(), context);
    }
}
