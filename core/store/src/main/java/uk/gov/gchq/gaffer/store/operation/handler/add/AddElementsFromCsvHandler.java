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

public class AddElementsFromCsvHandler implements OperationHandler<AddElementsFromCsv> {

    private final String NONE = "none";

    @Override
    public Void doOperation(final AddElementsFromCsv operation,
                            final Context context,
                            final Store store) throws OperationException {

        if(operation.getElementGeneratorClassName().equals(NONE) && operation.getElementGeneratorFilePath().equals(NONE) && operation.getElementGeneratorJson().equals("none")){
            throw new IllegalArgumentException("You must specify either and element generator classname, and element generator file path or some element generator json");
        }

        ElementGenerator generator = null;

        if(!operation.getElementGeneratorJson().equals(NONE)){
            try {
                generator = (ElementGenerator) Class.forName(operation.getElementGeneratorClassName()).newInstance();
            } catch (InstantiationException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }else if(!operation.getElementGeneratorFilePath().equals(NONE)){

            CsvElementGenerator csvElementGenerator;
            try {
                csvElementGenerator = JSONSerialiser.deserialise(
                        FileUtils.openInputStream(new File(operation.getElementGeneratorFilePath())),
                        CsvElementGenerator.class
                );
            } catch (final IOException e) {
                throw new OperationException("Cannot create fieldMappings from file " + operation.getElementGeneratorFilePath() + " " + e.getMessage());
            }

            char delimiter = operation.getDelimiter().charAt(0);
            boolean quoted = operation.isQuoted();
            char quoteChar = operation.getQuoteChar().charAt(0);

            csvElementGenerator.setDelimiter(delimiter);
            csvElementGenerator.setQuoted(quoted);
            csvElementGenerator.setQuoteChar(quoteChar);

            generator = csvElementGenerator;

        }else if(!operation.getElementGeneratorJson().equals(NONE)){

            CsvElementGenerator csvElementGenerator = null;
            try {
                csvElementGenerator = JSONSerialiser.deserialise(operation.getElementGeneratorJson(), CsvElementGenerator.class);
            } catch (SerialisationException e) {
                e.printStackTrace();
            }

            char delimiter = operation.getDelimiter().charAt(0);
            boolean quoted = operation.isQuoted();
            char quoteChar = operation.getQuoteChar().charAt(0);

            csvElementGenerator.setDelimiter(delimiter);
            csvElementGenerator.setQuoted(quoted);
            csvElementGenerator.setQuoteChar(quoteChar);

            generator = csvElementGenerator;
        }


        final Iterable<String> data = () -> {
            try {
                return FileUtils.lineIterator(new File(operation.getFilename()));
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        };

        return store.execute(new OperationChain.Builder()
                .first(new GenerateElements.Builder<String>()
                        .input(data)
                        .generator(generator)
                        .build())
                .then(new AddElements())
                .build(), context);
    }
}
