/*
 * Copyright 2016-2018 Crown Copyright
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
    @Override
    public Void doOperation(final AddElementsFromCsv operation,
                            final Context context,
                            final Store store) throws OperationException {

        final CsvElementGenerator csvElementGenerator;
        try {
            csvElementGenerator = JSONSerialiser.deserialise(
                    FileUtils.openInputStream(new File(operation.getMappingsFile())),
                    CsvElementGenerator.class
            );
        } catch (final IOException e) {
            throw new OperationException("Cannot create fieldMappings from file " + operation.getMappingsFile() + " " + e.getMessage());
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
                        .generator(csvElementGenerator)
                        .build())
                .then(new AddElements())
                .build(), context);
    }
}
