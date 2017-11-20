/*
 * Copyright 2016 Crown Copyright
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
package uk.gov.gchq.gaffer.store.operation.handler;

import org.apache.commons.io.FileUtils;

import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.SplitStoreFromFile;
import uk.gov.gchq.gaffer.operation.impl.SplitStoreFromIterable;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class SplitStoreFromFileHandler implements OperationHandler<SplitStoreFromFile> {
    @Override
    public Void doOperation(final SplitStoreFromFile operation,
                            final Context context, final Store store) throws OperationException {
        final List<String> splits = getSplits(operation, context, store);
        splitStoreFromIterable(splits, context, store);
        return null;
    }

    public List<String> getSplits(final SplitStoreFromFile operation, final Context context, final Store store) throws OperationException {
        if (!store.isSupported(SplitStoreFromIterable.class)) {
            throw new UnsupportedOperationException(
                    SplitStoreFromFile.class.getSimpleName()
                            + " cannot be executed as "
                            + SplitStoreFromIterable.class.getSimpleName()
                            + " is not supported."
            );
        }

        final File file = new File(operation.getInputPath());
        if (!file.exists()) {
            throw new OperationException("Splits file does not exist: " + operation.getInputPath());
        }

        try {
            return FileUtils.readLines(file);
        } catch (final IOException e) {
            throw new OperationException("Unable to read splits from file: " + operation.getInputPath(), e);
        }
    }

    public void splitStoreFromIterable(final Iterable<String> splits, final Context context, final Store store) throws OperationException {
        if (!store.isSupported(SplitStoreFromIterable.class)) {
            throw new UnsupportedOperationException(
                    SplitStoreFromFile.class.getSimpleName()
                            + " cannot be executed as "
                            + SplitStoreFromIterable.class.getSimpleName()
                            + " is not supported."
            );
        }

        store.execute(
                new SplitStoreFromIterable.Builder<>()
                        .input(splits)
                        .build(),
                context
        );
    }
}
