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

package uk.gov.gchq.gaffer.operation.impl.export.localfile;

import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.export.Exporter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Implementation of the {@link Exporter} interface for exporting an Iterable of strings to a local file.
 */
public class LocalFileExporter implements Exporter {

    @Override
    public void add(final String filePath, final Iterable<?> results) throws OperationException {
        try {
            Files.write(Paths.get(filePath), (Iterable<? extends CharSequence>) results);
        } catch (final IOException e) {
            throw new OperationException(e.getMessage(), e);
        }
    }

    @Override
    public Iterable<?> get(final String key) throws OperationException {
        throw new UnsupportedOperationException("Getting export from local file is not supported");
    }
}
