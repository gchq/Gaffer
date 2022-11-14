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

import org.apache.commons.io.FileUtils;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.export.Exporter;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Collectors;

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
    public Iterable<String> get(final String filePath) throws OperationException {
        Iterable<String> linesFromFile;

        InputStream fileToBeRead;
        final Path path = Paths.get(filePath);
        try {
            if (path.toFile().exists()) {
                fileToBeRead = FileUtils.openInputStream(new File(filePath));
            } else {
                fileToBeRead = StreamUtil.openStream(LocalFileExporter.class, filePath);
            }
            BufferedReader reader = new BufferedReader(new InputStreamReader(fileToBeRead));
            linesFromFile = reader.lines().collect(Collectors.toList());
        } catch (final IOException e) {
            throw new OperationException(e.getMessage());
        }
        return linesFromFile;
    }
}
