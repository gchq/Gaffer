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

package uk.gov.gchq.gaffer.operation.impl.imprt.localfile;

import org.apache.commons.io.FileUtils;

import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.imprt.Importer;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import java.util.stream.Collectors;

/**
 * Implementation of the {@link Importer} interface for importing data from a local file.
 */
public class LocalFileImporter implements Importer {

    @Override
    public Iterable<?> add(final String filePath) throws OperationException {
        Iterable<String> data;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(FileUtils.openInputStream(new File(filePath))))) {
            data =  reader.lines().collect(Collectors.toList());
        } catch (final IOException e) {
            throw new OperationException(e.getMessage());
        }
        return data;
    }
}
