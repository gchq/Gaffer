/*
 * Copyright 2017 Crown Copyright
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
package uk.gov.gchq.gaffer.store.schema.library;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.io.FileUtils;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Pattern;

public class FileSchemaLibrary extends SchemaLibrary {
    public static final String LIBRARY_PATH_KEY = "gaffer.store.schema.library.path";
    public static final String LIBRARY_PATH_DEFAULT = "schemaLibrary";
    private static final Pattern PATH_ALLOWED_CHARACTERS = Pattern.compile("[a-zA-Z0-9_/\\\\].*");

    private String path;

    @Override
    public void initialise(final StoreProperties storeProperties) {
        this.path = storeProperties.get(LIBRARY_PATH_KEY, LIBRARY_PATH_DEFAULT);
        if (!PATH_ALLOWED_CHARACTERS.matcher(path).matches()) {
            throw new IllegalArgumentException("path is invalid: " + path + " it must match the regex: " + PATH_ALLOWED_CHARACTERS);
        }
    }

    @Override
    protected void _add(final String graphId, final byte[] schema) {
        try {
            FileUtils.writeByteArrayToFile(new File(getSchemaPath(graphId)), schema);
        } catch (final IOException e) {
            throw new IllegalArgumentException("Could not write schema to path: " + getSchemaPath(graphId), e);
        }
    }

    @Override
    protected void _addOrUpdate(final String graphId, final byte[] schema) {
        _add(graphId, schema);
    }

    @SuppressFBWarnings(value = "PZLA_PREFER_ZERO_LENGTH_ARRAYS", justification = "null represents there is no schema")
    @Override
    protected byte[] _get(final String graphId) {
        final Path path = Paths.get(getSchemaPath(graphId));
        try {
            return path.toFile().exists() ? Files.readAllBytes(path) : null;
        } catch (IOException e) {
            throw new SchemaException("Unable to read schema bytes from file: " + getSchemaPath(graphId));
        }
    }

    private String getSchemaPath(final String graphId) {
        return path + "/" + graphId + ".json";
    }
}
