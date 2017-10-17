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

package uk.gov.gchq.gaffer.store.library;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.io.FileUtils;

import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.exception.OverwritingException;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.regex.Pattern;

/**
 * A {@code FileGraphLibrary} stores a {@link GraphLibrary} in a specified
 * location as files.  It will store a graphId file with the relationships between
 * the graphId, storePropertiesId and the schemaId.  It will also store the
 * StoreProperties and Schema in two other files.  They will be named using the ids.
 */
public class FileGraphLibrary extends GraphLibrary {
    private static final Pattern PATH_ALLOWED_CHARACTERS = Pattern.compile("[a-zA-Z0-9_/\\\\\\-]*");
    private static final String DEFAULT_PATH = "graphLibrary";
    private String path;

    public FileGraphLibrary() {
        this(DEFAULT_PATH);
    }

    public FileGraphLibrary(final String path) {
        setPath(path);
    }

    @Override
    public void initialise(final String path) {
        setPath(path);
    }

    public String getPath() {
        return path;
    }

    public void setPath(final String path) {
        if (null == path) {
            this.path = DEFAULT_PATH;
        } else {
            if (!PATH_ALLOWED_CHARACTERS.matcher(path).matches()) {
                throw new IllegalArgumentException("path is invalid: " + path + " it must match the regex: " + PATH_ALLOWED_CHARACTERS);
            }
            this.path = path;
        }
    }

    @Override
    public Pair<String, String> getIds(final String graphId) {
        Pair<String, String> ids;

        if (getGraphsPath(graphId).toFile().exists()) {
            try {
                List<String> lines = Files.readAllLines(getGraphsPath(graphId));
                String[] split = lines.get(0).trim().split(",");
                if ((null == split[0] || split[0].isEmpty()) || (null == split[1] || split[1].isEmpty())) {
                    return null;
                }
                ids = new Pair<>(split[0], split[1]);
            } catch (final IOException e) {
                throw new IllegalArgumentException("Could not read graphs file: " + getGraphsPath(graphId), e);
            }
        } else {
            return null;
        }
        return ids;
    }

    @Override
    protected void _addIds(final String graphId, final Pair<String, String> schemaAndPropsIds) throws OverwritingException {
        String schemaAndPropsIdsString = new String(schemaAndPropsIds.getFirst() + "," + schemaAndPropsIds.getSecond());
        try {
            FileUtils.writeStringToFile(getGraphsPath(graphId).toFile(), schemaAndPropsIdsString);
        } catch (final IOException e) {
            throw new IllegalArgumentException("Could not write Graphs to path: " + getSchemaPath(graphId), e);
        }
    }

    @Override
    protected void _addSchema(final String schemaId,
                              final byte[] schema) throws OverwritingException {
        if (null != schema) {
            try {
                FileUtils.writeByteArrayToFile(getSchemaPath(schemaId).toFile(), schema);
            } catch (final IOException e) {
                throw new IllegalArgumentException("Could not write schema to path: " + getSchemaPath(schemaId), e);
            }
        } else {
            throw new IllegalArgumentException("Schema cannot be null");
        }
    }

    @Override
    protected void _addProperties(final String propertiesId,
                                  final StoreProperties properties) {
        if (null != properties) {
            getPropertiesPath(propertiesId).toFile().getParentFile().mkdirs();
            try (FileOutputStream propertiesFileOutputStream = new FileOutputStream(getPropertiesPath(propertiesId).toFile())) {
                properties.getProperties().store(propertiesFileOutputStream, null);
            } catch (final IOException e) {
                throw new IllegalArgumentException("Could not write properties to path: " + getSchemaPath(propertiesId), e);
            }
        } else {
            throw new IllegalArgumentException("StoreProperties cannot be null");
        }
    }

    @SuppressFBWarnings(value = "PZLA_PREFER_ZERO_LENGTH_ARRAYS", justification = "null represents there is no schema")
    @Override
    protected byte[] _getSchema(final String graphId) {
        final Path path = getSchemaPath(graphId);
        try {
            return path.toFile().exists() ? Files.readAllBytes(path) : null;
        } catch (final IOException e) {
            throw new SchemaException("Unable to read schema bytes from file: " + getSchemaPath(graphId));
        }
    }

    @Override
    protected StoreProperties _getProperties(final String propertiesId) {
        final Path propertiesPath = getPropertiesPath(propertiesId);
        if (!propertiesPath.toFile().exists()) {
            return null;
        }
        return StoreProperties.loadStoreProperties(propertiesPath);
    }

    private Path getSchemaPath(final String schemaId) {
        return Paths.get(path + "/" + schemaId + "Schema.json");
    }

    private Path getPropertiesPath(final String propertiesId) {
        return Paths.get(path + "/" + propertiesId + "Props.properties");
    }

    private Path getGraphsPath(final String graphId) {
        return Paths.get(path + "/" + graphId + "Graphs.json");
    }
}
