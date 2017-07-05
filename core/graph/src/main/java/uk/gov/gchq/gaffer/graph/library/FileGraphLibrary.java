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
package uk.gov.gchq.gaffer.graph.library;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.graph.exception.OverwritingException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Pattern;

public class FileGraphLibrary extends GraphLibrary {
    private static final Pattern PATH_ALLOWED_CHARACTERS = Pattern.compile("[a-zA-Z0-9_/\\\\]*");
    private static final Logger LOGGER = LoggerFactory.getLogger(FileGraphLibrary.class);

    private final String path;

    public FileGraphLibrary(final String path) {
        if (!PATH_ALLOWED_CHARACTERS.matcher(path).matches()) {
            throw new IllegalArgumentException("path is invalid: " + path + " it must match the regex: " + PATH_ALLOWED_CHARACTERS);
        }
        this.path = path;
    }

    @Override
    public Pair<String, String> getIds(final String graphId) {
        Pair<String, String> ids = null;

        if (getGraphsPath(graphId).toFile().exists()) {
            try (FileReader fr = new FileReader(getGraphsPath(graphId).toFile());
                 BufferedReader br = new BufferedReader(fr)) {

                String currentLine;

                while ((currentLine = br.readLine()) != null) {
                    String[] splitCurrentLine = currentLine.trim().split(",");
                    ids = new Pair<>(splitCurrentLine[0], splitCurrentLine[1]);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            return null;
        }
        return ids;
    }

    @Override
    protected void _addIds(final String graphId, final Pair<String, String> schemaAndPropsIds) throws OverwritingException {
        try {
            String schemaAndPropsIdsString = new String(schemaAndPropsIds.getFirst() + "," + schemaAndPropsIds.getSecond());
            System.out.println(schemaAndPropsIdsString);
            try (PrintWriter out = new PrintWriter(getGraphsPath(graphId).toFile())) {
                out.println(schemaAndPropsIdsString);
            }
        } catch (FileNotFoundException e) {
            throw new IllegalArgumentException("Could not write Graphs to path: " + getSchemaPath(graphId), e);
        }
    }

    @Override
    protected void _addSchema(final String schemaId,
                              final byte[] schema) throws OverwritingException {
        try {
            FileUtils.writeByteArrayToFile(getSchemaPath(schemaId).toFile(), schema);
        } catch (final IOException e) {
            throw new IllegalArgumentException("Could not write schema to path: " + getSchemaPath(schemaId), e);
        }
    }

    @Override
    protected void _addProperties(final String propertiesId,
                                  final StoreProperties properties) {
        try {
            FileOutputStream fout = new FileOutputStream(getPropertiesPath(propertiesId).toFile());
            ObjectOutputStream oos = new ObjectOutputStream(fout);
            oos.writeObject(properties.getProperties());
            fout.close();
            oos.close();
        } catch (IOException e) {
            throw new IllegalArgumentException("Could not write properties to path: " + getSchemaPath(propertiesId), e);
        }
    }

    @SuppressFBWarnings(value = "PZLA_PREFER_ZERO_LENGTH_ARRAYS", justification = "null represents there is no schema")
    @Override
    protected byte[] _getSchema(final String graphId) {
        final Path path = getSchemaPath(graphId);
        try {
            return path.toFile().exists() ? Files.readAllBytes(path) : null;
        } catch (IOException e) {
            throw new SchemaException("Unable to read schema bytes from file: " + getSchemaPath(graphId));
        }
    }

    @Override
    protected StoreProperties _getProperties(final String propertiesId) {
        try {
            FileInputStream fis = new FileInputStream(getPropertiesPath(propertiesId).toFile());
            return StoreProperties.loadStoreProperties(fis);
        } catch (IOException e) {
            throw new IllegalArgumentException("Unable to read properties from file: " + getPropertiesPath(propertiesId));
        }
    }

    private Path getSchemaPath(final String schemaId) {
        return Paths.get(path + "/" + schemaId + ".json");
    }

    private Path getPropertiesPath(final String propertiesId) {
        return Paths.get(path + "/" + propertiesId + ".json");
    }

    private Path getGraphsPath(final String graphId) {
        return Paths.get(path + "/" + graphId + "Graphs.json");
    }
}
