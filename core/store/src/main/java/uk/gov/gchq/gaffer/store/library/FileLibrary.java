/*
 * Copyright 2017-2019 Crown Copyright
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

import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.store.util.Config;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Pattern;

/**
 * A {@code FileLibrary} stores a {@link Library} in a specified
 * location as files.  It will store a graphId file with the relationships between
 * the graphId, storePropertiesId and the schemaId.  It will also store the
 * StoreProperties and Schema in two other files.  They will be named using the ids.
 */
public class FileLibrary extends Library {
    private static final Pattern PATH_ALLOWED_CHARACTERS = Pattern.compile("[a-zA-Z0-9_/\\\\\\-]*");
    private static final String DEFAULT_PATH = "library";
    private String path;

    public FileLibrary() {
        this(DEFAULT_PATH);
    }

    public FileLibrary(final String path) {
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
    protected void _addConfig(final String storeId, final Config config) {
        if (null != config) {
            getConfigPath(storeId).toFile().getParentFile().mkdirs();
            try (FileOutputStream configFileOutputStream =
                         new FileOutputStream(getConfigPath(storeId).toFile())) {
                ObjectOutputStream objectOut = new ObjectOutputStream(configFileOutputStream);
                objectOut.writeObject(JSONSerialiser.serialise(config));
                objectOut.close();
            } catch (final IOException e) {
                throw new IllegalArgumentException("Could not write " +
                        "config to path: " + getConfigPath(storeId), e);
            }
        } else {
            throw new IllegalArgumentException("Config cannot be null");
        }
    }

    @Override
    protected Config _getConfig(final String configId) {
        final Path configPath = getConfigPath(configId);
        if (!configPath.toFile().exists()) {
            return null;
        }
        return new Config.BaseBuilder().json(configPath).build();
    }

    private Path getConfigPath(final String storeId) {
        return Paths.get(path + "/" + storeId + "Config.json");
    }
}