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

package gaffer.store;

import gaffer.store.schema.StoreSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Properties;

/**
 * A <code>StoreProperties</code> contains specific configuration information for the store, such as database
 * connection strings. It wraps {@link Properties} and lazy loads the all properties from a file when first used.
 */
public class StoreProperties {
    private static final Logger LOGGER = LoggerFactory.getLogger(StoreProperties.class);
    public static final String STORE_CLASS = "gaffer.store.class";
    public static final String STORE_SCHEMA_CLASS = "gaffer.store.schema.class";
    public static final String STORE_PROPERTIES_CLASS = "gaffer.store.properties.class";

    private Path propFileLocation;
    private Properties props;

    // Required for loading by reflection.
    public StoreProperties() {
    }

    public StoreProperties(final Path propFileLocation) {
        this.propFileLocation = propFileLocation;
    }

    public StoreProperties(final Properties props) {
        this.props = props;
    }

    /**
     * @param key the property key
     * @return a property properties file with the given key.
     */
    public String get(final String key) {
        if (props == null) {
            readProperties();
        }
        return props.getProperty(key);
    }

    /**
     * Get a parameter from the schema file, or the default value.
     *
     * @param key          the property key
     * @param defaultValue the default value to use if the property doesn't exist
     * @return a property properties file with the given key or the default value if the property doesn't exist
     */
    public String get(final String key, final String defaultValue) {
        if (props == null) {
            readProperties();
        }
        return props.getProperty(key, defaultValue);
    }

    /**
     * Set a parameter from the schema file.
     *
     * @param key   the key
     * @param value the value
     */
    public void set(final String key, final String value) {
        if (props == null) {
            readProperties();
        }
        props.setProperty(key, value);
    }

    public String getStoreClass() {
        return get(STORE_CLASS);
    }

    public void setStoreClass(final String storeClass) {
        set(STORE_CLASS, storeClass);
    }

    public String getStoreSchemaClass() {
        return get(STORE_SCHEMA_CLASS, StoreSchema.class.getName());
    }

    public void setStoreSchemaClass(final String storeSchemaClass) {
        set(STORE_SCHEMA_CLASS, storeSchemaClass);
    }

    public String getStorePropertiesClass() {
        return get(STORE_PROPERTIES_CLASS, StoreProperties.class.getName());
    }

    public void setStorePropertiesClass(final String storePropertiesClass) {
        set(STORE_PROPERTIES_CLASS, storePropertiesClass);
    }

    public void setProperties(final Properties properties) {
        this.props = properties;
        propFileLocation = null;
    }

    public Properties getProperties() {
        return props;
    }

    public static StoreProperties loadStoreProperties(final Path storePropertiesPath) {
        try {
            return loadStoreProperties(null != storePropertiesPath ? Files.newInputStream(storePropertiesPath) : null);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load store properties file : " + e.getMessage(), e);
        }
    }

    public static StoreProperties loadStoreProperties(final InputStream storePropertiesStream) {
        if (null == storePropertiesStream) {
            return new StoreProperties();
        }
        final Properties props = new Properties();
        try {
            props.load(storePropertiesStream);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load store properties file : " + e.getMessage(), e);
        } finally {
            try {
                storePropertiesStream.close();
            } catch (IOException e) {
                LOGGER.error("Failed to close store properties stream: " + e.getMessage(), e);
            }
        }
        final String storePropertiesClass = props.getProperty(StoreProperties.STORE_PROPERTIES_CLASS);
        final StoreProperties storeProperties;
        if (null == storePropertiesClass) {
            storeProperties = new StoreProperties();
        } else {
            try {
                storeProperties = Class.forName(storePropertiesClass).asSubclass(StoreProperties.class).newInstance();
            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                throw new RuntimeException("Failed to create store properties file : " + e.getMessage(), e);
            }
        }
        storeProperties.setProperties(props);
        return storeProperties;
    }

    private void readProperties() {
        if (null != propFileLocation) {
            try (final InputStream accIs = Files.newInputStream(propFileLocation, StandardOpenOption.READ)) {
                props = new Properties();
                props.load(accIs);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
