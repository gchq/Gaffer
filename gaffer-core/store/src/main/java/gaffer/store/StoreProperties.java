/*
 * Copyright 2016 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gaffer.store;

import gaffer.store.schema.StoreSchema;

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

    private void readProperties() {
        if (null != propFileLocation) {
            try (final InputStream accIs = Files.newInputStream(propFileLocation, StandardOpenOption.READ)) {
                if (accIs != null) {
                    props = new Properties();
                    props.load(accIs);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * @param key the property key
     * @return a property properties file with the given key.
     */
    public String get(final String key) {
        synchronized (this) {
            if (props == null) {
                readProperties();
            }
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
        synchronized (this) {
            if (props == null) {
                readProperties();
            }
        }
        return props.getProperty(key, defaultValue);
    }

    /**
     * Set a parameter from the schema file.
     *
     * @param key
     * @param value
     */
    public void set(final String key, final String value) {
        synchronized (this) {
            if (props == null) {
                readProperties();
            }
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
}
