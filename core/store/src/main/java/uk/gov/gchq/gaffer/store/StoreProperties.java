/*
 * Copyright 2016-2017 Crown Copyright
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

package uk.gov.gchq.gaffer.store;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.store.operationdeclaration.OperationDeclarations;
import uk.gov.gchq.gaffer.store.schema.Schema;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Properties;

/**
 * A <code>StoreProperties</code> contains specific configuration information for the store, such as database
 * connection strings. It wraps {@link Properties} and lazy loads the all properties from a file when first used.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "storePropertiesClassName")
public class StoreProperties implements Cloneable {
    private static final Logger LOGGER = LoggerFactory.getLogger(StoreProperties.class);
    public static final String STORE_CLASS = "gaffer.store.class";
    public static final String SCHEMA_CLASS = "gaffer.store.schema.class";
    public static final String STORE_PROPERTIES_CLASS = "gaffer.store.properties.class";
    public static final String OPERATION_DECLARATIONS = "gaffer.store.operation.declarations";

    public static final String JOB_TRACKER_CLASS = "gaffer.store.job.tracker.class";
    public static final String JOB_TRACKER_CONFIG_PATH = "gaffer.store.job.tracker.config.path";

    private Properties props = new Properties();

    // Required for loading by reflection.
    public StoreProperties() {
    }

    public StoreProperties(final Path propFileLocation) {
        if (null != propFileLocation) {
            try (final InputStream accIs = Files.newInputStream(propFileLocation, StandardOpenOption.READ)) {
                props.load(accIs);
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public StoreProperties(final Properties props) {
        setProperties(props);
    }

    /**
     * @param key the property key
     * @return a property properties file with the given key.
     */
    public String get(final String key) {
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
        return props.getProperty(key, defaultValue);
    }

    /**
     * Set a parameter from the schema file.
     *
     * @param key   the key
     * @param value the value
     */
    public void set(final String key, final String value) {
        props.setProperty(key, value);
    }

    /**
     * Returns the operation definitions from the file specified in the properties.
     * This is an optional feature, so if the property does not exist then this function
     * will return an empty object.
     *
     * @return The Operation Definitions to load dynamically
     */
    @JsonIgnore
    public OperationDeclarations getOperationDeclarations() {
        OperationDeclarations declarations = null;

        final String declarationsPaths = get(StoreProperties.OPERATION_DECLARATIONS);
        if (null != declarationsPaths) {
            declarations = OperationDeclarations.fromPaths(declarationsPaths);
        }

        if (null == declarations) {
            declarations = new OperationDeclarations.Builder().build();
        }

        return declarations;
    }

    public String getStoreClass() {
        return get(STORE_CLASS);
    }

    public void setStoreClass(final String storeClass) {
        set(STORE_CLASS, storeClass);
    }

    public String getJobTrackerClass() {
        return get(JOB_TRACKER_CLASS);
    }

    public void setJobTrackerClass(final String jobTrackerClass) {
        set(JOB_TRACKER_CLASS, jobTrackerClass);
    }

    public String getJobTrackerConfigPath() {
        return get(JOB_TRACKER_CONFIG_PATH);
    }

    public void setJobTrackerConfigPath(final String jobTrackerConfigPath) {
        set(JOB_TRACKER_CONFIG_PATH, jobTrackerConfigPath);
    }

    public String getSchemaClassName() {
        return get(SCHEMA_CLASS, Schema.class.getName());
    }

    public Class<? extends Schema> getSchemaClass() {
        final Class<? extends Schema> schemaClass;
        try {
            schemaClass = Class.forName(getSchemaClassName()).asSubclass(Schema.class);
        } catch (ClassNotFoundException e) {
            throw new SchemaException("Schema class was not found: " + getSchemaClassName(), e);
        }

        return schemaClass;
    }

    @JsonSetter
    public void setSchemaClass(final String schemaClass) {
        set(SCHEMA_CLASS, schemaClass);
    }

    public void setSchemaClass(final Class<? extends Schema> schemaClass) {
        set(SCHEMA_CLASS, schemaClass.getName());
    }

    public String getStorePropertiesClassName() {
        return get(STORE_PROPERTIES_CLASS, StoreProperties.class.getName());
    }

    public Class<? extends StoreProperties> getStorePropertiesClass() {
        final Class<? extends StoreProperties> clazz;
        try {
            clazz = Class.forName(getStorePropertiesClassName()).asSubclass(StoreProperties.class);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Store properties class was not found: " + getStorePropertiesClassName(), e);
        }

        return clazz;
    }

    public void setStorePropertiesClassName(final String storePropertiesClassName) {
        set(STORE_PROPERTIES_CLASS, storePropertiesClassName);
    }

    public void setStorePropertiesClass(final Class<? extends StoreProperties> storePropertiesClass) {
        set(STORE_PROPERTIES_CLASS, storePropertiesClass.getName());
    }

    public String getOperationDeclarationPaths() {
        return get(OPERATION_DECLARATIONS);
    }

    public void setOperationDeclarationPaths(final String paths) {
        set(OPERATION_DECLARATIONS, paths);
    }

    public void setProperties(final Properties properties) {
        if (null == properties) {
            this.props = new Properties();
        } else {
            this.props = properties;
        }
    }

    public Properties getProperties() {
        return props;
    }

    public static StoreProperties loadStoreProperties(final String pathStr) {
        final StoreProperties storeProperties;
        final Path path = Paths.get(pathStr);
        try {
            if (path.toFile().exists()) {
                storeProperties = loadStoreProperties(Files.newInputStream(path));
            } else {
                storeProperties = loadStoreProperties(StreamUtil.openStream(StoreProperties.class, pathStr));
            }
        } catch (final IOException e) {
            throw new RuntimeException("Failed to load store properties file : " + e.getMessage(), e);
        }

        return storeProperties;
    }


    public static StoreProperties loadStoreProperties(final Path storePropertiesPath) {
        try {
            return loadStoreProperties(null != storePropertiesPath ? Files.newInputStream(storePropertiesPath) : null);
        } catch (final IOException e) {
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
        } catch (final IOException e) {
            throw new RuntimeException("Failed to load store properties file : " + e.getMessage(), e);
        } finally {
            try {
                storePropertiesStream.close();
            } catch (final IOException e) {
                LOGGER.error("Failed to close store properties stream: " + e.getMessage(), e);
            }
        }
        return loadStoreProperties(props);
    }

    @SuppressWarnings("CloneDoesntCallSuperClone")
    @SuppressFBWarnings(value = "CN_IDIOM_NO_SUPER_CALL", justification = "Only inherits from Object")
    @Override
    public StoreProperties clone() {
        return StoreProperties.loadStoreProperties((Properties) getProperties().clone());
    }

    public static StoreProperties loadStoreProperties(final Properties props) {
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
}
