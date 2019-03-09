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

package uk.gov.gchq.gaffer.store;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import uk.gov.gchq.gaffer.commonutil.DebugUtil;
import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Properties;

/**
 * A {@code StoreProperties} contains specific configuration information for the store, such as database
 * connection strings. It wraps {@link Properties} and lazy loads the all properties from a file when first used.
 * <p>
 * All StoreProperties classes must be JSON serialisable.
 * </p>
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "storePropertiesClassName")
public class StoreProperties extends Properties implements Cloneable {
    // Required for loading by reflection.
    public StoreProperties() {
        super();
    }

    public StoreProperties(final String pathStr) {
        this(Paths.get(pathStr));
    }

    public StoreProperties(final Path propFileLocation) {
        try (InputStream buf = new BufferedInputStream(Files.newInputStream(propFileLocation))) {
            load(buf);
        } catch (final IOException e) {
            throw new RuntimeException("Failed to load store properties file : " + e.getMessage(), e);
        }
    }

    public StoreProperties(final InputStream storePropertiesStream) {
        if (null == storePropertiesStream) {
            return;
        }
        try (InputStream buf = new BufferedInputStream(storePropertiesStream)) {
            load(buf);
        } catch (final IOException e) {
            throw new RuntimeException("Failed to load store properties file : " + e.getMessage(), e);
        }
    }

    public StoreProperties(final Properties props) {
        this.putAll(props);
    }

    protected StoreProperties(final Properties props, final Class<? extends Store> storeClass) {
        this(props);
        if (null == StorePropertiesUtil.getStoreClass(this)) {
            StorePropertiesUtil.setStoreClass(this, storeClass);
        }
    }

    protected StoreProperties(final Class<? extends Store> storeClass) {
        this(new Properties(), storeClass);
    }

    protected StoreProperties(final Path propFileLocation, final Class<? extends Store> storeClass) {
        this(propFileLocation);
        if (null == StorePropertiesUtil.getStoreClass(this)) {
            StorePropertiesUtil.setStoreClass(this, storeClass);
        }
    }

    public static StoreProperties loadStoreProperties(final InputStream storePropertiesStream) {
        if (null == storePropertiesStream) {
            return new StoreProperties();
        }
        final StoreProperties props = new StoreProperties();
        try (InputStream buf = new BufferedInputStream(storePropertiesStream)) {
            props.load(buf);
        } catch (final IOException e) {
            throw new RuntimeException("Failed to load store properties file : " + e.getMessage(), e);
        }
        return props;
    }

    /**
     * Set a parameter from the schema file. If value is null then
     * property will be removed.
     *
     * @param key   the key
     * @param value the value
     */
    @Override
    public Object setProperty(final String key, final String value) {
        if (null == value) {
            return remove(key);
        } else {
            return super.setProperty(key, value);
        }
    }

    public void merge(final StoreProperties properties) {
        if (null != properties) {
            putAll(properties);
        }
    }

    @SuppressWarnings("CloneDoesntCallSuperClone")
    @SuppressFBWarnings(value = "CN_IDIOM_NO_SUPER_CALL", justification = "Only inherits from Object")
    @Override
    public StoreProperties clone() {
        StoreProperties cloned = new StoreProperties(this.defaults);
        this.forEach(cloned::put);
        return cloned;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (null == obj || getClass() != obj.getClass()) {
            return false;
        }

        final StoreProperties properties = (StoreProperties) obj;
        return new EqualsBuilder()
                .append(defaults, properties.defaults)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(5, 7)
                .append(defaults)
                .toHashCode();
    }

    @Override
    public String toString() {
        if (DebugUtil.checkDebugMode()) {
            return new ToStringBuilder(this)
                    .append("properties", defaults)
                    .toString();
        }

        // If we are not in debug mode then don't return the property values in case we leak sensitive properties.
        return super.toString();
    }
}
