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

package gaffer.store.schema;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import gaffer.data.elementdefinition.ElementDefinition;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Set;

/**
 * A <code>StoreElementDefinition</code> defines how an {@link gaffer.data.element.Element} should be stored in the
 * {@link gaffer.store.Store}.
 * Specifically this class holds a map of property names to {@link gaffer.store.schema.StorePropertyDefinition}s.
 *
 * @see gaffer.store.schema.StoreElementDefinition.Builder
 * @see gaffer.store.schema.StoreElementDefinition
 */
public class StoreElementDefinition implements ElementDefinition {
    private static final long serialVersionUID = 8903096088699346527L;

    /**
     * Map of property name to store property definition. It is stored as a {@link java.util.LinkedHashMap} to ensure the
     * order of the keys is preserved, allowing stores to iterate over the properties in a consistent order.
     */
    private LinkedHashMap<String, StorePropertyDefinition> properties;

    /**
     * Default constructor should only be used for deserialisation from json.
     */
    public StoreElementDefinition() {
        properties = new LinkedHashMap<>();
    }

    /**
     * Adds a {@link gaffer.store.schema.StorePropertyDefinition} for a given property name.
     *
     * @param propertyName the property name
     * @param propertyDef  the {@link gaffer.store.schema.StorePropertyDefinition} for the given property name
     */
    public void addProperty(final String propertyName, final StorePropertyDefinition propertyDef) {
        properties.put(propertyName, propertyDef);
    }

    /**
     * @param properties a {@link java.util.LinkedHashMap} of properties to add.
     */
    public void addProperties(final LinkedHashMap<String, StorePropertyDefinition> properties) {
        this.properties.putAll(properties);
    }

    /**
     * @param propertyName the property name of the definition to lookup
     * @return the {@link gaffer.store.schema.StorePropertyDefinition} of the given property name.
     */
    public StorePropertyDefinition getProperty(final String propertyName) {
        return properties.get(propertyName);
    }

    /**
     * @return an ordered collection of all {@link gaffer.store.schema.StorePropertyDefinition}s
     */
    @JsonIgnore
    public Collection<StorePropertyDefinition> getPropertyDefinitions() {
        return properties.values();
    }

    /**
     * @param propertyName the property name to check if it exists
     * @return true if there is a {@link gaffer.store.schema.StorePropertyDefinition} for the given property name
     */
    @Override
    public boolean containsProperty(final String propertyName) {
        return properties.containsKey(propertyName);
    }

    /**
     * @return a set of all property names.
     */
    @JsonIgnore
    public Set<String> getProperties() {
        return properties.keySet();
    }

    /**
     * @return a {@link java.util.LinkedHashMap} of all property names to {@link gaffer.store.schema.StorePropertyDefinition}.
     */
    @JsonProperty("properties")
    public LinkedHashMap<String, StorePropertyDefinition> getPropertyMap() {
        return properties;
    }

    /**
     * @param properties sets a {@link java.util.LinkedHashMap} of all property names to
     *                 {@link gaffer.store.schema.StorePropertyDefinition}.
     */
    public void setProperties(final LinkedHashMap<String, StorePropertyDefinition> properties) {
        this.properties = properties;
    }

    /**
     * Validates the {@link gaffer.store.schema.StoreElementDefinition}.
     * Currently there is no validation so this will just return <code>true</code>.
     * Override to add validation.
     *
     * @return <code>true</code>
     */
    @Override
    public boolean validate() {
        return true;
    }

    /**
     * Builder for {@link gaffer.store.schema.StoreElementDefinition}.
     *
     * @see gaffer.store.schema.StoreElementDefinition
     */
    public static class Builder {
        private final StoreElementDefinition elDef;

        public Builder() {
            this(new StoreElementDefinition());
        }

        public Builder(final StoreElementDefinition elDef) {
            this.elDef = elDef;
        }

        /**
         * @param propertyName the property name
         * @param propertyDef  the {@link gaffer.store.schema.StorePropertyDefinition} for the given property name
         * @return this Builder
         * @see gaffer.store.schema.StoreElementDefinition#addProperty(String, gaffer.store.schema.StorePropertyDefinition)
         */
        public Builder property(final String propertyName, final StorePropertyDefinition propertyDef) {
            elDef.addProperty(propertyName, propertyDef);
            return this;
        }

        /**
         * Builds the {@link gaffer.store.schema.StoreElementDefinition} and returns it.
         *
         * @return the built {@link gaffer.store.schema.StoreElementDefinition}.
         */
        public StoreElementDefinition build() {
            return elDef;
        }
    }
}
