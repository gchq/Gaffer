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

package gaffer.data.elementdefinition;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSetter;
import gaffer.data.Validator;
import gaffer.data.element.ElementComponentKey;
import gaffer.data.element.IdentifierType;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * An ElementDefinition that has maps of property name to property value type and
 * {@link gaffer.data.element.IdentifierType} to identifier value type.
 *
 * @see gaffer.data.elementdefinition.TypedElementDefinition.Builder
 */
public abstract class TypedElementDefinition implements ElementDefinitionWithIds {
    private static final Map<String, Class<?>> CLASSES = new HashMap<>();

    /**
     * A validator to validate the element definition
     */
    private final Validator elementDefValidator;

    /**
     * Property map of property name to accepted type.
     */
    private LinkedHashMap<String, String> properties;

    /**
     * Identifier map of identifier type to accepted type.
     */
    private LinkedHashMap<IdentifierType, String> identifiers;

    public TypedElementDefinition(final Validator<? extends TypedElementDefinition> elementDefValidator) {
        this.elementDefValidator = elementDefValidator;
        properties = new LinkedHashMap<>();
        identifiers = new LinkedHashMap<>();
    }

    /**
     * Uses the element definition validator to validate the element definition.
     *
     * @return true if the element definition is valid, otherwise false.
     * @see gaffer.data.elementdefinition.ElementDefinitionWithIds#validate()
     */
    public boolean validate() {
        return elementDefValidator.validate(this);
    }

    public Set<String> getProperties() {
        return properties.keySet();
    }

    public boolean containsProperty(final String propertyName) {
        return properties.containsKey(propertyName);
    }


    @JsonGetter("properties")
    public Map<String, String> getPropertyMap() {
        return Collections.unmodifiableMap(properties);
    }

    @JsonSetter("properties")
    void setPropertyMap(final LinkedHashMap<String, String> properties) {
        this.properties = properties;
    }

    @JsonIgnore
    public Collection<IdentifierType> getIdentifiers() {
        return identifiers.keySet();
    }

    protected Map<IdentifierType, String> getIdentifierMap() {
        return identifiers;
    }

    public boolean containsIdentifier(final IdentifierType identifierType) {
        return identifiers.containsKey(identifierType);
    }

    public Class<?> getPropertyClass(final String propertyName) {
        return getClass(getPropertyClassName(propertyName));
    }

    public Class<?> getIdentifierClass(final IdentifierType idType) {
        return getClass(getIdentifierClassName(idType));
    }

    public String getPropertyClassName(final String propertyName) {
        return properties.get(propertyName);
    }

    public String getIdentifierClassName(final IdentifierType idType) {
        return identifiers.get(idType);
    }

    public Class<?> getClass(final ElementComponentKey key) {
        if (key.isId()) {
            return getIdentifierClass(key.getIdentifierType());
        }

        return getPropertyClass(key.getPropertyName());
    }

    public Class<?> getClass(final String className) {
        if (null == className) {
            return null;
        }

        Class<?> clazz = CLASSES.get(className);
        if (null == clazz) {
            try {
                clazz = Class.forName(className);
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException("Class could not be found: " + className, e);
            }
            CLASSES.put(className, clazz);
        }

        return clazz;
    }

    public static class Builder {
        private final TypedElementDefinition elDef;

        public Builder(final TypedElementDefinition elDef) {
            this.elDef = elDef;
        }

        public Builder property(final String propertyName, final Class<?> clazz) {
            elDef.properties.put(propertyName, clazz.getName());
            return this;
        }

        protected Builder identifier(final IdentifierType identifierType, final Class<?> clazz) {
            elDef.identifiers.put(identifierType, clazz.getName());
            return this;
        }

        public TypedElementDefinition build() {
            return elDef;
        }

        protected TypedElementDefinition getElementDef() {
            return elDef;
        }
    }
}
