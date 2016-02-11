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

package gaffer.data.element;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * An <code>ElementComponentKey</code> is used to represent an {@link gaffer.data.element.IdentifierType} or
 * property name.
 * It is particularly useful when serialising into JSON as the isId flag allows you to distinguish between identifier
 * types and property names.
 * This class essentially is to avoid the issue where a property name is equal to an identifier type.
 * If the isId field is set to true, then the key is an identifier type, otherwise it is a property name.
 */
public class ElementComponentKey {
    private String key;
    private boolean isId;

    public ElementComponentKey() {
        // required for serialisation
    }

    public ElementComponentKey(final String key) {
        this(key, false);
    }

    public ElementComponentKey(final IdentifierType identifierType) {
        this(identifierType.name(), true);
    }

    public ElementComponentKey(final String key, final boolean isId) {
        this.key = key;
        this.isId = isId;
    }

    /**
     * Converts property names into {@link gaffer.data.element.ElementComponentKey}s.
     *
     * @param propertyNames the property names to be converted
     * @return an array of {@link gaffer.data.element.ElementComponentKey}s
     */
    public static ElementComponentKey[] createKeys(final String... propertyNames) {
        final ElementComponentKey[] keys = new ElementComponentKey[propertyNames.length];
        for (int i = 0; i < propertyNames.length; i++) {
            keys[i] = new ElementComponentKey(propertyNames[i]);
        }

        return keys;
    }

    /**
     * Converts {@link IdentifierType}s into {@link gaffer.data.element.ElementComponentKey}s.
     *
     * @param idTypes the {@link IdentifierType}s to be converted
     * @return an array of {@link gaffer.data.element.ElementComponentKey}s
     */
    public static ElementComponentKey[] createKeys(final IdentifierType... idTypes) {
        final ElementComponentKey[] keys = new ElementComponentKey[idTypes.length];
        for (int i = 0; i < idTypes.length; i++) {
            keys[i] = new ElementComponentKey(idTypes[i]);
        }

        return keys;
    }

    /**
     * @return the {@link gaffer.data.element.IdentifierType} of the key.
     * @throws IllegalStateException if the key does not represent an {@link gaffer.data.element.IdentifierType}
     */
    @JsonIgnore
    public IdentifierType getIdentifierType() throws IllegalStateException {
        if (!isId) {
            throw new IllegalStateException("The key is not an identifier type");
        }

        return IdentifierType.valueOf(key);
    }

    /**
     * @return the property name stored in the key.
     * @throws IllegalStateException if the key does not represent an property name
     */
    @JsonIgnore
    public String getPropertyName() {
        if (isId) {
            throw new IllegalStateException("The key is not an property name");
        }

        return key;
    }

    /**
     * @return the property name stored in the key or {@link gaffer.data.element.IdentifierType} of the key depending
     * on the isId flag.
     */
    @JsonIgnore
    public Object getKeyObject() {
        if (isId) {
            return IdentifierType.valueOf(key);
        }

        return key;
    }

    public String getKey() {
        return key;
    }

    public void setKey(final String key) {
        this.key = key;
    }

    @JsonProperty("isId")
    public boolean isId() {
        return isId;
    }

    public void setIsId(final boolean isId) {
        this.isId = isId;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ElementComponentKey)) {
            return false;
        }

        final ElementComponentKey that = (ElementComponentKey) o;

        return isId == that.isId && !(key != null ? !key.equals(that.key) : that.key != null);
    }

    @Override
    public int hashCode() {
        int result = key != null ? key.hashCode() : 0;
        result = 31 * result + (isId ? 1 : 0);
        return result;
    }
}
