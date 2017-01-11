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

package uk.gov.gchq.gaffer.store.schema;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


/**
 * <code>TypeDefinitions</code> simply extends {@link HashMap} with key = {@link String}, value = {@link TypeDefinition}.
 * This is required for serialising to JSON.
 */
public class TypeDefinitions extends HashMap<String, TypeDefinition> {
    private static final long serialVersionUID = -4289118603357719559L;
    public static final String LOCKED_MSG = "This object is locked and cannot be modified";
    private boolean locked = false;

    public TypeDefinition getType(final String typeName) {
        final TypeDefinition type = get(typeName);
        if (null == type) {
            throw new IllegalArgumentException("Unable to find type with name: " + typeName);
        }

        return type;
    }

    public void merge(final TypeDefinitions types) {
        if (locked) {
            throw new UnsupportedOperationException(LOCKED_MSG);
        }

        for (final Map.Entry<String, TypeDefinition> entry : types.entrySet()) {
            if (!containsKey(entry.getKey())) {
                put(entry.getKey(), entry.getValue());
            } else {
                getType(entry.getKey()).merge(entry.getValue());
            }
        }
    }

    public void lock() {
        this.locked = true;
    }

    public TypeDefinition put(final String key, final TypeDefinition value) {
        if (locked) {
            throw new UnsupportedOperationException(LOCKED_MSG);
        }
        return super.put(key, value);
    }

    public void putAll(final Map<? extends String, ? extends TypeDefinition> m) {
        if (locked) {
            throw new UnsupportedOperationException(LOCKED_MSG);
        }
        super.putAll(m);
    }

    public TypeDefinition remove(final Object key) {
        if (locked) {
            throw new UnsupportedOperationException(LOCKED_MSG);
        }
        return super.remove(key);
    }

    public void clear() {
        if (locked) {
            throw new UnsupportedOperationException(LOCKED_MSG);
        }
        super.clear();
    }

    @Override
    public Set<String> keySet() {
        if (locked) {
            return Collections.unmodifiableSet(super.keySet());
        }

        return super.keySet();
    }

    public Collection<TypeDefinition> values() {
        if (locked) {
            return Collections.unmodifiableCollection(super.values());
        }

        return super.values();
    }

    public Set<Entry<String, TypeDefinition>> entrySet() {
        if (locked) {
            return Collections.unmodifiableSet(super.entrySet());
        }

        return super.entrySet();
    }

    public boolean remove(final Object key, final Object value) {
        if (locked) {
            throw new UnsupportedOperationException(LOCKED_MSG);
        }

        return super.remove(key, value);
    }
}
