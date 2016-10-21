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

import java.util.HashMap;
import java.util.Map;

/**
 * <code>TypeDefinitions</code> simply extends {@link HashMap} with key = {@link String}, value = {@link TypeDefinition}.
 * This is required for serialising to JSON.
 */
public class TypeDefinitions extends HashMap<String, TypeDefinition> {
    private static final long serialVersionUID = -4289118603357719559L;

    public TypeDefinition getType(final String typeName) {
        final TypeDefinition type = get(typeName);
        if (null == type) {
            throw new IllegalArgumentException("Unable to find type with name: " + typeName);
        }

        return type;
    }

    public void merge(final TypeDefinitions types) {
        for (Map.Entry<String, TypeDefinition> entry : types.entrySet()) {
            if (!containsKey(entry.getKey())) {
                put(entry.getKey(), entry.getValue());
            } else {
                getType(entry.getKey()).merge(entry.getValue());
            }
        }
    }
}
