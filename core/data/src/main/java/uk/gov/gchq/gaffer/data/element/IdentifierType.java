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

package uk.gov.gchq.gaffer.data.element;

import java.util.HashMap;
import java.util.Map;

/**
 * The <code>IdentifierType</code> enum contains the identifier types used for {@link uk.gov.gchq.gaffer.data.element.Entity}s and
 * {@link uk.gov.gchq.gaffer.data.element.Edge}s.
 */
public enum IdentifierType {
    // Entity identifier type
    VERTEX,

    // Edge identifier types
    /**
     * An Edge's source vertex
     */
    SOURCE,

    /**
     * An Edge's destination vertex
     */
    DESTINATION,

    /**
     * An Edge's directed flag
     */
    DIRECTED;

    private static final Map<String, IdentifierType> VALUES = new HashMap<>(values().length);

    static {
        for (final IdentifierType identifierType : IdentifierType.values()) {
            VALUES.put(identifierType.name(), identifierType);
        }
    }

    public static IdentifierType fromName(final String name) {
        return VALUES.get(name);
    }
}
