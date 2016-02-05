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

/**
 * The <code>IdentifierType</code> enum contains the identifier types used for {@link gaffer.data.element.Entity}s and
 * {@link gaffer.data.element.Edge}s.
 */
public enum IdentifierType {
    // Entity identifier type
    VERTEX("vertex"),

    // Edge identifier types
    /**
     * An Edge's source vertex
     */
    SOURCE("source"),

    /**
     * An Edge's destination vertex
     */
    DESTINATION("destination"),

    /**
     * An Edge's directed flag
     */
    DIRECTED("directed");

    private final String propertyName;

    IdentifierType(final String propertyName) {
        this.propertyName = propertyName;
    }

    public String getPropertyName() {
        return propertyName;
    }
}
