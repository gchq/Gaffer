/*
 * Copyright 2016-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.data.generator;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A {@code MapGenerator} is a generator which creates a representation of an {@link Element}
 * using a {@link LinkedHashMap}.
 * <p>
 * For example, providing an {@link uk.gov.gchq.gaffer.data.element.Edge} with fields:
 * <ul>
 * <li>Group: "EdgeGroup"</li>
 * <li>Source: "A"</li>
 * <li>Destination: "B"</li>
 * <li>Directed: true</li>
 * <li>Property1: "propValue"</li>
 * </ul>
 * The generator will store these in a {@code LinkedHashMap}, for which the fields will be represented as:<br>
 * <pre>
 *     {
 *          GROUP: "EdgeGroup",
 *          SOURCE: "A",
 *          DESTINATION: "B",
 *          DIRECTED: "true"
 *          PROPERTY1: "propValue"
 *     }
 *     </pre>
 * <p>Any constants relevant to a particular {@code Element} can be added,
 * and will be stored in a separate {@code LinkedHashMap}.
 */
@Since("1.0.0")
@Summary("Generates a Map for each element")
public class MapGenerator implements OneToOneObjectGenerator<Map<String, Object>> {
    public static final String GROUP = "GROUP";
    private LinkedHashMap<String, String> fields = new LinkedHashMap<>();
    private LinkedHashMap<String, String> constants = new LinkedHashMap<>();

    @Override
    public Map<String, Object> _apply(final Element element) {
        final Map<String, Object> map = new LinkedHashMap<>(fields.size() + constants.size());
        for (final Map.Entry<String, String> entry : fields.entrySet()) {
            final Object value = getFieldValue(element, entry.getKey());
            if (null != value) {
                map.put(entry.getValue(), value);
            }
        }

        map.putAll(constants);

        return map;
    }

    /**
     * Attempts to find the value of a field from a given {@link Element},
     * corresponding to a provided key, where the key is the name of the field.
     *
     * @param element the Element from which to retrieve a field value
     * @param key     the name of the field to be retrieved
     * @return the value of the field
     */
    private Object getFieldValue(final Element element, final String key) {
        final IdentifierType idType = IdentifierType.fromName(key);
        final Object value;
        if (null == idType) {
            if (GROUP.equals(key)) {
                value = element.getGroup();
            } else {
                value = element.getProperty(key);
            }
        } else {
            value = element.getIdentifier(idType);
        }
        return value;
    }

    public LinkedHashMap<String, String> getFields() {
        return fields;
    }

    public void setFields(final LinkedHashMap<String, String> fields) {
        if (null == fields) {
            this.fields = new LinkedHashMap<>();
        }
        this.fields = fields;
    }

    public LinkedHashMap<String, String> getConstants() {
        return constants;
    }

    public void setConstants(final LinkedHashMap<String, String> constants) {
        if (null == constants) {
            this.constants = new LinkedHashMap<>();
        }
        this.constants = constants;
    }

    public static class Builder {
        private LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        private LinkedHashMap<String, String> constants = new LinkedHashMap<>();

        /**
         * Stores the group of an {@link Element}.
         *
         * @param mapKey the group of the {@code Element}
         * @return a new {@link Builder}
         */
        public Builder group(final String mapKey) {
            fields.put(GROUP, mapKey);
            return this;
        }

        /**
         * Stores any additional properties of an {@link Element}.<br>
         * For example: property("count", "3").<br>
         * This would add the "count" property with a value of "3".
         *
         * @param propertyName the name of the property
         * @param mapKey       the value of the property
         * @return a new {@link Builder}
         */
        public Builder property(final String propertyName, final String mapKey) {
            fields.put(propertyName, mapKey);
            return this;
        }

        /**
         * Stores the Vertex of an {@link uk.gov.gchq.gaffer.data.element.Entity}.
         *
         * @param mapKey the vertex contained within the {@code Entity}
         * @return a new {@link Builder}
         */
        public Builder vertex(final String mapKey) {
            return identifier(IdentifierType.VERTEX, mapKey);
        }

        /**
         * Stores the Source Vertex of an {@link uk.gov.gchq.gaffer.data.element.Edge}.
         *
         * @param mapKey the source vertex
         * @return a new {@link Builder}
         */
        public Builder source(final String mapKey) {
            return identifier(IdentifierType.SOURCE, mapKey);
        }

        /**
         * Stores the Destination Vertex of an {@link uk.gov.gchq.gaffer.data.element.Edge}
         *
         * @param mapKey the destination vertex
         * @return a new {@link Builder}
         */
        public Builder destination(final String mapKey) {
            return identifier(IdentifierType.DESTINATION, mapKey);
        }

        /**
         * Stores the Direction flag, indicating whether or not the {@link uk.gov.gchq.gaffer.data.element.Edge}
         * is directed.
         *
         * @param mapKey true or false for if the {@code Edge} is directed or not
         * @return a new {@link Builder}
         */
        public Builder direction(final String mapKey) {
            return identifier(IdentifierType.DIRECTED, mapKey);
        }

        /**
         * Allows an {@link IdentifierType} of an {@link Element} to be stored, such as
         * an {@link uk.gov.gchq.gaffer.data.element.Edge}'s {@link IdentifierType#MATCHED_VERTEX}.
         *
         * @param identifierType the {@code IdentifierType} of the {@code Element}
         * @param mapKey         the value for the corresponding field
         * @return a new {@link Builder}
         */
        public Builder identifier(final IdentifierType identifierType, final String mapKey) {
            fields.put(identifierType.name(), mapKey);
            return this;
        }

        /**
         * Stores any constants specific to a given {@link Element}.
         *
         * @param key   the name of the constant
         * @param value the value of the constant
         * @return a new {@link Builder}
         */
        public Builder constant(final String key, final String value) {
            constants.put(key, value);
            return this;
        }

        /**
         * Passes all of the configured information about an {@link Element} into a new {@link MapGenerator}
         *
         * @return a new {@code MapGenerator}, containing all fields and constants
         */
        public MapGenerator build() {
            final MapGenerator generator = new MapGenerator();
            generator.setFields(fields);
            generator.setConstants(constants);

            return generator;
        }
    }
}
