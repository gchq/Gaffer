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

package uk.gov.gchq.gaffer.data.generator;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.IdentifierType;
import java.util.LinkedHashMap;
import java.util.Map;

public class MapGenerator extends OneToOneElementGenerator<Map<String, Object>> {
    public static final String GROUP = "GROUP";
    private LinkedHashMap<String, String> fields = new LinkedHashMap<>();
    private LinkedHashMap<String, String> constants = new LinkedHashMap<>();

    @Override
    public Element getElement(final Map<String, Object> map) {
        throw new UnsupportedOperationException("Generating elements is not supported");
    }

    @Override
    public Map<String, Object> getObject(final Element element) {
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

        public Builder group(final String mapKey) {
            fields.put(GROUP, mapKey);
            return this;
        }

        public Builder property(final String propertyName, final String mapKey) {
            fields.put(propertyName, mapKey);
            return this;
        }

        public Builder vertex(final String mapKey) {
            return identifier(IdentifierType.VERTEX, mapKey);
        }

        public Builder source(final String mapKey) {
            return identifier(IdentifierType.SOURCE, mapKey);
        }

        public Builder destination(final String mapKey) {
            return identifier(IdentifierType.DESTINATION, mapKey);
        }

        public Builder direction(final String mapKey) {
            return identifier(IdentifierType.DIRECTED, mapKey);
        }

        public Builder identifier(final IdentifierType identifierType, final String mapKey) {
            fields.put(identifierType.name(), mapKey);
            return this;
        }

        public Builder constant(final String key, final String value) {
            constants.put(key, value);
            return this;
        }

        public MapGenerator build() {
            final MapGenerator generator = new MapGenerator();
            generator.setFields(fields);
            generator.setConstants(constants);

            return generator;
        }
    }
}
