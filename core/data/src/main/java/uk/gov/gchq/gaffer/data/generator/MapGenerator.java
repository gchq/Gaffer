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
    private LinkedHashMap<String, String> fields;
    private LinkedHashMap<String, String> constants;

    @Override
    public Element getElement(final Map<String, Object> map) {
        throw new UnsupportedOperationException("Generating elements is not supported");
    }

    @Override
    public Map<String, Object> getObject(final Element element) {
        final Map<String, Object> map = new LinkedHashMap<>(fields.size() + constants.size());
        for (final Map.Entry<String, String> entry : fields.entrySet()) {
            final String key = entry.getKey();
            final IdentifierType idType = IdentifierType.fromName(key);
            if (null == idType) {
                if (GROUP.equals(key)) {
                    map.put(entry.getValue(), element.getGroup());
                } else {
                    map.put(entry.getValue(), element.getProperty(key));
                }
            }
            map.put(entry.getValue(), element.getIdentifier(idType));
        }

        map.putAll(constants);

        return map;
    }

    public LinkedHashMap<String, String> getFields() {
        return fields;
    }

    public void setFields(final LinkedHashMap<String, String> fields) {
        this.fields = fields;
    }

    public LinkedHashMap<String, String> getConstants() {
        return constants;
    }

    public void setConstants(final LinkedHashMap<String, String> constants) {
        this.constants = constants;
    }
}
