/*
 * Copyright 2019 Crown Copyright
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

package uk.gov.gchq.gaffer.data.element.function;

import java.util.HashMap;

public class ElementTupleDefinition extends HashMap<String, Object> {
    public static final String GROUP = "GROUP";
    public static final String VERTEX = "VERTEX";
    public static final String SOURCE = "SOURCE";
    public static final String DESTINATION = "DESTINATION";
    public static final String DIRECTED = "DIRECTED";

    private static final long serialVersionUID = -2147030485732699831L;

    ElementTupleDefinition() {
        // constructor for json serialisation
    }

    public ElementTupleDefinition(final String group) {
        put(GROUP, group);
    }

    public String getGroup() {
        return (String) get(GROUP);
    }

    @Override
    public Object get(final Object key) {
        Object value = super.get(key);
        if (null == value) {
            if (DIRECTED.equals(key)) {
                value = true;
            }
        }
        return value;
    }

    public ElementTupleDefinition vertex(final Object value) {
        put(VERTEX, value);
        return this;
    }

    public ElementTupleDefinition source(final Object value) {
        put(SOURCE, value);
        return this;
    }

    public ElementTupleDefinition destination(final Object value) {
        put(DESTINATION, value);
        return this;
    }

    public ElementTupleDefinition directed(final Object value) {
        put(DIRECTED, value);
        return this;
    }

    public ElementTupleDefinition property(final String key, final Object value) {
        put(key, value);
        return this;
    }

    public ElementTupleDefinition property(final String key) {
        put(key, key);
        return this;
    }
}
