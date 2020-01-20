/*
 * Copyright 2020 Crown Copyright
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

import uk.gov.gchq.gaffer.types.TypeValue;
import uk.gov.gchq.koryphe.tuple.Tuple;

import java.util.Arrays;

import static java.util.Objects.isNull;

public class TypeValueTuple implements Tuple<String> {
    private TypeValue tv;

    public TypeValueTuple() {
        this(null);
    }

    public TypeValueTuple(final TypeValue tv) {
        if (isNull(tv)) {
            this.tv = new TypeValue();
        } else {
            this.tv = tv;
        }
    }

    @Override
    public void put(final String key, final Object value) {
        final String stringValue = isNull(value) ? null : value.toString();
        if ("type".equalsIgnoreCase(key)) {
            tv.setType(stringValue);
        } else if ("value".equalsIgnoreCase(key)) {
            tv.setValue(stringValue);
        }
    }

    @Override
    public Object get(final String key) {
        if ("type".equalsIgnoreCase(key)) {
            return tv.getType();
        }
        if ("value".equalsIgnoreCase(key)) {
            return tv.getValue();
        }
        return null;
    }

    @Override
    public Iterable<Object> values() {
        return Arrays.asList(tv.getType(), tv.getValue());
    }
}
