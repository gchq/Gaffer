/*
 * Copyright 2021 Crown Copyright
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

import uk.gov.gchq.gaffer.types.TypeSubTypeValue;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;
import uk.gov.gchq.koryphe.tuple.Tuple;

import java.util.Arrays;

import static java.util.Objects.isNull;

/**
 * A {@link TypeSubTypeValueTuple} class represents a {@link TypeSubTypeValue} object as a Koryphe n-valued {@link Tuple}.
 */
@Since("1.19.0")
@Summary("Tuple object for TypeSubTypeValue")
public class TypeSubTypeValueTuple implements Tuple<String> {
    private final TypeSubTypeValue tsv;
    public static final String TYPE = "type";
    public static final String SUBTYPE = "subType";
    public static final String VALUE = "value";

    public TypeSubTypeValueTuple() {
        this.tsv = new TypeSubTypeValue();
    }

    public TypeSubTypeValueTuple(final TypeSubTypeValue tsv) {
        if (isNull(tsv)) {
            this.tsv = new TypeSubTypeValue();
        } else {
            this.tsv = tsv;
        }
    }

    @Override
    public void put(final String key, final Object value) {
        final String stringValue = isNull(value) ? null : value.toString();
        if (TYPE.equalsIgnoreCase(key)) {
            tsv.setType(stringValue);
        } else if (SUBTYPE.equalsIgnoreCase(key)) {
            tsv.setSubType(stringValue);
        } else if (VALUE.equalsIgnoreCase(key)) {
            tsv.setValue(stringValue);
        }
    }

    @Override
    public Object get(final String key) {
        if (TYPE.equalsIgnoreCase(key)) {
            return tsv.getType();
        }
        if (SUBTYPE.equalsIgnoreCase(key)) {
            return tsv.getSubType();
        }
        if (VALUE.equalsIgnoreCase(key)) {
            return tsv.getValue();
        }
        return null;
    }

    @Override
    public Iterable<Object> values() {
        return Arrays.asList(tsv.getType(), tsv.getSubType(), tsv.getValue());
    }
}
