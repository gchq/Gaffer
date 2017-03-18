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

package uk.gov.gchq.koryphe.tuple;

import uk.gov.gchq.koryphe.tuple.n.Tuple5;
import java.util.Iterator;

public class ReferenceArrayTuple<R> extends Tuple5 {
    private final R[] fields;
    private final Tuple<R> tuple;

    @SafeVarargs
    public ReferenceArrayTuple(final Tuple<R> tuple, final R... fields) {
        this.tuple = tuple;
        this.fields = fields;
    }

    @Override
    public Object get(final Integer index) {
        if (null != tuple) {
            return tuple.get(fields[index]);
        }

        return null;
    }

    @Override
    public void put(final Integer index, final Object value) {
        if (null != tuple) {
            final R field = fields[index];
            tuple.put(field, value);
        }
    }

    @Override
    public Iterable<Object> values() {
        final ArrayTuple selected = new ArrayTuple(fields.length);
        for (int i = 0; i < fields.length; i++) {
            selected.put(i, get(i));
        }
        return selected;
    }

    @Override
    public Iterator<Object> iterator() {
        return values().iterator();
    }
}
