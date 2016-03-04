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

package gaffer.tuple;

import java.util.ArrayList;

public class ArrayTuple extends ArrayList<Object> implements Tuple<Integer> {
    private static final long serialVersionUID = -4729093309234184200L;

    public ArrayTuple(final int size) {
        super(size);
    }

    public void putValue(final Integer reference, final Object value) {
        add(reference, value);
    }

    public Object getValue(final Integer reference) {
        return get(reference);
    }

    public Iterable<Object> values() {
        return this;
    }
}
