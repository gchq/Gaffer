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

package uk.gov.gchq.koryphe.tuple.n;

import uk.gov.gchq.koryphe.tuple.ArrayTuple;

/**
 * An {@link ArrayTuple} containing 5 entries.
 */
public class TupleN extends ArrayTuple {
    public TupleN(final int size) {
        super(size);
    }

    public TupleN(final Object... values) {
        super(values.length);
        int i = 0;
        for (final Object value : values) {
            put(i, value);
            i++;
        }
    }
}
