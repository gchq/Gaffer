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

package koryphe.tuple.tuplen.adapter;

import koryphe.tuple.adapter.TupleAdapter;

/**
 * A {@link TupleAdapter} that refers to more than 5 values in the wrapped tuple.
 * @param <R> Type of reference used by wrapped tuple.
 */
public class TupleAdapterN<R> extends TupleAdapter5<Object, Object, Object, Object, Object, R> {
    public TupleAdapterN(final TupleAdapter<R, ?>... references) {
        super(references);
    }

    public TupleAdapterN() { }
}
