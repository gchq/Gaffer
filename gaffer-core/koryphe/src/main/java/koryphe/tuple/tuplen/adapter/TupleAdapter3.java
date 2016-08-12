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

import com.fasterxml.jackson.annotation.JsonIgnore;
import koryphe.tuple.adapter.TupleAdapter;
import koryphe.tuple.tuplen.Tuple3;

/**
 * A {@link TupleAdapter} that refers to 3 values in the wrapped tuple.
 * @param <A> Type of value referred to at index 0.
 * @param <B> Type of value referred to at index 1.
 * @param <C> Type of value referred to at index 2.
 * @param <R> Type of reference used by wrapped tuple.
 */
public class TupleAdapter3<A, B, C, R> extends TupleAdapter2<A, B, R> implements Tuple3<A, B, C> {
    public TupleAdapter3(final TupleAdapter<R, A> first, final TupleAdapter<R, B> second, final TupleAdapter<R, C> third) {
        super(first, second, third);
    }

    public TupleAdapter3() { }

    protected TupleAdapter3(final TupleAdapter<R, ?>... references) {
        super(references);
    }

    @Override
    @JsonIgnore
    public C get2() {
        return (C) get(2);
    }

    @Override
    @JsonIgnore
    public void put2(final C c) {
        put(2, c);
    }
}
