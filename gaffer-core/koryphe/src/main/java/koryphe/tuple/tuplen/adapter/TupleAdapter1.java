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
import koryphe.tuple.tuplen.Tuple1;
import koryphe.tuple.adapter.TupleAdapter;

/**
 * A {@link TupleAdapter} that refers to a single value in the wrapped tuple.
 * @param <A> Type of value referred to at index 0.
 * @param <R> Type of reference used by wrapped tuple.
 */
public class TupleAdapter1<A, R> extends TupleAdapter<R, Tuple1<A>> implements Tuple1<A> {
    public TupleAdapter1(final TupleAdapter<R, A> reference) {
        super(reference);
    }

    public TupleAdapter1() { }

    protected TupleAdapter1(final TupleAdapter<R, ?>... references) {
        super(references);
    }

    @Override
    @JsonIgnore
    public A get0() {
        return (A) get(0);
    }

    @Override
    @JsonIgnore
    public void put0(final A a) {
        put(0, a);
    }
}
