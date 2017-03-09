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

package koryphe.tuple.n.mask;

import com.fasterxml.jackson.annotation.JsonIgnore;
import koryphe.tuple.mask.TupleMask;
import koryphe.tuple.n.Tuple1;

/**
 * A {@link TupleMask} that refers to a single value in the wrapped tuple.
 * @param <A> Type of value referred to at index 0.
 * @param <R> Type of reference used by wrapped tuple.
 */
public class TupleMask1<A, R> extends TupleMask<R, Tuple1<A>> implements Tuple1<A> {
    public TupleMask1(final TupleMask<R, A> reference) {
        super(reference);
    }

    public TupleMask1() { }

    protected TupleMask1(final TupleMask<R, ?>... references) {
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
