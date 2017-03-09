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

package uk.gov.gchq.koryphe.tuple.n.mask;

import com.fasterxml.jackson.annotation.JsonIgnore;
import uk.gov.gchq.koryphe.tuple.mask.TupleMask;
import uk.gov.gchq.koryphe.tuple.n.Tuple5;

/**
 * A {@link TupleMask} that refers to 5 values in the wrapped tuple.
 * @param <A> Type of value referred to at index 0.
 * @param <B> Type of value referred to at index 1.
 * @param <C> Type of value referred to at index 2.
 * @param <D> Type of value referred to at index 3.
 * @param <E> Type of value referred to at index 4.
 * @param <R> Type of reference used by wrapped tuple.
 */
public class TupleMask5<A, B, C, D, E, R> extends TupleMask4<A, B, C, D, R> implements Tuple5<A, B, C, D, E> {
    public TupleMask5(final TupleMask<R, A> first, final TupleMask<R, B> second, final TupleMask<R, C> third, final TupleMask<R, D> fourth, final TupleMask<R, E> fifth) {
        super(first, second, third, fourth, fifth);
    }

    public TupleMask5() { }

    protected TupleMask5(final TupleMask<R, ?>... references) {
        super(references);
    }

    @Override
    @JsonIgnore
    public E get4() {
        return (E) get(4);
    }

    @Override
    @JsonIgnore
    public void put4(final E e) {
        put(4, e);
    }
}
