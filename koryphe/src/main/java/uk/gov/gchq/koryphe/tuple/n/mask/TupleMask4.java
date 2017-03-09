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
import uk.gov.gchq.koryphe.tuple.n.Tuple4;

/**
 * A {@link TupleMask} that refers to 4 values in the wrapped tuple.
 * @param <A> Type of value referred to at index 0.
 * @param <B> Type of value referred to at index 1.
 * @param <C> Type of value referred to at index 2.
 * @param <D> Type of value referred to at index 3.
 * @param <R> Type of reference used by wrapped tuple.
 */
public class TupleMask4<A, B, C, D, R> extends TupleMask3<A, B, C, R> implements Tuple4<A, B, C, D> {
    public TupleMask4(final TupleMask<R, A> first, final TupleMask<R, B> second, final TupleMask<R, C> third, final TupleMask<R, D> fourth) {
        super(first, second, third, fourth);
    }

    public TupleMask4() { }

    protected TupleMask4(final TupleMask<R, ?>... references) {
        super(references);
    }

    @Override
    @JsonIgnore
    public D get3() {
        return (D) get(3);
    }

    @Override
    @JsonIgnore
    public void put3(final D d) {
        put(3, d);
    }
}
