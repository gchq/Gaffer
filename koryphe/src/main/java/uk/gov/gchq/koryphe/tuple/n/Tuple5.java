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
 *
 * @param <A> Type of the entry at index 0.
 * @param <B> Type of the entry at index 1.
 * @param <C> Type of the entry at index 2.
 * @param <D> Type of the entry at index 3.
 * @param <E> Type of the entry at index 4.
 */
public class Tuple5<A, B, C, D, E> extends Tuple4<A, B, C, D> {
    public Tuple5() {
        super(5);
    }

    public Tuple5(final A a, final B b, final C c, final D d, final E e) {
        this();
        put0(a);
        put1(b);
        put2(c);
        put3(d);
        put4(e);
    }

    /**
     * Pass-through constructor for larger tuple sizes.
     *
     * @param size Tuple size.
     */
    protected Tuple5(final int size) {
        super(size);
        if (size < 5) {
            throw new IllegalArgumentException("Invalid size");
        }
    }

    public E get4() {
        return (E) get(4);
    }

    public void put4(final E e) {
        put(4, e);
    }
}
