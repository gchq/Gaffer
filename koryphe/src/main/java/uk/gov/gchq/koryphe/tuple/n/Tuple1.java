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
 * An {@link ArrayTuple} containing 2 entries.
 *
 * @param <A> Type of the entry at index 0.
 */
public class Tuple1<A> extends ArrayTuple {
    public Tuple1() {
        super(1);
    }

    public Tuple1(final A a) {
        this();
        put0(a);
    }

    /**
     * Pass-through constructor for larger tuple sizes.
     *
     * @param size Size of tuple.
     */
    protected Tuple1(final int size) {
        super(size);
        if (size < 1) {
            throw new IllegalArgumentException("Invalid size");
        }
    }

    public A get0() {
        return (A) get(0);
    }

    public void put0(final A a) {
        put(0, a);
    }
}
