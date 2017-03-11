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
 * @param <B> Type of the entry at index 1.
 */
public class Tuple2<A, B> extends Tuple1<A> {
    public Tuple2() {
        super(2);
    }

    public Tuple2(final A a, final B b) {
        this();
        put0(a);
        put1(b);
    }

    /**
     * Pass-through constructor for larger tuple sizes.
     *
     * @param size Size of tuple.
     */
    protected Tuple2(final int size) {
        super(size);
        if (size < 2) {
            throw new IllegalArgumentException("Invalid size");
        }
    }

    public B get1() {
        return (B) get(1);
    }

    public void put1(final B b) {
        put(1, b);
    }
}
