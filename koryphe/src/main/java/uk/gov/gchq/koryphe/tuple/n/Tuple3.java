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

/**
 * An {@link TupleN} containing 3 entries.
 *
 * @param <A> Type of the entry at index 0.
 * @param <B> Type of the entry at index 1.
 * @param <C> Type of the entry at index 2.
 */
public class Tuple3<A, B, C> extends Tuple2<A, B> {
    public Tuple3() {
        super(3);
    }

    public Tuple3(final A a, final B b, final C c) {
        this();
        put0(a);
        put1(b);
        put2(c);
    }

    /**
     * Pass-through constructor for larger tuple sizes.
     *
     * @param size Tuple size.
     */
    protected Tuple3(final int size) {
        super(size);
        if (size < 3) {
            throw new IllegalArgumentException("Invalid size");
        }
    }

    public C get2() {
        return (C) get(2);
    }

    public void put2(final C c) {
        put(2, c);
    }
}
