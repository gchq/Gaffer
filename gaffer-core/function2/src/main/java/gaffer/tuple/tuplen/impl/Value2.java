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

package gaffer.tuple.tuplen.impl;

import gaffer.tuple.impl.ArrayTuple;
import gaffer.tuple.tuplen.Tuple2;

public class Value2<A, B> extends ArrayTuple implements Tuple2<A, B> {
    public Value2() {
        super(2);
    }

    /**
     * Pass-through constructor for larger tuple sizes.
     * @param size Size of tuple.
     */
    protected Value2(final int size) {
        super(size);
        if (size < 2) {
            throw new IllegalArgumentException("Invalid size");
        }
    }

    @Override
    public B get1() {
        return (B) get(1);
    }

    @Override
    public void put1(final B b) {
        put(1, b);
    }

    @Override
    public A get0() {
        return (A) get(0);
    }

    @Override
    public void put0(final A a) {
        put(0, a);
    }
}
