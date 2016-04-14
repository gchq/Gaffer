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

package gaffer.tuple.tuplen.value;

import gaffer.tuple.tuplen.Tuple5;

public class Value5<A, B, C, D, E> extends Value4<A, B, C, D> implements Tuple5<A, B, C, D, E> {
    public Value5() {
        super(5);
    }

    /**
     * Pass-through constructor for larger tuple sizes.
     * @param size Tuple size.
     */
    protected Value5(final int size) {
        super(size);
        if (size < 5) {
            throw new IllegalArgumentException("Invalid size");
        }
    }

    @Override
    public E get4() {
        return (E) get(4);
    }

    @Override
    public void put4(final E e) {
        put(4, e);
    }
}
