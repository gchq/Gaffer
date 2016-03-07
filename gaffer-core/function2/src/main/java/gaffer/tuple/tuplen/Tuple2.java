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

package gaffer.tuple.tuplen;

import gaffer.tuple.ArrayTuple;

/**
 * A {@link gaffer.tuple.Tuple} with two values of the specified generic types.
 * @param <A> Type of first tuple value.
 * @param <B> Type of second tuple value.
 */
public abstract class Tuple2<A, B> extends Tuple1<A> {
    /**
     * Get the value at index 1.
     * @return Value.
     */
    public abstract B get1();

    /**
     * Put a value into index 1.
     * @param b Value to put.
     */
    public abstract void put1(B b);

    /**
     * @return New {@link gaffer.tuple.ArrayTuple} of size 2.
     */
    public static Tuple2 createTuple() {
        return new ArrayTuple(2);
    }
}
