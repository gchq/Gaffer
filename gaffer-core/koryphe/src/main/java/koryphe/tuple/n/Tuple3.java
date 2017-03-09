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

package koryphe.tuple.n;

/**
 * A {@link koryphe.tuple.Tuple} with three values of the specified generic types.
 * @param <A> Type of first tuple value.
 * @param <B> Type of second tuple value.
 * @param <C> Type of third tuple value.
 */
public interface Tuple3<A, B, C> extends Tuple2<A, B> {
    /**
     * Get the value at index 2.
     * @return Value.
     */
    C get2();

    /**
     * Put a value into index 2.
     * @param c Value to put.
     */
    void put2(final C c);
}
