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

import uk.gov.gchq.koryphe.tuple.Tuple;

/**
 * A {@link Tuple} with a single value of the specified generic type.
 * @param <A> Type of first tuple value.
 */
public interface Tuple1<A> extends Tuple<Integer> {
    /**
     * Get the value at index 0.
     * @return Value.
     */
    A get0();

    /**
     * Put a value into index 0.
     * @param a Value to put.
     */
    void put0(final A a);
}
