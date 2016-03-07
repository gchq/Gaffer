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
import gaffer.tuple.view.TupleView;

/**
 * A {@link gaffer.tuple.Tuple} with four values of the specified generic types.
 * @param <A> Type of first tuple value.
 * @param <B> Type of second tuple value.
 * @param <C> Type of third tuple value.
 * @param <D> Type of fourth tuple value.
 */
public abstract class Tuple4<A, B, C, D> extends Tuple3<A, B, C> {
    public abstract D get3();
    public abstract void put3(D d);

    /**
     * @return New {@link gaffer.tuple.ArrayTuple} of size 4.
     */
    public static Tuple4 createTuple() {
        return new ArrayTuple(4);
    }
}
