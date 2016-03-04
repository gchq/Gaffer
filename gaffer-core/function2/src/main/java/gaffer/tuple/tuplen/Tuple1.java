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
import gaffer.tuple.Tuple;
import gaffer.tuple.view.TupleView;

/**
 * A {@link gaffer.tuple.Tuple} with a single value of the specified generic type.
 * @param <A> Type of first tuple value.
 */
public abstract class Tuple1<A> implements Tuple<Integer> {
    public abstract A get0();
    public abstract void put0(A a);

    /**
     * @return New <code>Tuple1</code> backed by a new {@link gaffer.tuple.ArrayTuple} of size 1.
     */
    public static Tuple1 createTuple() {
        return new TupleView(new Integer[][]{{0}}, new ArrayTuple(1));
    }
}
