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

public abstract class Tuple3<A, B, C> extends Tuple2<A, B> {
    public abstract C get2();
    public abstract void put2(C c);

    public static Tuple3 createTuple() {
        return new TupleView(new Integer[][]{{0}, {1}, {2}}, new ArrayTuple(3));
    }
}
