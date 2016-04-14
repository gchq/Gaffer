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

package gaffer.tuple.tuplen.view;

import gaffer.tuple.view.Reference;
import gaffer.tuple.tuplen.Tuple2;

public class View2<A, B, R> extends View1<A, R> implements Tuple2<A, B> {
    public View2(final Reference<R> first, final Reference<R> second) {
        super(first, second);
    }

    protected View2(final Reference<R>... references) {
        super(references);
        if (references.length < 2) {
            throw new IllegalStateException("Invalid number of references");
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
}
