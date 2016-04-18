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

import gaffer.tuple.tuplen.Tuple1;
import gaffer.tuple.view.Reference;
import gaffer.tuple.view.TupleView;

/**
 * A {@link gaffer.tuple.view.TupleView} that refers to a single value in the wrapped tuple.
 * @param <A> Type of value referred to at index 0.
 * @param <R> Type of reference used by wrapped tuple.
 */
public class View1<A, R> extends TupleView<R> implements Tuple1<A> {
    public View1(final Reference<R> reference) {
        super(reference);
    }

    protected View1(final Reference<R>... references) {
        super(references);
        if (references.length < 1) {
            throw new IllegalStateException("Invalid number of references");
        }
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
