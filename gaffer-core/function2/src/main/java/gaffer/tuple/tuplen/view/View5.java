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
import gaffer.tuple.tuplen.Tuple5;

/**
 * A {@link gaffer.tuple.view.TupleView} that refers to 5 values in the wrapped tuple.
 * @param <A> Type of value referred to at index 0.
 * @param <B> Type of value referred to at index 1.
 * @param <C> Type of value referred to at index 2.
 * @param <D> Type of value referred to at index 3.
 * @param <E> Type of value referred to at index 4.
 * @param <R> Type of reference used by wrapped tuple.
 */
public class View5<A, B, C, D, E, R> extends View4<A, B, C, D, R> implements Tuple5<A, B, C, D, E> {
    public View5(final Reference<R> first, final Reference<R> second, final Reference<R> third, final Reference<R> fourth, final Reference<R> fifth) {
        super(first, second, third, fourth, fifth);
    }

    protected View5(final Reference<R>... references) {
        super(references);
        if (references.length < 5) {
            throw new IllegalStateException("Invalid number of references");
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
