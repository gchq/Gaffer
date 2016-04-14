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

package gaffer.tuple.view;

import gaffer.tuple.ArrayTuple;
import gaffer.tuple.Tuple;

import java.util.Iterator;

/**
 * Wraps a {@link gaffer.tuple.Tuple} making the tuple values available via an integer index. It can contain
 * arbitrarily complex references, with each value returned by the view being either a single value, or another tuple
 * containing a subset of tuple values. This functionality can be used to make a flat tuple appear as a
 * multi-dimensional data structure.
 * @param <R> The type of reference used by the wrapped tuple.
 */
public class TupleView<R> extends View<R> implements Tuple<Integer> {
    protected Tuple<R> tuple;
    protected View<R>[] references;

    public TupleView(final Reference<R>... references) {
        this.references = new View[references.length];
        int i = 0;
        for (Reference<R> reference : references) {
            this.references[i++] = View.createView(reference);
        }
    }

    public TupleView(final Tuple<R> tuple, final Reference<R>... references) {
        this(references);
        setTuple(tuple);
    }

    public void setTuple(final Tuple<R> tuple) {
        this.tuple = tuple;
    }

    public Tuple<R> getTuple() {
        return tuple;
    }

    @Override
    public Object select(final Tuple<R> tuple) {
        setTuple(tuple);
        return this;
    }

    @Override
    public void project(final Tuple<R> tuple, final Object values) {
        setTuple(tuple);
        int i = 0;
        for (Object value : (Iterable) values) {
            put(i++, value);
        }
    }

    @Override
    public Reference<R> getReference() {
        Reference<R>[] references = new Reference[this.references.length];
        int i = 0;
        for (View<R> reference : this.references) {
            references[i++] = reference.getReference();
        }
        Reference reference = new Reference<R>();
        reference.setTuple(references);
        return reference;
    }

    @Override
    public void put(final Integer reference, final Object value) {
        references[reference].project(tuple, value);
    }

    @Override
    public Object get(final Integer reference) {
        return references[reference].select(tuple);
    }

    @Override
    public Iterable<Object> values() {
        ArrayTuple selected = new ArrayTuple(references.length);
        for (int i = 0; i < references.length; i++) {
            selected.put(i, get(i));
        }
        return selected;
    }

    @Override
    public Iterator<Object> iterator() {
        return values().iterator();
    }
}
