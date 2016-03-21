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

import gaffer.tuple.Tuple;
import gaffer.tuple.tuplen.Tuple5;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A <code>TupleView</code> allows the selection of multiple values from a {@link gaffer.tuple.Tuple}. Each value is
 * selected by a {@link gaffer.tuple.view.View}, so can be either a single or multiple values. This allows the tuple to
 * be viewed as a multi-dimensional data structure.
 * @param <R> The type of reference used by tuples.
 */
public class TupleView<R> extends Tuple5<Object, Object, Object, Object, Object> implements View<R> {
    private List<View<R>> views;
    private Tuple<R> tuple;

    /**
     * Create a <code>TupleView</code> from multiple references.
     * @param reference Tuple references.
     */
    public TupleView(final Reference<R> reference) {
        setReference(reference);
    }

    /**
     * @param tuple The tuple viewed by this <code>TupleView</code>.
     */
    public void setTuple(final Tuple<R> tuple) {
        this.tuple = tuple;
    }

    /**
     * @return The tuple viewed by this <code>TupleView</code>.
     */
    public Tuple<R> getTuple() {
        return tuple;
    }

    /**
     * Sets the {@link Reference} to be used by this <code>TupleView</code>.
     * @param reference Multiple references.
     * @throws IllegalArgumentException If reference is null or is a singleton.
     */
    public void setReference(final Reference<R> reference) {
        if (reference == null || reference.getTupleReferences() == null) {
            throw new IllegalArgumentException("Reference passed to TupleView must be a non-null tuple reference.");
        } else {
            List<View<R>> views = new ArrayList<View<R>>();
            for (Reference<R> ref : reference.getTupleReferences()) {
                views.add(ref.createView());
            }
            setViews(views);
        }
    }

    /**
     * Get the {@link Reference} used by this <code>TupleView</code>.
     * @return Multiple references.
     */
    public Reference<R> getReference() {
        if (views == null) {
            return null;
        } else {
            Reference<R> reference = new Reference<>();
            Reference<R>[] references = new Reference[views.size()];
            int i = 0;
            for (View<R> view : views) {
                references[i++] = view.getReference();
            }
            reference.setTupleReferences(references);
            return reference;
        }
    }

    /**
     * @param views {@link gaffer.tuple.view.View}s to be used by this <code>TupleView</code>.
     */
    public void setViews(final List<View<R>> views) {
        this.views = views;
    }

    /**
     * @return {@link gaffer.tuple.view.View}s to be used by this <code>TupleView</code>.
     */
    public List<View<R>> getViews() {
        return views;
    }

    /**
     * Project a value into the viewed {@link gaffer.tuple.Tuple} using the reference at the given index.
     * @param index Index of reference used to project.
     * @param value Value to put.
     */
    public void put(final Integer index, final Object value) {
        views.get(index).project(tuple, value);
    }

    /**
     * Get a value from the viewed {@link gaffer.tuple.Tuple} using the reference at the given index.
     * @param index Index of the reference used to get.
     * @return Value at reference.
     */
    public Object get(final Integer index) {
        return views.get(index).select(tuple);
    }

    /**
     * @return Ordered {@link java.lang.Iterable} of the values selected by this <code>TupleView</code>.
     */
    public Iterable<Object> values() {
        List<Object> values = new ArrayList(views.size());
        for (View<R> view : views) {
            values.add(view.select(tuple));
        }
        return values;
    }

    /**
     * @return Iterator over the values selected by this <code>TupleView</code>.
     */
    public Iterator<Object> iterator() {
        return values().iterator();
    }

    /**
     * Get a {@link gaffer.tuple.Tuple} containing the values selected by this <code>TupleView</code>.
     * @param source Tuple to select from.
     * @return Tuple containing the selected values.
     */
    public Object select(final Tuple<R> source) {
        tuple = source;
        return this;
    }

    /**
     * Project an {@link java.lang.Iterable} of values into a target {@link gaffer.tuple.Tuple}.
     * @param target Tuple to project into.
     * @param values Values to project.
     */
    public void project(final Tuple<R> target, final Object values) {
        //values assumed to be iterable
        tuple = target;
        int i = 0;
        for (Object value : (Iterable) values) {
            put(i++, value);
        }
    }
}
