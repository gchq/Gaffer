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
 * A <code>TupleView</code> acts as a mask for a wrapped {@link gaffer.tuple.Tuple}. It allows the
 * selection of just a subset of the values, and also allows the flat data structure of a
 * {@link gaffer.tuple.Tuple} to be presented in two dimensions.
 * <p/>
 * A simple case is the execution of a {@link gaffer.function2.Function} that accepts a single input,
 * that we want to execute using one of the values from a {@link gaffer.tuple.Tuple} - for example:
 * <p/>
 * <code>
 *     StatelessFunction&lt;Integer, ?&gt; function; //function accepts single integer input.<br/>
 *     TupleView&lt;String&gt; view = new TupleView().addHandler("a");<br/>
 *     Object output = function.execute(view.select(tuple));<br/>
 * </code>
 * <p/>
 * But the <code>TupleView</code> can also be used to select the input to a
 * {@link gaffer.function2.Function} with a more complicated input signature, such as a collection
 * of <code>String</code>s, an <code>Integer</code>, and a <code>Tuple2&lt;Double, String&gt;</code>:
 * <p/>
 * <code>
 *     StatelessFunction&lt;Tuple3&lt;Iterable&lt;String&gt;, Integer, Tuple2&lt;Double, String&gt;&gt;, ?&gt; function;
 *     <br/>
 *     //select "a", "b" and "c" into the first argument, "d" into the second and "e" and "f" into the third.<br/>
 *     TupleView&lt;String&gt; view = new TupleView().addHandler("a", "b", "c").addHandler("d").addHandler("e", "f");<br/>
 *     Object output = function.execute(view.select(tuple));<br/>
 * </code>
 * <p/>
 * A <code>TupleView</code> can also be used to project values into an output tuple in a similar way:
 * <p/>
 * <code>
 *     StatelessFunction&lt;String, Tuple2&lt;String, Integer&gt;&gt; function;<br/>
 *     //project first result value into "a" and the second into "b".<br/>
 *     TupleView&lt;String&gt; view = new TupleView().addHandler("a").addHandler("b");<br/>
 *     view.project(tuple, function.execute("in"));<br/>
 * </code>
 * @param <R> The type of reference used to select from and project into tuples.
 * @see gaffer.tuple.view.SingleReferenceHandler
 * @see gaffer.tuple.view.MultiReferenceHandler
 */
public class TupleView<R> extends Tuple5<Object, Object, Object, Object, Object> {
    private Tuple<R> tuple;
    private List<ReferenceHandler<R>> handlers;

    /**
     * Create a new <code>TupleView</code>.
     */
    public TupleView() { }

    /**
     * @param tuple Tuple to be viewed.
     */
    public void setTuple(final Tuple<R> tuple) {
        this.tuple = tuple;
    }

    /**
     * Add a {@link ReferenceHandler} to this <code>TupleView</code>.
     * @param handler Handler to add.
     * @return This view.
     */
    public TupleView<R> addHandler(final ReferenceHandler<R> handler) {
        if (handlers == null) {
            handlers = new ArrayList<>();
        }
        handlers.add(handler);
        return this;
    }

   /**
     * Add a {@link ReferenceHandler} to this <code>TupleView</code> using the specified references.
     * @param references The references to add.
     * @return This view.
     */
    public TupleView<R> addHandler(final R... references) {
        if (references.length == 1) {
            return addHandler(new SingleReferenceHandler<R>(references[0]));
        } else {
            return addHandler(new MultiReferenceHandler<R>(references));
        }
    }

    /**
     * @return {@link ReferenceHandler}s used by this <code>TupleView</code>.
     */
    public List<ReferenceHandler<R>> getHandlers() {
        return handlers;
    }

    /**
     * @return References used by this <code>TupleView</code>.
     */
    public List<List<R>> getReferences() {
        List<List<R>> references = new ArrayList(handlers.size());
        for (ReferenceHandler<R> handler : handlers) {
            references.add(handler.getReferences());
        }
        return references;
    }

    /**
     * Select the value(s) referenced by this <code>TupleView</code> from the viewed {@link gaffer.tuple.Tuple}.
     * @param source Tuple to select from.
     * @return Selected value(s).
     */
    public Object select(final Tuple<R> source) {
        setTuple(source);
        if (handlers.size() == 1) {
            return get(0);
        } else {
            return this;
        }
    }

    /**
     * Project value(s) into the viewed {@link gaffer.tuple.Tuple} using this <code>TupleView</code>'s references.
     * @param target Tuple to project into.
     * @param values Value(s) to project.
     */
    public void project(final Tuple<R> target, final Object values) {
        setTuple(target);
        if (handlers.size() == 1) {
            put(0, values);
        } else {
            int i = 0;
            for (Object value : (Iterable) values) {
                put(i++, value);
            }
        }
    }

    @Override
    public Object get(final Integer index) {
        return handlers.get(index).select(tuple);
    }

    @Override
    public void put(final Integer index, final Object value) {
        handlers.get(index).project(tuple, value);
    }

    @Override
    public Iterable<Object> values() {
        List<Object> values = new ArrayList<Object>(handlers.size());
        for (int i = 0; i < handlers.size(); i++) {
            values.add(handlers.get(i).select(tuple));
        }
        return values;
    }

    @Override
    public Iterator<Object> iterator() {
        return values().iterator();
    }
}
