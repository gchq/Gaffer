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

package gaffer.tuple.handler;

import gaffer.tuple.Tuple;
import gaffer.tuple.tuplen.Tuple5;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class TupleView<R> extends Tuple5<Object, Object, Object, Object, Object> implements TupleHandler<R> {
    protected Tuple<R> tuple;
    protected List<TupleHandler<R>> handlers;

    public TupleView(final List<TupleHandler<R>> handlers) {
        this.handlers = handlers;
    }

    public TupleView(final List<TupleHandler<R>> handlers, final Tuple<R> tuple) {
        this(handlers);
        setTuple(tuple);
    }

    public TupleView(final R[][] references) {
        setReferences(references);
    }

    public TupleView(final R[] references) {
        setReferences(references);
    }

    public TupleView(final R[][] references, final Tuple<R> tuple) {
        this(references);
        setTuple(tuple);
    }

    public TupleView(final R[] references, final Tuple<R> tuple) {
        this(references);
        setTuple(tuple);
    }

    public void setReferences(final R[][] references) {
        handlers = new ArrayList<TupleHandler<R>>(references.length);
        for (R[] refs : references) {
            if (refs.length == 1) {
                handlers.add(new SingleReferenceHandler<R>(refs[0]));
            } else if (refs.length > 1) {
                handlers.add(new MultiReferenceHandler<R>(Arrays.asList(refs)));
            }
        }
    }

    public void setReferences(final R[] references) {
        handlers = new ArrayList<TupleHandler<R>>(references.length);
        for (R reference : references) {
            handlers.add(new SingleReferenceHandler<R>(reference));
        }
    }

    public Object select(final Tuple<R> tuple) {
        if (handlers.size() == 1) {
            return handlers.get(0).select(tuple);
        } else {
            setTuple(tuple);
            return this;
        }
    }

    public void project(final Tuple<R> tuple, final Object values) {
        if (handlers.size() == 1) {
            handlers.get(0).project(tuple, values);
        } else {
            setTuple(tuple);
            int i = 0;
            for (Object value : (Iterable) values) {
                put(i++, value);
            }
        }
    }

    public void setTuple(final Tuple<R> tuple) {
        this.tuple = tuple;
    }

    public Object get(final Integer index) {
        return handlers.get(index).select(tuple);
    }

    public void put(final Integer index, final Object value) {
        handlers.get(index).project(tuple, value);
    }

    public Iterable<Object> values() {
        List<Object> values = new ArrayList<Object>(handlers.size());
        for (int i = 0; i < handlers.size(); i++) {
            values.add(handlers.get(i).select(tuple));
        }
        return values;
    }

    public Iterator<Object> iterator() {
        return values().iterator();
    }

    public Object get4() {
        return get(4);
    }

    public void put4(final Object value) {
        put(4, value);
    }

    public Object get3() {
        return get(3);
    }

    public void put3(final Object value) {
        put(3, value);
    }

    public Object get2() {
        return get(2);
    }

    public void put2(final Object value) {
        put(2, value);
    }

    public Object get1() {
        return get(1);
    }

    public void put1(final Object value) {
        put(1, value);
    }

    public Object get0() {
        return get(0);
    }

    public void put0(final Object value) {
        put(0, value);
    }
}
