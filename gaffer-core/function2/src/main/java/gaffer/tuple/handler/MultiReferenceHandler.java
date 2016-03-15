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

/**
 * A <code>MultiReferenceHandler</code> contains a list of references and presents these as a
 * <code>Tuple&lt;Integer&gt;</code>.
 * @param <R> The type of reference used to select from and project into tuples.
 */
public class MultiReferenceHandler<R> extends Tuple5<Object, Object, Object, Object, Object> implements TupleHandler<R> {
    private R[] references;
    private Tuple<R> tuple;

    /**
     * Create a <code>MultiReferenceHandler</code> with the given references.
     * @param references Tuple references.
     */
    public MultiReferenceHandler(final R... references) {
        this.references = references;
        this.tuple = null;
    }

    @Override
    public void put(final Integer index, final Object value) {
        tuple.put(references[index], value);
    }

    @Override
    public Object get(final Integer index) {
        return tuple.get(references[index]);
    }

    @Override
    public Iterable<Object> values() {
        List<Object> values = new ArrayList<>(references.length);
        for (R reference : references) {
            values.add(tuple.get(reference));
        }
        return values;
    }

    @Override
    public Iterator<Object> iterator() {
        return values().iterator();
    }

    @Override
    public Object select(final Tuple<R> source) {
        tuple = source;
        return this;
    }

    @Override
    public void project(final Tuple<R> target, final Object values) {
        //values assumed to be iterable
        tuple = target;
        int i = 0;
        for (Object value : (Iterable) values) {
            put(i++, value);
        }
    }

    @Override
    public List<R> getReferences() {
        return Arrays.asList(references);
    }
}
