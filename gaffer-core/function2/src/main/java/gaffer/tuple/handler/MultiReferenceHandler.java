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

import java.util.ArrayList;
import java.util.List;

/**
 * A {@link gaffer.tuple.handler.TupleHandler} that selects and projects the values into/out of a
 * {@link gaffer.tuple.Tuple} using a {@link java.util.List} of references.
 * <p/>
 * The <code>MultiReferenceHandler</code> uses a {@link gaffer.tuple.handler.TupleView} to select the values.
 *
 * @see gaffer.tuple.handler.TupleView
 *
 * @param <R> The type of reference used to select from and project into tuples.
 */
public class MultiReferenceHandler<R> implements TupleHandler<R> {
    private TupleView<R> view;

    /**
     * Create a <code>MultiReferenceHandler</code> with the given references.
     * @param references Value references.
     */
    public MultiReferenceHandler(final List<R> references) {
        List<TupleHandler<R>> handlers = new ArrayList<TupleHandler<R>>(references.size());
        for (R reference : references) {
            handlers.add(new SingleReferenceHandler<R>(reference));
        }
        view = new TupleView<R>(handlers);
    }

    /**
     * Get an int referenced {@link gaffer.tuple.Tuple} containing the selected values. The first selected value
     * will be found at reference 0, the second at reference 1, etc.
     * @param source Source {@link gaffer.tuple.Tuple}.
     * @return {@link gaffer.tuple.handler.TupleView} with selected values.
     */
    public Tuple<Integer> select(final Tuple<R> source) {
        view.setTuple(source);
        return view;
    }

    /**
     * Project values into the target tuple. The values must implement {@link java.lang.Iterable}.
     * @param target Target {@link gaffer.tuple.Tuple}.
     * @param values {link java.lang.Iterable} result values to project.
     */
    public void project(final Tuple<R> target, final Object values) {
        view.setTuple(target);
        int i = 0;
        for (Object value : (Iterable) values) {
            view.put(i++, value);
        }
    }
}
